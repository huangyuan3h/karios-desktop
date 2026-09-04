"""Import vendor A-share minute CSVs (网盘 按年汇总) into bar_5min.

Same schema: BOM CSV with 时间/代码/OHLC/成交量/成交额.
Bar-end timestamps (5min first print 09:35; 14:30 close = 14:30 price).
2026 files mix `2026-01-05 09:35:00` and `2026/09/02 14:45`.

15min: store 1430 + 1500 only. 5min: last-hour 1430…1500 (7 bars).
5min overwrites ext_15min; never the reverse.
"""
from __future__ import annotations

import csv
import logging
import re
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any

from data_sync_service.db.bar_5min import upsert_5min_payload
from data_sync_service.service.bar_5min import LAST_HOUR_TIMES

logger = logging.getLogger(__name__)

SOURCE_EXT_15MIN = "ext_15min"
SOURCE_EXT_5MIN = "ext_5min"
TIMES_15MIN = frozenset({"1430", "1500"})
FLUSH_ROWS = 60_000
_FILE_RE = re.compile(r"^(sz|sh|bj)(\d{6})", re.IGNORECASE)
_COL_OPEN = "开盘价"
_COL_CLOSE = "收盘价"
_COL_HIGH = "最高价"
_COL_LOW = "最低价"
_COL_VOL = "成交量"
_COL_AMT = "成交额"
_COL_TIME = "时间"


def filename_to_ts_code(name: str) -> str | None:
    """sz000001_2025.csv → 000001.SZ. Skip BJ (S-gap universe excludes it)."""
    stem = Path(name).name
    m = _FILE_RE.match(stem)
    if not m:
        return None
    prefix, code = m.group(1).lower(), m.group(2)
    if prefix == "bj":
        return None
    exch = "SZ" if prefix == "sz" else "SH"
    return f"{code}.{exch}"


def detect_freq(path: Path) -> int:
    """5 or 15 from folder/file name. Default 15 (narrower last-hour keep)."""
    blob = str(path).lower()
    if "15min" in blob or "15分钟" in blob or "15分" in blob:
        return 15
    if "5min" in blob or "5分钟" in blob or "5分" in blob:
        return 5
    return 15


def source_for_freq(freq: int) -> str:
    return SOURCE_EXT_5MIN if freq == 5 else SOURCE_EXT_15MIN


def times_for_freq(freq: int) -> frozenset[str]:
    return LAST_HOUR_TIMES if freq == 5 else TIMES_15MIN


def parse_keep_times(raw: str | None) -> frozenset[str] | None:
    """Comma-separated HHMM list. None = use freq default."""
    if not raw:
        return None
    out = {p.strip() for p in raw.split(",") if p.strip()}
    bad = [t for t in out if len(t) != 4 or not t.isdigit()]
    if bad:
        raise ValueError(f"invalid --times {bad}; expected HHMM like 1330,1400")
    return frozenset(out)


def parse_vendor_ts(raw: str) -> tuple[str, str] | None:
    """Return (YYYY-MM-DD, HHMM) from vendor time strings."""
    ts = (raw or "").strip()
    if " " not in ts:
        return None
    date_part, time_part = ts.split(" ", 1)
    date_part = date_part.replace("/", "-")
    if len(date_part) != 10 or date_part[4] != "-" or date_part[7] != "-":
        return None
    hhmm = time_part.replace(":", "")[:4]
    if len(hhmm) != 4 or not hhmm.isdigit():
        return None
    return date_part, hhmm


def _num(val: Any) -> float | None:
    try:
        if val is None or val == "":
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def parse_vendor_csv(
    path: Path,
    *,
    keep_times: frozenset[str],
) -> tuple[str | None, list[dict[str, Any]]]:
    """Return (ts_code, last-hour rows). ts_code from filename, not CSV 代码."""
    ts_code = filename_to_ts_code(path.name)
    if ts_code is None:
        return None, []
    rows: list[dict[str, Any]] = []
    for rec in _iter_last_hour_rows(path, keep_times):
        rows.append(
            {
                "trade_date": rec[0],
                "time": rec[1],
                "open": rec[2],
                "high": rec[3],
                "low": rec[4],
                "close": rec[5],
                "vol": rec[6],
                "amount": rec[7],
            }
        )
    return ts_code, rows


def _header_index(header: list[str]) -> dict[str, int]:
    cleaned = [h.lstrip("\ufeff").strip() for h in header]
    return {name: i for i, name in enumerate(cleaned)}


def _iter_last_hour_rows(
    path: Path,
    keep_times: frozenset[str],
) -> Iterable[tuple[str, str, float | None, float | None, float | None, float, float | None, float | None]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return
        idx = _header_index(header)
        try:
            i_time = idx[_COL_TIME]
            i_open = idx[_COL_OPEN]
            i_close = idx[_COL_CLOSE]
            i_high = idx[_COL_HIGH]
            i_low = idx[_COL_LOW]
            i_vol = idx[_COL_VOL]
            i_amt = idx[_COL_AMT]
        except KeyError:
            return
        for raw in reader:
            if len(raw) <= i_close:
                continue
            parsed = parse_vendor_ts(raw[i_time] if i_time < len(raw) else "")
            if parsed is None:
                continue
            day, hhmm = parsed
            if hhmm not in keep_times:
                continue
            close = _num(raw[i_close])
            if close is None or close <= 0:
                continue
            yield (
                day,
                hhmm,
                _num(raw[i_open]) if i_open < len(raw) else None,
                _num(raw[i_high]) if i_high < len(raw) else None,
                _num(raw[i_low]) if i_low < len(raw) else None,
                close,
                _num(raw[i_vol]) if i_vol < len(raw) else None,
                _num(raw[i_amt]) if i_amt < len(raw) else None,
            )


def _parse_file_payload(args: tuple[str, tuple[str, ...], str]) -> tuple[str, int, list[tuple]]:
    path_s, keep, source = args
    path = Path(path_s)
    ts_code = filename_to_ts_code(path.name)
    if ts_code is None:
        return path.name, 0, []
    keep_set = frozenset(keep)
    payload = [
        (ts_code, day, hhmm, o, h, low, c, vol, amt, source)
        for day, hhmm, o, h, low, c, vol, amt in _iter_last_hour_rows(path, keep_set)
    ]
    return ts_code, 1 if payload else 0, payload


def iter_csv_files(root: Path) -> Iterable[Path]:
    if root.is_file() and root.suffix.lower() == ".csv":
        yield root
        return
    if not root.is_dir():
        return
    yield from sorted(root.rglob("*.csv"))


def import_vendor_tree(
    root: Path,
    *,
    freq: int | None = None,
    source: str | None = None,
    workers: int = 6,
    keep_times: frozenset[str] | None = None,
) -> dict[str, int]:
    """Walk a folder (or one CSV) and upsert selected minute bars."""
    resolved = root.expanduser().resolve()
    freq_n = freq if freq in (5, 15) else detect_freq(resolved)
    src = source or source_for_freq(freq_n)
    keep = keep_times or times_for_freq(freq_n)
    files = [str(p) for p in iter_csv_files(resolved)]
    stats = {
        "files": len(files),
        "imported": 0,
        "stored": 0,
        "skipped": 0,
        "freq": freq_n,
    }
    logger.info(
        "import %s files=%s freq=%s source=%s workers=%s times=%s",
        resolved,
        len(files),
        freq_n,
        src,
        workers,
        ",".join(sorted(keep)),
    )
    buf: list[tuple] = []
    jobs = [(p, tuple(sorted(keep)), src) for p in files]
    done = 0
    last_ts = ""

    def _flush() -> None:
        nonlocal buf
        if not buf:
            return
        n = upsert_5min_payload(buf)
        stats["stored"] += n
        buf = []

    if workers <= 1 or len(jobs) < 8:
        for job in jobs:
            ts_code, imported, payload = _parse_file_payload(job)
            done += 1
            if imported:
                stats["imported"] += 1
                last_ts = ts_code
                buf.extend(payload)
            else:
                stats["skipped"] += 1
            if len(buf) >= FLUSH_ROWS:
                _flush()
            if done % 100 == 0 or done == len(jobs):
                logger.info("  %s/%s %s stored=%s", done, len(jobs), last_ts, stats["stored"] + len(buf))
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for ts_code, imported, payload in pool.map(
                _parse_file_payload, jobs, chunksize=16
            ):
                done += 1
                if imported:
                    stats["imported"] += 1
                    last_ts = ts_code
                    buf.extend(payload)
                else:
                    stats["skipped"] += 1
                if len(buf) >= FLUSH_ROWS:
                    _flush()
                if done % 100 == 0 or done == len(jobs):
                    logger.info(
                        "  %s/%s %s stored=%s",
                        done,
                        len(jobs),
                        last_ts,
                        stats["stored"] + len(buf),
                    )
    _flush()
    return stats
