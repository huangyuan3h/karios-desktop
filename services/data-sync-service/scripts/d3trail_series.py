"""Extract exit-day full-session 5-minute series from vendor zips (D-series).

Vendor minute CSVs (~/Downloads/{2024,2025,2026}_5min.zip, one file per
symbol-year) hold the full session; bar_5min in Postgres only keeps sparse
prints (1000/1330/1400/1430...). A day-3 conditional-order trail needs the
running high, so this module surgically extracts just the (ts, date) pairs
a replay actually exits on.

Row format per pair: [(hhmm, open, high, low, close), ...] ascending,
bars with hhmm <= "1430" only (the conditional order lives until the fixed
14:30 print; anything later is unknowable at decision time).
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

ZIP_DIR = Path.home() / "Downloads"
ZIP_NAMES = ("2024_5min.zip", "2025_5min.zip", "2026_5min.zip")
EXIT_CUTOFF = "1430"


def ts_to_member(ts: str, year: str) -> str:
    """'600000.SH' + '2026' -> 'sh600000_2026.csv'."""
    code, exch = ts.split(".")
    return f"{exch.lower()}{code}_{year}.csv"


def parse_member_rows(raw: bytes, day: str) -> list[tuple[str, float, float, float, float]]:
    """All 5-min bars for one date, ascending, hhmm <= 1430."""
    out: list[tuple[str, float, float, float, float]] = []
    text = raw.decode("utf-8-sig")
    for row in csv.DictReader(io.StringIO(text)):
        ts_raw = (row.get("时间") or "").strip().replace("/", "-")
        if len(ts_raw) < 16 or ts_raw[:10] != day:
            continue
        hhmm = ts_raw[11:13] + ts_raw[14:16]
        if hhmm > EXIT_CUTOFF:
            continue
        try:
            out.append(
                (
                    hhmm,
                    float(row["开盘价"]),
                    float(row["最高价"]),
                    float(row["最低价"]),
                    float(row["收盘价"]),
                )
            )
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda r: r[0])
    return out


def extract_series(pairs: set[tuple[str, str]]) -> dict[tuple[str, str], list]:
    """pairs = {(ts_code, 'YYYY-MM-DD')}. Returns {(ts, day): [bars]}.

    Missing file/date → pair absent (caller falls back to fixed-time exit
    and counts the fallback via exitPxSrc provenance).
    """
    by_zip: dict[str, list[tuple[str, str]]] = {}
    for ts, day in pairs:
        by_zip.setdefault(f"{day[:4]}_5min.zip", []).append((ts, day))
    out: dict[tuple[str, str], list] = {}
    for zip_name, items in sorted(by_zip.items()):
        path = ZIP_DIR / zip_name
        if not path.exists():
            logger.warning("missing vendor zip %s (%d pairs skipped)", path, len(items))
            continue
        with zipfile.ZipFile(path) as z:
            names = set(z.namelist())
            for ts, day in items:
                member = ts_to_member(ts, day[:4])
                if member not in names:
                    continue
                with z.open(member) as f:
                    rows = parse_member_rows(f.read(), day)
                if rows:
                    out[(ts, day)] = rows
    return out
