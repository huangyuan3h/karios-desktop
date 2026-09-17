#!/usr/bin/env python3
"""Import full-day 5min bars for limit-up event days from local vendor CSVs.

Runs against repo data/{2024,2025,2026}_5min (48 bars/day, 09:35-15:00). We keep
ONLY rows whose (ts_code, date) is a limit-up event day (daily close == exact
up_limit from data/limit/stk_limit.csv), which shrinks ~180M rows to a few M.

Purpose: H-BOARD-1 board-quality features (first seal / sealed share / reopen /
tail seal) without a full-market 5min re-import.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/import_5min_event_days.py --dry-run --limit-files 50
  PYTHONPATH=src python3 scripts/import_5min_event_days.py            # full run
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db.bar_5min import upsert_5min_payload  # noqa: E402
from data_sync_service.service.ext_minute_csv import (  # noqa: E402
    filename_to_ts_code,
    iter_csv_files,
    parse_vendor_ts,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT.parents[1] / "data"
LIMIT = ROOT / "data" / "limit" / "stk_limit.csv"
SOURCE = "ext_5min"
FLUSH_ROWS = 60_000

MORNING = [f"{h:02d}{m:02d}" for h in (9, 10, 11) for m in range(0, 60, 5)]
AFTERNOON = [f"{h:02d}{m:02d}" for h in (13, 14) for m in range(0, 60, 5)]
ALL_TIMES = frozenset(
    [t for t in MORNING if "0930" < t <= "1130"] + [t for t in AFTERNOON if "1300" < t <= "1455"]
    + ["1500"]
)


def _limit_map(start: str, end: str) -> dict[tuple[str, str], float]:
    lim: dict[tuple[str, str], float] = {}
    if not LIMIT.exists():
        raise SystemExit(f"missing {LIMIT} (run scripts/sync_stk_limit.py first)")
    with LIMIT.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            d = str(r.get("trade_date") or "")
            if start <= d <= end:
                try:
                    lim[(d, str(r["ts_code"]))] = float(r["up_limit"])
                except (KeyError, TypeError, ValueError):
                    continue
    return lim


def _events(start: str, end: str) -> dict[str, set[str]]:
    from data_sync_service.db import get_connection

    from hotmoney_lib import latest_adj

    lim = _limit_map(start, end)
    logger.info("limit map rows in range: %d", len(lim))
    by_ts: dict[str, set[str]] = defaultdict(set)
    with get_connection() as conn:
        adj_latest = latest_adj(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, close, adj_factor FROM daily "
                "WHERE trade_date >= %s AND trade_date <= %s AND ts_code ~ '\\.(SH|SZ)$'",
                (start, end),
            )
            for ts, d, c, adj in cur.fetchall():
                up = lim.get((str(d), str(ts)))
                if up is None or c is None or not adj:
                    continue
                # lesson 2026-09-15: daily is qfq, limits are raw
                raw = float(c) * adj_latest.get(str(ts), float(adj)) / float(adj)
                if raw >= up - 0.011:
                    by_ts[str(ts)].add(str(d))
    return by_ts


def _rows_for_file(path: Path, want_dates: set[str]) -> list[tuple]:
    ts = filename_to_ts_code(path.name)
    if ts is None or not want_dates:
        return []
    keep: list[tuple] = []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return []
        idx = {h.lstrip("\ufeff").strip(): i for i, h in enumerate(header)}
        try:
            i_t, i_o, i_c, i_h, i_l = (
                idx["时间"],
                idx["开盘价"],
                idx["收盘价"],
                idx["最高价"],
                idx["最低价"],
            )
            i_v, i_a = idx["成交量"], idx["成交额"]
        except KeyError:
            return []
        keep_ts = ts
        for raw in reader:
            if len(raw) <= max(i_c, i_a, i_t, i_l, i_h):
                continue
            parsed = parse_vendor_ts(raw[i_t])
            if parsed is None:
                continue
            day, hhmm = parsed
            if day not in want_dates or hhmm not in ALL_TIMES:
                continue
            try:
                close = float(raw[i_c])
                open_ = float(raw[i_o])
                high = float(raw[i_h])
                low = float(raw[i_l])
                vol = float(raw[i_v]) if raw[i_v] else None
                amt = float(raw[i_a]) if raw[i_a] else None
            except (TypeError, ValueError):
                continue
            if close <= 0:
                continue
            keep.append((keep_ts, day, hhmm, open_, high, low, close, vol, amt, SOURCE))
    return keep


def _discover() -> list[Path]:
    out: list[Path] = []
    if not DATA_ROOT.is_dir():
        return out
    for p in sorted(DATA_ROOT.iterdir()):
        if p.is_dir() and ("5min" in p.name or "5分钟" in p.name):
            out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-09-11")
    ap.add_argument("--limit-files", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--path", type=Path, default=None, help="One vendor folder")
    args = ap.parse_args()

    roots = [args.path] if args.path else _discover()
    if not roots:
        logger.error("no *5min* folders under %s", DATA_ROOT)
        return 1

    by_ts = _events(args.start, args.end)
    n_events = sum(len(v) for v in by_ts.values())
    logger.info("limit-up event days: %d stock-days across %d symbols", n_events, len(by_ts))
    if n_events == 0:
        return 1

    files: list[Path] = []
    for root in roots:
        files.extend(iter_csv_files(root))
    if args.limit_files is not None:
        files = files[: args.limit_files]
    logger.info("files=%d roots=%s", len(files), [str(r) for r in roots])

    buf: list[tuple] = []
    kept = 0
    written = 0
    hit_files = 0
    for i, path in enumerate(files, 1):
        ts = filename_to_ts_code(path.name)
        want = by_ts.get(ts or "", set())
        rows = _rows_for_file(path, want)
        if rows:
            hit_files += 1
            kept += len(rows)
            buf.extend(rows)
        if len(buf) >= FLUSH_ROWS:
            if not args.dry_run:
                # OPT-211 P3: insert-only (see backfill_5min_event_days.py).
                written += upsert_5min_payload(buf, on_conflict="nothing")
            else:
                written += len(buf)
            buf = []
        if i % 500 == 0 or i == len(files):
            logger.info("  %d/%d hit_files=%d kept=%d written=%d", i, len(files), hit_files, kept, written)
    if buf:
        if not args.dry_run:
            written += upsert_5min_payload(buf, on_conflict="nothing")
        else:
            written += len(buf)
    logger.info(
        "done files=%d hit_files=%d kept=%d written=%d dry_run=%s",
        len(files),
        hit_files,
        kept,
        written,
        args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
