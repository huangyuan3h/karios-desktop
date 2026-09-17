#!/usr/bin/env python3
"""Phase-2 backfill: full-day 5min bars for 2021-2023 limit-up event days (baostock).

For H-BOARD-1 phase 2 (2021-23 needs full-day bars for first-seal / reopen
features; DB only has 14:30-15:00 for those years). Multi-process baostock
sessions; only fetch (symbol, year) pairs that actually contain eligible
limit-up events; only store event-day rows (new source `baostock_full`).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/backfill_5min_event_days.py --dry-run
  PYTHONPATH=src python3 scripts/backfill_5min_event_days.py --workers 6
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db.bar_5min import upsert_5min_payload  # noqa: E402
from data_sync_service.service.bar_5min import (  # noqa: E402
    rows_from_baostock,
    to_baostock_code,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
LIMIT = ROOT / "data" / "limit" / "stk_limit.csv"
SOURCE = "baostock_full"
START, END = "2021-01-01", "2023-12-31"
AMOUNT_FLOOR_QIAN = 70_000.0
MIN_LIST_DAYS = 60
MIN_BARS = 40
FLUSH_ROWS = 60_000


def _is_cn_stock(ts: str) -> bool:
    code = ts.split(".")[0]
    return ts.endswith((".SH", ".SZ")) and code[:2] in ("60", "00", "30", "68")


def _list_date_iso(raw: Any) -> str | None:
    s = str(raw or "")
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s[:10] if len(s) >= 10 else None


def _eligible_events() -> dict[str, dict[str, set[str]]]:
    """{ts_code: {year: {dates}}} for limit-up events passing the E3 universe."""
    lim: dict[tuple[str, str], float] = {}
    with LIMIT.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            d = str(r.get("trade_date") or "")
            if START <= d <= END:
                try:
                    lim[(d, str(r["ts_code"]))] = float(r["up_limit"])
                except (KeyError, TypeError, ValueError):
                    continue
    from data_sync_service.db import get_connection

    from hotmoney_lib import latest_adj

    sb: dict[str, tuple[str, str | None]] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, name, list_date FROM stock_basic")
            for ts, name, ld in cur.fetchall():
                sb[str(ts)] = (str(name or ""), _list_date_iso(ld))
        adj_latest = latest_adj(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, close, amount, adj_factor FROM daily "
                "WHERE trade_date >= %s AND trade_date <= %s",
                (START, END),
            )
            out: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
            for ts, d, c, amt, adj in cur.fetchall():
                ts = str(ts)
                if not _is_cn_stock(ts):
                    continue
                name, ld = sb.get(ts, ("", None))
                if "ST" in name or "退" in name:
                    continue
                up = lim.get((str(d), ts))
                if up is None or c is None or not adj:
                    continue
                raw = float(c) * adj_latest.get(ts, float(adj)) / float(adj)
                if raw < up - 0.011:
                    continue
                if amt is not None and float(amt) < AMOUNT_FLOOR_QIAN:
                    continue
                if ld is not None and ld > (
                    datetime.fromisoformat(str(d)) - timedelta(days=MIN_LIST_DAYS)
                ).date().isoformat():
                    continue
                out[ts][str(d)[:4]].add(str(d))
            return out


_BS_LOGGED = False


def _bs_login(retries: int = 4) -> None:
    global _BS_LOGGED
    import baostock as bs  # type: ignore[import-not-found]

    if _BS_LOGGED:
        return
    last = ""
    for attempt in range(retries):
        lg = bs.login()
        if str(lg.error_code) == "0":
            _BS_LOGGED = True
            return
        last = f"{lg.error_code}: {lg.error_msg}"
        time.sleep(20.0 * (attempt + 1))
    raise RuntimeError(f"baostock login failed: {last}")


def _worker(job: tuple[str, str, tuple[str, ...]]) -> tuple[str, str, list[tuple], str]:
    ts_code, year, dates = job
    import baostock as bs  # type: ignore[import-not-found]

    bs_code = to_baostock_code(ts_code)
    if bs_code is None:
        return ts_code, year, [], ""
    want = set(dates)
    try:
        _bs_login()
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,time,code,open,high,low,close,volume,amount",
            start_date=f"{year}-01-01",
            end_date=f"{year}-12-31",
            frequency="5",
            adjustflag="3",
        )
        if str(rs.error_code) != "0":
            return ts_code, year, [], f"{bs_code} {year}: {rs.error_msg}"[:120]
        raw: list[list[str]] = []
        while str(rs.error_code) == "0" and rs.next():
            raw.append(list(rs.get_row_data()))
    except Exception as exc:  # noqa: BLE001
        return ts_code, year, [], f"{bs_code} {year}: {str(exc)[:120]}"
    payload: list[tuple] = []
    for row in rows_from_baostock(raw):
        if row["trade_date"] not in want:
            continue
        payload.append(
            (
                ts_code,
                row["trade_date"],
                row["time"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["vol"],
                row["amount"],
                SOURCE,
            )
        )
    return ts_code, year, payload, ""


def _covered() -> set[tuple[str, str]]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, count(*) FROM bar_5min "
                "WHERE source = %s AND trade_date >= %s AND trade_date <= %s "
                "GROUP BY 1, 2 HAVING count(*) >= %s",
                (SOURCE, START, END, MIN_BARS),
            )
            return {(str(a), str(b)) for a, b, _n in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--limit-jobs", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    events = _eligible_events()
    jobs: list[tuple[str, str, tuple[str, ...]]] = []
    for ts, years in events.items():
        for year, dates in years.items():
            jobs.append((ts, year, tuple(sorted(dates))))
    n_events = sum(len(v) for y in events.values() for v in y.values())
    logger.info(
        "eligible events=%d symbols=%d jobs=%d", n_events, len(events), len(jobs)
    )
    if args.limit_jobs is not None:
        jobs = jobs[: args.limit_jobs]

    cov = _covered() if not args.dry_run else set()
    todo = [j for j in jobs if not all((j[0], d) in cov for d in j[2])]
    logger.info("covered jobs skipped=%d pending jobs=%d", len(jobs) - len(todo), len(todo))
    if args.dry_run:
        for j in jobs[:10]:
            logger.info("  job %s %s dates=%d", j[0], j[1], len(j[2]))
        return 0
    if not todo:
        return 0

    stored = 0
    failed = 0
    started = time.time()
    buf: list[tuple] = []

    def _flush() -> None:
        nonlocal buf, stored
        if buf:
            # OPT-211 P3: insert-only — fill missing event-day rows, never
            # rewrite an existing print (re-fetched vendor bars repriced
            # satellite 14:30 fills on 09-15).
            stored += upsert_5min_payload(buf, on_conflict="nothing")
            buf = []

    with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for i, (_ts, _year, payload, err) in enumerate(pool.map(_worker, todo, chunksize=4), 1):
            if err:
                failed += 1
                if failed <= 20:
                    logger.warning("  job failed: %s", err)
            buf.extend(payload)
            if len(buf) >= FLUSH_ROWS:
                _flush()
            if i % 50 == 0 or i == len(todo):
                rate = i / max(1e-9, time.time() - started)
                eta_h = (len(todo) - i) / max(1e-9, rate) / 3600
                logger.info(
                    "  %d/%d failed=%d stored=%d rate=%.2f jobs/s eta=%.1fh",
                    i, len(todo), failed, stored, rate, eta_h,
                )
    _flush()
    logger.info("done stored=%d failed=%d", stored, failed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
