#!/usr/bin/env python3
"""Backfill daily limit prices (tushare stk_limit) 2021+ for CN A-shares.

Exact 涨停/跌停 prices per stock-day: needed to tell "open at limit" (unbuyable)
from ordinary opens, and to find sealed boards for the hot-money anatomy study.

Output: data/limit/stk_limit.csv (resume-safe: existing dates are skipped).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_stk_limit.py --limit 5     # smoke
  PYTHONPATH=src python3 scripts/sync_stk_limit.py --start 2021-01-01
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.clients.tushare_pool import get_pool  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUT = Path(__file__).resolve().parents[1] / "data" / "limit" / "stk_limit.csv"
COLS = ["trade_date", "ts_code", "up_limit", "down_limit"]
DEFAULT_START = "2021-01-01"
DEFAULT_SLEEP = 0.5


def _trading_days(start: str, end: str) -> list[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT trade_date FROM daily
                WHERE ts_code = '000001.SZ' AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (start, end),
            )
            return [str(r[0]) for r in cur.fetchall()]


def _load_done(path: Path) -> set[str]:
    if not path.exists():
        return set()
    done: set[str] = set()
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            d = str(row.get("trade_date") or "").strip()
            if d:
                done.add(d)
    return done


def _fetch_day(pro: Any, trade_date: str, *, retries: int = 3) -> list[dict[str, Any]]:
    compact = trade_date.replace("-", "")
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            df = pro.stk_limit(trade_date=compact)
            if df is None or df.empty:
                return []
            return [{c: rec.get(c, "") for c in COLS} for rec in df.to_dict("records")]
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            wait = 2.0 * (attempt + 1)
            logger.warning("  retry %s in %.0fs: %s", compact, wait, str(exc)[:120])
            time.sleep(wait)
    raise RuntimeError(f"stk_limit {compact} failed: {str(last_err)[:200]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--limit", type=int, default=None, help="Max trading days this run")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP, help="Seconds between days")
    args = ap.parse_args()

    from datetime import date

    end = args.end or date.today().isoformat()
    days = _trading_days(args.start, end)
    done = _load_done(OUT)
    pending = [d for d in days if d not in done]
    if args.limit is not None:
        pending = pending[: args.limit]
    logger.info(
        "days=%d done=%d pending=%d source=stk_limit out=%s", len(days), len(done), len(pending), OUT
    )
    if not pending:
        return 0

    OUT.parent.mkdir(parents=True, exist_ok=True)
    is_new = not OUT.exists()
    pro = get_pool().pro()
    stored = 0
    failed = 0
    with OUT.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLS)
        if is_new:
            writer.writeheader()
        for i, day in enumerate(pending):
            try:
                rows = _fetch_day(pro, day)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                logger.warning("FAIL %s: %s", day, str(exc)[:160])
                continue
            for row in rows:
                row["trade_date"] = day
                writer.writerow(row)
            fh.flush()
            stored += len(rows)
            if (i + 1) % 20 == 0 or (i + 1) == len(pending):
                logger.info("  %d/%d %s rows_day=%d total=%d", i + 1, len(pending), day, len(rows), stored)
            if args.sleep > 0:
                time.sleep(args.sleep)
    logger.info("done stored=%d failed_days=%d -> %s", stored, failed, OUT)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
