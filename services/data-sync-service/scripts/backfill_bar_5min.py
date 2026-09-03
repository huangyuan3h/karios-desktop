"""Backfill last-hour (14:30–15:00) 5-minute bars for CN A-shares.

Default source is baostock (1y per symbol in one query). Tushare stk_mins
has longer history but 1 call/min — only use for repair.

Usage:
    cd services/data-sync-service
    PYTHONPATH=src python3 scripts/backfill_bar_5min.py
    PYTHONPATH=src python3 scripts/backfill_bar_5min.py --start 2025-09-03 --end 2026-09-03
    PYTHONPATH=src python3 scripts/backfill_bar_5min.py --limit 20   # smoke
    PYTHONPATH=src python3 scripts/backfill_bar_5min.py --source tushare --limit 5
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from data_sync_service.service.bar_5min import (
    SOURCE_BAOSTOCK,
    SOURCE_TUSHARE,
    backfill_symbols,
    list_cn_a_share_codes,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _default_start() -> str:
    return (date.today() - timedelta(days=365)).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=_default_start())
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--source", choices=(SOURCE_BAOSTOCK, "tushare"), default=SOURCE_BAOSTOCK)
    ap.add_argument("--limit", type=int, default=None, help="Max symbols this run (resume-safe)")
    ap.add_argument("--sleep", type=float, default=None, help="Override inter-symbol sleep")
    ap.add_argument("--no-skip-covered", action="store_true")
    args = ap.parse_args()

    source = SOURCE_TUSHARE if args.source == "tushare" else SOURCE_BAOSTOCK
    codes = list_cn_a_share_codes()
    logger.info("universe %d A-shares %s..%s source=%s", len(codes), args.start, args.end, source)
    res = backfill_symbols(
        ts_codes=codes,
        start_date=args.start,
        end_date=args.end,
        source=source,
        max_symbols=args.limit,
        sleep_seconds=args.sleep,
        skip_covered=not args.no_skip_covered,
    )
    logger.info(
        "done pending=%s ok=%s stored=%s failed=%s skipped=%s",
        res["pending"], res["ok"], res["stored"], res["failed"], res["skipped"],
    )
    for err in res["errors"]:
        logger.warning("  %s", err)
    return 0 if res["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
