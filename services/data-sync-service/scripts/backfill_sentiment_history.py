"""Backfill CN market sentiment history (TIP-014 · data-gap-backfill 方案 A).

`market_cn_sentiment_daily` starts 2026-01-05; OOS2/train windows
(2024-08..2025-12) have no sentiment data, so neutral_block / entry_style
auto / max_hold_env_shorten cannot replay those windows. All inputs of
compute_cn_sentiment_for_date come from the local `daily` table + the free
tushare daily API — so the history is fully recomputable at zero cost.

Usage:
    PYTHONPATH=src python3 scripts/backfill_sentiment_history.py [start] [end]
    # default: 2024-08-01 .. 2025-12-31 (one trading day before live data)

Verification:
    1. Recompute 2026-01-05 (already in DB) → identical up/down/risk_mode.
    2. Backfilled rows use the same _persist_sentiment_for_date chain as live.
    3. Idempotent: upsert on (date) — safe to re-run.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from data_sync_service.service.market_sentiment import (
    _persist_sentiment_for_date,
    is_cn_trading_day,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_START = "2024-08-01"
DEFAULT_END = "2025-12-31"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("start", nargs="?", default=DEFAULT_START)
    ap.add_argument("end", nargs="?", default=DEFAULT_END)
    args = ap.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if start > end:
        logger.error("start %s > end %s", args.start, args.end)
        return 2

    # Sanity: never overwrite live data (2026-01-05 onwards).
    if start >= date(2026, 1, 5):
        logger.error("refusing to touch live data window (>= 2026-01-05)")
        return 2

    ok = 0
    failed: list[str] = []
    skipped = 0
    d = start
    while d <= end:
        if not is_cn_trading_day(d):
            d += timedelta(days=1)
            continue
        try:
            _persist_sentiment_for_date(d.isoformat())
            ok += 1
            if ok % 20 == 0:
                logger.info("progress: %s (%d ok)", d.isoformat(), ok)
        except Exception as exc:  # noqa: BLE001
            failed.append(f"{d.isoformat()}: {str(exc)[:120]}")
            logger.warning("failed %s: %s", d.isoformat(), str(exc)[:120])
        d += timedelta(days=1)

    logger.info("done: ok=%d failed=%d skipped(non-trading)=%d", ok, len(failed), skipped)
    for f in failed[:10]:
        logger.warning("  FAIL %s", f)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
