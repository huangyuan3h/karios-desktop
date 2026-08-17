"""Backfill stock_forecast (业绩预告) — P14 PEAD data.

Tushare ``forecast`` requires ann_date (one day per call) — sync is
day-by-day inside sync_forecast_for_dates with retry + 0.5s sleep.
This script slices the range into 10-day windows so progress is visible
and resumable (idempotent).

Usage:
    PYTHONPATH=src python3 scripts/backfill_forecast.py [--start 2024-01-01] [--end 2026-08-07]
"""

from __future__ import annotations

import argparse
import sys
import time

from data_sync_service.db.stock_forecast import sync_forecast_for_dates


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2024-01-01")
    ap.add_argument("--end", default="2026-08-07")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()

    from datetime import date as _date
    from datetime import timedelta

    d = _date.fromisoformat(args.start)
    end = _date.fromisoformat(args.end)
    total = 0
    while d <= end:
        w_end = min(d + timedelta(days=9), end)
        n = sync_forecast_for_dates(d.isoformat(), w_end.isoformat())
        total += n
        print(f"  {d.isoformat()}..{w_end.isoformat()}: +{n} (cum {total})", flush=True)
        d = w_end + timedelta(days=1)
        time.sleep(args.sleep)
    print(f"done: {total} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
