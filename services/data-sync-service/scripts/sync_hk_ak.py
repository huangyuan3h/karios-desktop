"""Manual one-shot sync for HK daily K-lines via akshare (preferred source).

akshare (Sina Finance) is the highest-priority HK data source: no per-call
rate cap, full history, ~0.2s/call. Use this when the watchlist shows
`no_bars` for HK tickers and you want a one-shot bootstrap before the daily
cron takes over.

Default backfill window is **5 years** (today − 5y). Pre-existing rows older
than the window are left untouched — we never DELETE history, only upsert.

Usage:
    cd services/data-sync-service
    PYTHONPATH=src python scripts/sync_hk_ak.py [--limit N] [--delay S] [--only-missing] [--years N]
"""

from __future__ import annotations

import argparse
import sys
import time

from data_sync_service.db.daily import get_last_trade_date
from data_sync_service.db.stock_basic import fetch_ts_codes_by_market
from data_sync_service.service.hk_daily_ak import sync_hk_daily_for_ts_code_ak

DEFAULT_BACKFILL_YEARS = 5


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync HK daily K-lines via akshare")
    parser.add_argument("--limit", type=int, default=0, help="Max number of symbols (0 = all)")
    parser.add_argument("--delay", type=float, default=0.2, help="Seconds between calls")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip symbols that already have daily rows",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=DEFAULT_BACKFILL_YEARS,
        help=f"First-time backfill window in years (default {DEFAULT_BACKFILL_YEARS})",
    )
    parser.add_argument("--symbol", action="append", default=[], help="Specific ts_code(s)")
    args = parser.parse_args(argv)

    if args.symbol:
        ts_codes = [s.strip().upper() for s in args.symbol]
    else:
        ts_codes = fetch_ts_codes_by_market("HK")

    if args.only_missing:
        before = len(ts_codes)
        ts_codes = [tc for tc in ts_codes if get_last_trade_date(tc) is None]
        print(f"--only-missing: filtered {before} -> {len(ts_codes)} stocks without daily bars")

    if args.limit > 0:
        ts_codes = ts_codes[: args.limit]
        print(f"--limit: processing first {len(ts_codes)} stocks")

    print(f"Total to sync: {len(ts_codes)} HK stocks (delay={args.delay}s, backfill_years={args.years})")
    print()

    succeeded = 0
    skipped = 0
    failed = 0
    total_rows = 0
    t0 = time.monotonic()

    for i, tc in enumerate(ts_codes, 1):
        try:
            r = sync_hk_daily_for_ts_code_ak(tc, backfill_years=args.years)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"[{i}/{len(ts_codes)}] {tc}: EXC {type(e).__name__}: {e}")
            continue
        ok = r.get("ok")
        if ok:
            updated = int(r.get("updated") or 0)
            total_rows += updated
            if updated > 0:
                succeeded += 1
                latest = r.get("latest_trade_date", "")
                print(f"[{i}/{len(ts_codes)}] {tc}: ok, +{updated} bars (latest={latest})")
            else:
                skipped += 1
                msg = str(r.get("message", ""))[:60]
                print(f"[{i}/{len(ts_codes)}] {tc}: skipped ({msg})")
        else:
            failed += 1
            err = str(r.get("error", ""))[:80]
            print(f"[{i}/{len(ts_codes)}] {tc}: FAIL {err}")
        if args.delay > 0 and i < len(ts_codes):
            time.sleep(args.delay)

    elapsed = time.monotonic() - t0
    print()
    print(f"=== Summary ===")
    print(f"Total processed: {len(ts_codes)}")
    print(f"Succeeded (new bars): {succeeded}")
    print(f"Skipped (no data): {skipped}")
    print(f"Failed: {failed}")
    print(f"Total rows upserted: {total_rows}")
    print(f"Elapsed: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())