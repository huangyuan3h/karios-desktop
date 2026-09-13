#!/usr/bin/env python3
"""Sync CN IPO calendar (tushare new_share) for the 打新/new-share line.

Fields: ipo_date, issue_date, price, pe, amount(发行总量), market_amount(网上发行),
limit_amount(顶格申购需配市值,万元), funds(募资,亿元), ballot(中签率,%).

Output: data/ipo/new_share.csv (gitignored). Read-only research input.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_new_share.py --start 20200101 --end 20260831
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "ipo"
COLS = ["ts_code", "sub_code", "name", "ipo_date", "issue_date", "amount",
        "market_amount", "price", "pe", "limit_amount", "funds", "ballot"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="20200101")
    ap.add_argument("--end", default="20260831")
    args = ap.parse_args()

    from data_sync_service.clients.tushare_pool import get_pool

    pro = get_pool().pro()
    df = pro.new_share(start_date=args.start, end_date=args.end)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    keep = [c for c in COLS if c in df.columns]
    df[keep].to_csv(OUT_DIR / "new_share.csv", index=False)
    print(f"new_share rows={len(df)} -> {OUT_DIR / 'new_share.csv'}")
    print(f"date range: {df['ipo_date'].min()} ~ {df['ipo_date'].max()}")
    print(f"with ballot: {df['ballot'].notna().sum()}/{len(df)}; with price: {df['price'].notna().sum()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
