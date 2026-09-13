#!/usr/bin/env python3
"""Sync 大宗交易 (tushare block_trade) 2021+ by trading day.

Output: data/block/block_trade.csv  ts_code,trade_date,price,vol,amount,buyer,seller

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_block_trade.py --start 20210101 --end 20260831
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "block" / "block_trade.csv"
ETF = Path(__file__).resolve().parents[1] / "data" / "etf" / "etf_daily.csv"
COLS = ["ts_code", "trade_date", "price", "vol", "amount", "buyer", "seller"]


def _calendar(start: str, end: str) -> list[str]:
    days = set()
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            if start <= iso.replace("-", "") <= end:
                days.add(iso.replace("-", ""))
    return sorted(days)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260831")
    args = ap.parse_args()

    from data_sync_service.clients.tushare_pool import get_pool

    pro = get_pool().pro()
    days = _calendar(args.start, args.end)
    print(f"{len(days)} trading days", flush=True)
    rows: list[dict] = []
    empty = 0
    for i, d in enumerate(days):
        try:
            df = pro.block_trade(trade_date=d)
        except Exception as ex:  # noqa: BLE001
            print(f"  FAIL {d}: {str(ex)[:80]}", flush=True)
            continue
        if df is None or df.empty:
            empty += 1
            continue
        for r in df.itertuples():
            rows.append({"ts_code": str(r.ts_code), "trade_date": str(r.trade_date),
                         "price": getattr(r, "price", ""), "vol": getattr(r, "vol", ""),
                         "amount": getattr(r, "amount", ""), "buyer": getattr(r, "buyer", ""),
                         "seller": getattr(r, "seller", "")})
        if i % 100 == 0:
            print(f"  {i}/{len(days)} rows={len(rows)}", flush=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows)
    inst = sum(1 for r in rows if "机构专用" in str(r["buyer"]) or "机构专用" in str(r["seller"]))
    print(f"\nwrote {len(rows)} rows (empty days {empty}); 含机构专用 {inst} -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
