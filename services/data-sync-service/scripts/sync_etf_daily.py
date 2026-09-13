#!/usr/bin/env python3
"""Sync a curated CN broad + sector ETF daily panel (adjusted) via tushare.

Local `daily` has only a handful of funds and must NOT be polluted (the S-3
universe would ingest ETFs as candidates). So ETFs live in a separate read-only
file under data/etf/ (gitignored), used by research scripts only.

Sources: tushare `fund_daily` (OHLCV) + `fund_adj` (复权因子).
qfq-relative return = close(t)*adj(t) / close(t0)*adj(t0)  (constant scale cancels).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_etf_daily.py --start 20210101 --end 20260807
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "etf"

# ts_code -> (label, kind)  kind: broad | sector
UNIVERSE: dict[str, tuple[str, str]] = {
    # broad
    "510050.SH": ("上证50", "broad"),
    "510300.SH": ("沪深300", "broad"),
    "510500.SH": ("中证500", "broad"),
    "159915.SZ": ("创业板", "broad"),
    "588000.SH": ("科创50", "broad"),
    "159949.SZ": ("创业板50", "broad"),
    # twin-star menu / duration assets (rate-sensitive)
    "518880.SH": ("黄金", "broad"),
    "513350.SH": ("原油", "broad"),
    "513100.SH": ("纳指100", "broad"),
    "513110.SH": ("纳指100b", "broad"),
    "513180.SH": ("恒生科技", "broad"),
    "511260.SH": ("十年国债", "broad"),
    # sector
    "512880.SH": ("证券", "sector"),
    "512800.SH": ("银行", "sector"),
    "512480.SH": ("半导体", "sector"),
    "512760.SH": ("芯片", "sector"),
    "159995.SZ": ("半导体芯片", "sector"),
    "515880.SH": ("通信", "sector"),
    "515000.SH": ("科技龙头", "sector"),
    "512720.SH": ("计算机", "sector"),
    "512010.SH": ("医药", "sector"),
    "512170.SH": ("医疗", "sector"),
    "159928.SZ": ("消费", "sector"),
    "512690.SH": ("酒", "sector"),
    "515170.SH": ("食品饮料", "sector"),
    "515030.SH": ("新能源车", "sector"),
    "515790.SH": ("光伏", "sector"),
    "516160.SH": ("新能源", "sector"),
    "159611.SZ": ("电力公用", "sector"),
    "512660.SH": ("军工", "sector"),
    "512400.SH": ("有色金属", "sector"),
    "515220.SH": ("煤炭", "sector"),
    "515210.SH": ("钢铁", "sector"),
    "512200.SH": ("房地产", "sector"),
    "512980.SH": ("传媒", "sector"),
    "512580.SH": ("环保", "sector"),
    "159865.SZ": ("畜牧养殖", "sector"),
    "512070.SH": ("非银金融", "sector"),
    "516110.SH": ("汽车", "sector"),
    "159766.SZ": ("旅游", "sector"),
    "159869.SZ": ("动漫游戏", "sector"),
}

FIELDS = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount", "adj_factor", "close_adj"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="20210101")
    ap.add_argument("--end", default="20260807")
    args = ap.parse_args()

    from data_sync_service.clients.tushare_pool import get_pool

    pro = get_pool().pro()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []
    meta: list[tuple[str, str, str]] = []
    for ts, (label, kind) in UNIVERSE.items():
        try:
            px = pro.fund_daily(ts_code=ts, start_date=args.start, end_date=args.end)
            adj = pro.fund_adj(ts_code=ts, start_date=args.start, end_date=args.end)
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {ts} {label}: {str(e)[:120]}", flush=True)
            continue
        adj_map = {str(r.trade_date): float(r.adj_factor) for r in adj.itertuples()} if len(adj) else {}
        n = 0
        for r in px.itertuples():
            d = str(r.trade_date)
            f = adj_map.get(d)
            if f is None:
                continue
            close = float(r.close)
            rows.append({
                "ts_code": ts, "trade_date": d, "open": float(r.open), "high": float(r.high),
                "low": float(r.low), "close": close, "pre_close": float(r.pre_close),
                "vol": float(r.vol), "amount": float(r.amount), "adj_factor": f, "close_adj": round(close * f, 6),
            })
            n += 1
        meta.append((ts, label, kind))
        print(f"  {ts:<11} {label:<8} {kind:<6} n={n}", flush=True)

    with (OUT_DIR / "etf_daily.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    with (OUT_DIR / "etf_meta.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["ts_code", "label", "kind"])
        w.writerows(meta)
    print(f"\nwrote {len(rows)} rows / {len(meta)} ETFs -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
