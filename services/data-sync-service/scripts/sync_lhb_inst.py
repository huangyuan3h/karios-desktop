#!/usr/bin/env python3
"""Sync 龙虎榜机构买卖统计 (akshare stock_lhb_jgmmtj_em) 2021+.

Institutional-seat footprint on the daily 龙虎榜: per-stock 机构买入/卖出净额,
机构家数, 占成交额比, 上榜原因. Backfillable (date-ranged, long history).

Output: data/lhb/lhb_inst.csv

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_lhb_inst.py --start 202101 --end 202608
"""

from __future__ import annotations

import argparse
import calendar
import csv
import time
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "lhb" / "lhb_inst.csv"
RENAME = {"代码": "code", "名称": "name", "收盘价": "close", "涨跌幅": "pct_chg",
          "买方机构数": "buy_inst_n", "卖方机构数": "sell_inst_n",
          "机构买入总额": "inst_buy_amt", "机构卖出总额": "inst_sell_amt",
          "机构买入净额": "inst_net_amt", "市场总成交额": "mkt_amount",
          "机构净买额占总成交额比": "net_amt_ratio", "换手率": "turnover",
          "流通市值": "float_mv", "上榜原因": "reason", "上榜日期": "lhb_date"}
COLS = list(RENAME.values())


def _months(start: str, end: str) -> list[tuple[str, str]]:
    y0, m0 = int(start[:4]), int(start[4:6])
    y1, m1 = int(end[:4]), int(end[4:6])
    out = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        last = calendar.monthrange(y, m)[1]
        out.append((f"{y}{m:02d}01", f"{y}{m:02d}{last:02d}"))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="202101")
    ap.add_argument("--end", default="202608")
    args = ap.parse_args()

    import akshare as ak

    rows_out = []
    for s, e in _months(args.start, args.end):
        for attempt in range(3):
            try:
                df = ak.stock_lhb_jgmmtj_em(start_date=s, end_date=e)
                break
            except Exception as ex:  # noqa: BLE001
                if attempt == 2:
                    print(f"  FAIL {s}~{e}: {str(ex)[:80]}", flush=True)
                    df = None
                else:
                    time.sleep(2.0 * (attempt + 1))
        if df is None or df.empty:
            continue
        df = df.rename(columns=RENAME)
        keep = [c for c in COLS if c in df.columns]
        for r in df.to_dict("records"):
            rows_out.append({c: r.get(c, "") for c in COLS})
        print(f"  {s}~{e}: {len(df)} -> total {len(rows_out)}", flush=True)
        time.sleep(0.3)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows_out)
    print(f"\nwrote {len(rows_out)} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
