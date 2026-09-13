#!/usr/bin/env python3
"""Sync 股东户数 (tushare stk_holdernumber) 2021+ by ann_date month chunks.

Output: data/gdhs/gdhs.csv  ts_code,ann_date,end_date,holder_num

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_gdhs.py --start 202101 --end 202608
"""

from __future__ import annotations

import argparse
import calendar
import csv
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "gdhs" / "gdhs.csv"
COLS = ["ts_code", "ann_date", "end_date", "holder_num"]


def _months(start: str, end: str) -> list[tuple[str, str]]:
    y0, m0 = int(start[:4]), int(start[4:6])
    y1, m1 = int(end[:4]), int(end[4:6])
    out, y, m = [], y0, m0
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

    from data_sync_service.clients.tushare_pool import get_pool

    pro = get_pool().pro()
    seen: set[tuple] = set()
    rows: list[dict] = []
    for s, e in _months(args.start, args.end):
        try:
            df = pro.stk_holdernumber(start_date=s, end_date=e)
        except Exception as ex:  # noqa: BLE001
            print(f"  FAIL {s}~{e}: {str(ex)[:90]}", flush=True)
            continue
        if df is None or df.empty:
            continue
        for r in df.itertuples():
            key = (str(r.ts_code), str(r.ann_date), str(r.end_date))
            if key in seen:
                continue
            seen.add(key)
            rows.append({"ts_code": key[0], "ann_date": key[1], "end_date": key[2],
                         "holder_num": getattr(r, "holder_num", "")})
        print(f"  {s}~{e}: {len(df)} -> total {len(rows)}", flush=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        w.writerows(rows)
    print(f"\nwrote {len(rows)} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
