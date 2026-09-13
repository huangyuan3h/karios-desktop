#!/usr/bin/env python3
"""Merge per-CB akshare valuation files into one PIT table.

Input:  data/cb/valuation/<code>.csv   (from fetch_cb_valuation.py)
Output: data/cb/cb_valuation.csv       date,ts_code,close_em,conv_value,conv_prem_pct,
                                       pure_bond_value,pure_bond_prem_pct
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "data" / "cb"
VAL = ROOT / "valuation"
OUT = ROOT / "cb_valuation.csv"
DAILY = ROOT / "cb_daily.csv"


def _bare_to_ts() -> dict[str, str]:
    m: dict[str, str] = {}
    with DAILY.open() as fh:
        for r in csv.DictReader(fh):
            ts = r["ts_code"]
            m.setdefault(ts.split(".")[0], ts)
    return m


def main() -> int:
    b2t = _bare_to_ts()
    n = 0
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "ts_code", "close_em", "conv_value", "conv_prem_pct", "pure_bond_value", "pure_bond_prem_pct"])
        for p in sorted(VAL.glob("*.csv")):
            bare = p.stem
            ts = b2t.get(bare, bare)
            with p.open(encoding="utf-8") as sf:
                for row in csv.DictReader(sf):
                    d = row.get("date")
                    if not d:
                        continue
                    w.writerow([
                        d, ts, row.get("close_em", ""), row.get("conv_value", ""),
                        row.get("conv_prem_pct", ""), row.get("pure_bond_value", ""),
                        row.get("pure_bond_prem_pct", ""),
                    ])
                    n += 1
    print(f"merged {n} rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
