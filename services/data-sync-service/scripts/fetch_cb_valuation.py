#!/usr/bin/env python3
"""Fetch PIT convertible-bond valuation history (akshare/东财) for CB universe.

tushare cb_price_chg / cb_call need a higher tier (no access), so use akshare
`bond_zh_cov_value_analysis(symbol)` = per-CB daily history of
  日期, 收盘价, 纯债价值, 转股价值, 转股溢价率, 纯债溢价率
which is point-in-time (no look-ahead). Needed for the 双低 experiment.

Per-CB cache under data/cb/valuation/<code>.csv (resume-safe), merged by
scripts/merge_cb_valuation.py. Read-only w.r.t. the live system.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/fetch_cb_valuation.py --csv data/cb/cb_daily.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "cb" / "valuation"
COLS = {"日期": "date", "收盘价": "close_em", "纯债价值": "pure_bond_value",
        "转股价值": "conv_value", "转股溢价率": "conv_prem_pct", "纯债溢价率": "pure_bond_prem_pct"}


def _codes(csv_path: Path) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    with csv_path.open() as fh:
        for r in csv.DictReader(fh):
            bare = r["ts_code"].split(".")[0]
            if bare not in seen:
                seen.add(bare)
                codes.append(bare)
    return codes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default=str(Path(__file__).resolve().parents[1] / "data" / "cb" / "cb_daily.csv"))
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--retries", type=int, default=3)
    args = ap.parse_args()

    import akshare as ak

    codes = _codes(Path(args.csv))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"{len(codes)} CB codes -> {OUT_DIR}", flush=True)
    ok = skip = fail = 0
    failures: list[str] = []
    for i, code in enumerate(codes):
        dst = OUT_DIR / f"{code}.csv"
        if dst.exists() and dst.stat().st_size > 0:
            skip += 1
            continue
        for attempt in range(args.retries):
            try:
                df = ak.bond_zh_cov_value_analysis(symbol=code)
                if df is None or df.empty:
                    raise RuntimeError("empty")
                df = df.rename(columns=COLS)
                keep = [c for c in COLS.values() if c in df.columns]
                df[keep].to_csv(dst, index=False)
                ok += 1
                break
            except Exception as e:  # noqa: BLE001
                if attempt == args.retries - 1:
                    fail += 1
                    failures.append(code)
                    print(f"  FAIL {code}: {str(e)[:120]}", flush=True)
                else:
                    time.sleep(2.0 * (attempt + 1))
        if i % 50 == 0:
            print(f"  {i}/{len(codes)} ok={ok} skip={skip} fail={fail}", flush=True)
        time.sleep(args.sleep)

    print(f"\ndone: ok={ok} skip={skip} fail={fail}")
    if failures:
        print("failures:", ",".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
