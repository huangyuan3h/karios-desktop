#!/usr/bin/env python3
"""Build dataset: PG -> samples -> features -> Parquet cache + npz for train."""

import argparse
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from ml_forecast.data import load_bars, load_calendar, build_samples
from ml_forecast.features import build_feature_tensor, compute_norm_stats, apply_norm
import numpy as np

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20, help="forward N days")
    ap.add_argument("--x", type=float, default=8.0, help="threshold X %")
    ap.add_argument("--L", type=int, default=60)
    ap.add_argument("--start", default="2021-08-01")
    ap.add_argument("--end", default="2026-08-07")
    args = ap.parse_args()
    cal = load_calendar(args.start, args.end)
    print(f"calendar {len(cal)} {cal[0]}->{cal[-1]}")
    bars = load_bars(args.start, args.end, lookback_days=args.L+5)
    print(f"bars {len(bars)} ts {bars['ts_code'].nunique()}")
    samples = build_samples(bars, cal, n_forward=args.n, x_pct=args.x)
    print(f"samples {len(samples)} pos_rate {samples['label_cls'].mean():.3f} pos {samples['label_cls'].sum()}")
    print(samples.head())
    # save raw samples
    out_dir = Path(__file__).resolve().parents[1] / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    samples.to_parquet(out_dir / f"samples_N{args.n}_X{args.x}.parquet", index=False)
    print(f"saved {out_dir / f'samples_N{args.n}_X{args.x}.parquet'}")
    # build features (heavy, 250w *60*14 ~ 8GB) - do in chunks by split to save RAM
    # calendar splits
    splits = {
        "train": ("2025-08-01","2026-02-01"),
        "OOS2": ("2024-08-01","2025-08-01"),
        "valid": ("2026-03-01","2026-08-07"),
        "holdout": ("2026-08-08","2026-08-22"),
    }
    # quick check: filter samples per split
    for name,(s,e) in splits.items():
        sub = samples[(samples["trade_date"]>=s)&(samples["trade_date"]<=e)]
        print(f"{name} {s}->{e} n={len(sub)} pos={sub['label_cls'].mean():.3f}" if len(sub)>0 else f"{name} n=0")

if __name__=="__main__":
    main()
