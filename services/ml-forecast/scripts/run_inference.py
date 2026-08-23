#!/usr/bin/env python3
import sys, pathlib, torch
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/"src"))
import pandas as pd, numpy as np
from ml_forecast.data import load_bars, load_calendar, build_samples, load_rs_rank_map, load_extra_maps
from ml_forecast.features import build_feature_tensor, apply_norm
from ml_forecast.model import TCNForecast

import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--ckpt", required=True)
ap.add_argument("--n", type=int, default=10)
ap.add_argument("--x", type=float, default=5.0)
ap.add_argument("--L", type=int, default=60)
ap.add_argument("--windows", nargs="+", default=["valid","OOS2"])
ap.add_argument("--out", default="models/preds_full.parquet")
args = ap.parse_args()

ckpt = torch.load(args.ckpt, map_location="cpu", weights_only=False)
mean = ckpt["mean"]; std = ckpt["std"]
cfg = ckpt.get("args", {})
print(f"ckpt mean {mean[:2]} std {std[:2]} cfg {cfg}")
# infer hidden/levels from ckpt? use TCN 64 4 0.2
model = TCNForecast(in_dim=len(mean), hidden=cfg.get("hidden",64), levels=cfg.get("levels",4), dropout=cfg.get("dropout",0.2))
model.load_state_dict(ckpt["model"])
model.eval()
device = torch.device("cpu")

cal = load_calendar("2021-08-01","2026-08-07")
bars = load_bars("2021-08-01","2026-08-07", lookback_days=args.L+5)
print(f"bars {len(bars)} cal {len(cal)}")

# extra maps if 18 dim
rs_map = score_map = mv_map = turnover_map = None
if len(mean)==18:
    print("loading feat+ maps")
    rs_map = load_rs_rank_map("2021-08-01","2026-08-07")
    score_map, mv_map, turnover_map = load_extra_maps("2021-08-01","2026-08-07")
    feat_kwargs = dict(rs_map=rs_map, score_map=score_map, mv_map=mv_map, turnover_map=turnover_map)
else:
    feat_kwargs = {}

# build samples full
samples = build_samples(bars, cal, n_forward=args.n, x_pct=args.x)
print(f"samples {len(samples)}")

windows = {
    "OOS2": ("2024-08-01","2025-08-01"),
    "train": ("2025-08-01","2026-02-01"),
    "valid": ("2026-03-01","2026-08-07"),
}
all_preds=[]
for w in args.windows:
    s,e = windows[w]
    sub = samples[(samples["trade_date"]>=s)&(samples["trade_date"]<=e)]
    print(f"{w} {len(sub)}")
    # chunk by day
    for day, g in sub.groupby("trade_date"):
        if len(g)==0: continue
        # build features for this day's g
        X, y_reg, y_cls, days, tss = build_feature_tensor(bars, cal, g, L=args.L, **feat_kwargs)
        if len(X)==0: continue
        X = apply_norm(X, mean, std)
        # batch inference
        ds = torch.from_numpy(X)
        with torch.no_grad():
            # chunk 2048
            probs=[]
            regs=[]
            for i in range(0, len(ds), 2048):
                xb = ds[i:i+2048]
                pr, lg = model(xb)
                probs.append(torch.sigmoid(lg).numpy())
                regs.append((pr.numpy()*20))
            probs = np.concatenate(probs); regs=np.concatenate(regs)
        df = pd.DataFrame({"trade_date": days, "ts_code": tss, "pred_prob": probs, "pred_reg": regs, "label_cls": y_cls, "label_reg": y_reg, "window": w})
        all_preds.append(df)
        if len(all_preds)%50==0:
            print(f"  {w} {day} {len(g)} -> {len(df)} total {sum(len(d) for d in all_preds)}")

if all_preds:
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.concat(all_preds).to_parquet(out, index=False)
    print(f"saved {out} {len(pd.concat(all_preds))}")
