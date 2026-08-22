#!/usr/bin/env python3
"""Full pipeline: build features for 3 windows, train TCN, report AUC/IC, save ckpt."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import numpy as np
import pandas as pd
from ml_forecast.data import load_bars, load_calendar, build_samples
from ml_forecast.features import build_feature_tensor, compute_norm_stats, apply_norm
from ml_forecast.train import train_one_model
from ml_forecast.evaluate import evaluate_offline

import argparse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--x", type=float, default=8.0)
    ap.add_argument("--L", type=int, default=60)
    ap.add_argument("--epochs", type=int, default=60)
    ap.add_argument("--model", choices=["tcn","lstm"], default="tcn")
    args = ap.parse_args()
    cal = load_calendar("2021-08-01","2026-08-07")
    print(f"calendar {len(cal)}")
    bars = load_bars("2021-08-01","2026-08-07", lookback_days=args.L+5)
    print(f"bars {len(bars)}")
    samples = build_samples(bars, cal, n_forward=args.n, x_pct=args.x)
    print(f"samples total {len(samples)} pos {samples['label_cls'].mean():.3f}")

    def split_samples(s,e):
        sub = samples[(samples["trade_date"]>=s)&(samples["trade_date"]<=e)]
        return sub

    train_s = split_samples("2025-08-01","2026-02-01")
    valid_s = split_samples("2026-03-01","2026-08-07")
    oos2_s = split_samples("2024-08-01","2025-08-01")
    print(f"train {len(train_s)} valid {len(valid_s)} OOS2 {len(oos2_s)}")
    # downsample for quick iteration (keep pos stratify)
    def downsample(df, nmax, seed=42):
        if len(df) <= nmax:
            return df
        # stratify: keep pos rate
        pos = df[df["label_cls"]==1]
        neg = df[df["label_cls"]==0]
        pos_n = int(nmax * len(pos)/len(df))
        neg_n = nmax - pos_n
        return pd.concat([pos.sample(n=pos_n, random_state=seed), neg.sample(n=neg_n, random_state=seed)]).sample(frac=1, random_state=seed)
    train_s = downsample(train_s, 20000)
    valid_s = downsample(valid_s, 10000)
    oos2_s = downsample(oos2_s, 10000)
    print(f"downsampled train {len(train_s)} valid {len(valid_s)} OOS2 {len(oos2_s)}")

    # build tensors per split (to avoid OOM, build sequentially)
    print("building train features...")
    X_train, y_reg_train, y_cls_train, _, _ = build_feature_tensor(bars, cal, train_s, L=args.L)
    print(f"X_train {X_train.shape} y_pos {y_cls_train.mean():.3f}")
    print("building valid features...")
    X_valid, y_reg_valid, y_cls_valid, valid_days, valid_tss = build_feature_tensor(bars, cal, valid_s, L=args.L)
    print(f"X_valid {X_valid.shape}")
    print("building OOS2 features...")
    X_oos2, y_reg_oos2, y_cls_oos2, oos2_days, oos2_tss = build_feature_tensor(bars, cal, oos2_s, L=args.L)
    print(f"X_oos2 {X_oos2.shape}")

    # norm by train
    mean, std = compute_norm_stats(X_train)
    print(f"norm mean {mean[:3]} std {std[:3]}")
    X_train = apply_norm(X_train, mean, std)
    X_valid = apply_norm(X_valid, mean, std) if len(X_valid)>0 else X_valid
    X_oos2 = apply_norm(X_oos2, mean, std) if len(X_oos2)>0 else X_oos2

    # pos_weight
    pos_rate = y_cls_train.mean()
    pos_weight = (1-pos_rate)/max(pos_rate,1e-6)
    print(f"pos_rate {pos_rate:.3f} pos_weight {pos_weight:.2f}")

    model, best_auc, preds_reg_valid, preds_prob_valid, trues_reg_valid, trues_cls_valid = train_one_model(
        X_train, y_reg_train, y_cls_train, X_valid, y_reg_valid, y_cls_valid,
        model_type=args.model, epochs=args.epochs, batch=1024, pos_weight=pos_weight
    )
    print(f"\n=== Valid offline ===")
    metrics_valid = evaluate_offline(preds_prob_valid, preds_reg_valid, trues_cls_valid, trues_reg_valid)
    for k,v in metrics_valid.items():
        print(f"{k}: {v:.4f}" if isinstance(v,float) else f"{k}: {v}")

    # OOS2 inference
    import torch
    device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")
    if torch.cuda.is_available(): device=torch.device("cuda")
    model.to(device); model.eval()
    from torch.utils.data import DataLoader, TensorDataset
    if len(X_oos2)>0:
        ds = TensorDataset(torch.from_numpy(X_oos2))
        loader = DataLoader(ds, batch_size=2048)
        preds_reg_oos=[]; preds_prob_oos=[]
        with torch.no_grad():
            for (xb,) in loader:
                pr, lg = model(xb.to(device))
                preds_reg_oos.append((pr.cpu().numpy()*20))
                preds_prob_oos.append(torch.sigmoid(lg).cpu().numpy())
        preds_reg_oos=np.concatenate(preds_reg_oos); preds_prob_oos=np.concatenate(preds_prob_oos)
        metrics_oos = evaluate_offline(preds_prob_oos, preds_reg_oos, y_cls_oos2, y_reg_oos2)
        print(f"\n=== OOS2 offline (BLIND) ===")
        for k,v in metrics_oos.items():
            print(f"{k}: {v:.4f}" if isinstance(v,float) else f"{k}: {v}")
    else:
        metrics_oos={}
        preds_prob_oos=None

    # save ckpt + predictions
    out = Path(__file__).resolve().parents[1] / "models"
    out.mkdir(parents=True, exist_ok=True)
    ckpt_path = out / f"{args.model}_N{args.n}_X{args.x}_L{args.L}_auc{best_auc:.3f}.pt"
    torch.save({"model": model.state_dict(), "mean": mean, "std": std, "args": vars(args)}, ckpt_path)
    print(f"saved {ckpt_path}")

    # save predictions for backtest join
    pred_valid_df = pd.DataFrame({"trade_date": valid_days, "ts_code": valid_tss, "pred_prob": preds_prob_valid, "pred_reg": preds_reg_valid, "label_cls": trues_cls_valid, "label_reg": trues_reg_valid})
    pred_valid_df.to_parquet(out / f"pred_valid_N{args.n}_X{args.x}.parquet", index=False)
    if len(X_oos2)>0:
        pred_oos_df = pd.DataFrame({"trade_date": oos2_days, "ts_code": oos2_tss, "pred_prob": preds_prob_oos, "pred_reg": preds_reg_oos, "label_cls": y_cls_oos2, "label_reg": y_reg_oos2})
        pred_oos_df.to_parquet(out / f"pred_oos2_N{args.n}_X{args.x}.parquet", index=False)
    print("done")

if __name__=="__main__":
    main()
