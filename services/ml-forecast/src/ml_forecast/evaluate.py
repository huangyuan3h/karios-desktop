"""Offline metrics + backtest hook."""

from __future__ import annotations

import numpy as np
from sklearn.metrics import roc_auc_score, average_precision_score

def evaluate_offline(pred_prob, pred_reg, true_cls, true_reg):
    out={}
    if len(np.unique(true_cls))>1:
        out["AUC"] = float(roc_auc_score(true_cls, pred_prob))
        out["AP"] = float(average_precision_score(true_cls, pred_prob))
    else:
        out["AUC"]=0.5; out["AP"]=0.0
    # precision@K
    for k in [0.05, 0.10, 0.20]:
        thr = np.quantile(pred_prob, 1-k) if len(pred_prob)>0 else 0
        mask = pred_prob >= thr
        if mask.sum()>0:
            out[f"prec@{int(k*100)}%"] = float(true_cls[mask].mean())
        else:
            out[f"prec@{int(k*100)}%"]=0.0
    # threshold X precision
    for thr in [0.5, 0.6, 0.7]:
        mask = pred_prob >= thr
        out[f"prec_thr{thr}"] = float(true_cls[mask].mean()) if mask.sum()>0 else 0.0
        out[f"n_thr{thr}"] = int(mask.sum())
    # IC
    if len(pred_reg)>10:
        r1 = np.argsort(np.argsort(pred_reg))
        r2 = np.argsort(np.argsort(true_reg))
        out["IC"] = float(np.corrcoef(r1, r2)[0,1]) if np.std(r1)>0 else 0.0
    else:
        out["IC"]=0.0
    out["pos_rate"] = float(true_cls.mean()) if len(true_cls)>0 else 0
    return out
