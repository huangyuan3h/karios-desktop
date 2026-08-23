"""Training loop with early stop on valid AUC."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import roc_auc_score, average_precision_score
from torch.utils.data import Dataset, DataLoader

from ml_forecast.model import LSTMForecast, TCNForecast, TransformerForecast

class SeqDataset(Dataset):
    def __init__(self, X, y_reg, y_cls):
        self.X = torch.from_numpy(X)
        self.y_reg = torch.from_numpy(y_reg)
        self.y_cls = torch.from_numpy(y_cls).float()
    def __len__(self): return len(self.X)
    def __getitem__(self, idx):
        return self.X[idx], self.y_reg[idx], self.y_cls[idx]

def spearman_ic(pred, label):
    # rank correlation approx via scipy could be heavy; use numpy rank
    if len(pred)<10: return 0.0
    r1 = np.argsort(np.argsort(pred))
    r2 = np.argsort(np.argsort(label))
    return np.corrcoef(r1, r2)[0,1] if np.std(r1)>0 and np.std(r2)>0 else 0.0

def train_one_model(X_train, y_reg_train, y_cls_train, X_valid, y_reg_valid, y_cls_valid, model_type="tcn", epochs=80, batch=1024, lr=3e-4, device=None, pos_weight=None, hidden=64, levels=4, dropout=0.2):
    if device is None:
        # force CPU for stability (MPS caused NaN in first run)
        device=torch.device("cpu")
    print(f"device {device} train {len(X_train)} valid {len(X_valid)} pos_rate {y_cls_train.mean():.3f}")
    in_dim = X_train.shape[-1]
    if model_type=="tcn":
        model = TCNForecast(in_dim=in_dim, hidden=hidden, levels=levels, dropout=dropout)
    elif model_type=="transformer":
        model = TransformerForecast(in_dim=in_dim, hidden=hidden, dropout=dropout)
    else:
        model = LSTMForecast(in_dim=in_dim, hidden=hidden, layers=1, dropout=dropout)
    model.to(device)
    train_ds = SeqDataset(X_train, y_reg_train, y_cls_train)
    valid_ds = SeqDataset(X_valid, y_reg_valid, y_cls_valid)
    train_loader = DataLoader(train_ds, batch_size=batch, shuffle=True, drop_last=False, num_workers=0)
    valid_loader = DataLoader(valid_ds, batch_size=batch*2, shuffle=False, num_workers=0)
    opt = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=epochs)
    bce = nn.BCEWithLogitsLoss(pos_weight=torch.tensor(float(pos_weight), dtype=torch.float32, device=device) if pos_weight else None)
    mse = nn.MSELoss()
    best_auc = -1
    best_state = None
    patience=12
    bad=0
    for epoch in range(1, epochs+1):
        model.train()
        total_loss=0
        for X,y_reg,y_cls in train_loader:
            X=X.to(device); y_reg=y_reg.to(device); y_cls=y_cls.to(device)
            pred_reg, logit = model(X)
            loss_reg = mse(pred_reg, y_reg.clamp(-30,100)/20)  # scale
            loss_cls = bce(logit, y_cls)
            loss = loss_reg + 0.5*loss_cls
            opt.zero_grad(); loss.backward(); torch.nn.utils.clip_grad_norm_(model.parameters(),1.0); opt.step()
            total_loss+=float(loss.item())*len(X)
        sched.step()
        # valid metrics
        model.eval()
        preds_reg=[]; preds_logit=[]; trues_reg=[]; trues_cls=[]
        with torch.no_grad():
            for X,y_reg,y_cls in valid_loader:
                X=X.to(device)
                pr, lg = model(X)
                preds_reg.append(pr.cpu().numpy()*20)
                preds_logit.append(torch.sigmoid(lg).cpu().numpy())
                trues_reg.append(y_reg.numpy())
                trues_cls.append(y_cls.numpy())
        preds_reg=np.concatenate(preds_reg) if preds_reg else np.array([])
        preds_prob=np.concatenate(preds_logit) if preds_logit else np.array([])
        trues_reg=np.concatenate(trues_reg) if trues_reg else np.array([])
        trues_cls=np.concatenate(trues_cls) if trues_cls else np.array([])
        try: auc = roc_auc_score(trues_cls, preds_prob) if len(np.unique(trues_cls))>1 else 0.5
        except: auc=0.5
        try: ap = average_precision_score(trues_cls, preds_prob) if len(np.unique(trues_cls))>1 else 0
        except: ap=0
        ic = spearman_ic(preds_reg, trues_reg) if len(preds_reg)>0 else 0
        avg_loss = total_loss/len(train_ds) if len(train_ds)>0 else 0
        print(f"epoch {epoch:02d} loss {avg_loss:.4f} valid AUC {auc:.4f} AP {ap:.4f} IC {ic:.3f} lr {opt.param_groups[0]['lr']:.2e}")
        if auc > best_auc + 0.002:
            best_auc = auc
            best_state = {k:v.cpu() for k,v in model.state_dict().items()}
            bad=0
        else:
            bad+=1
        if bad>=patience:
            print(f"early stop at epoch {epoch} best AUC {best_auc:.4f}")
            break
    if best_state is not None:
        model.load_state_dict({k:v.to(device) for k,v in best_state.items()})
    # final valid
    model.eval()
    with torch.no_grad():
        preds_reg=[]; preds_prob=[]
        for X,y_reg,y_cls in valid_loader:
            X=X.to(device)
            pr, lg = model(X)
            preds_reg.append((pr.cpu().numpy()*20))
            preds_prob.append(torch.sigmoid(lg).cpu().numpy())
        preds_reg=np.concatenate(preds_reg) if preds_reg else np.array([])
        preds_prob=np.concatenate(preds_prob) if preds_prob else np.array([])
    return model, best_auc, preds_reg, preds_prob, trues_reg, trues_cls
