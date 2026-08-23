"""TCN + LSTM baselines."""

from __future__ import annotations

import torch
import torch.nn as nn


class Chomp1d(nn.Module):
    def __init__(self, chomp_size: int):
        super().__init__()
        self.chomp_size = chomp_size
    def forward(self, x):
        return x[:, :, :-self.chomp_size].contiguous() if self.chomp_size>0 else x

class TCNBlock(nn.Module):
    def __init__(self, in_ch, out_ch, kernel=3, dilation=1, dropout=0.2):
        super().__init__()
        pad = (kernel-1)*dilation
        self.conv1 = nn.Conv1d(in_ch, out_ch, kernel, padding=pad, dilation=dilation)
        self.chomp1 = Chomp1d(pad)
        self.relu1 = nn.ReLU()
        self.drop1 = nn.Dropout(dropout)
        self.conv2 = nn.Conv1d(out_ch, out_ch, kernel, padding=pad, dilation=dilation)
        self.chomp2 = Chomp1d(pad)
        self.relu2 = nn.ReLU()
        self.drop2 = nn.Dropout(dropout)
        self.down = nn.Conv1d(in_ch, out_ch, 1) if in_ch != out_ch else None
        self.relu = nn.ReLU()
    def forward(self, x):
        y = self.conv1(x)
        y = self.chomp1(y)
        y = self.relu1(y)
        y = self.drop1(y)
        y = self.conv2(y)
        y = self.chomp2(y)
        y = self.relu2(y)
        y = self.drop2(y)
        res = x if self.down is None else self.down(x)
        return self.relu(y + res)

class TCNForecast(nn.Module):
    def __init__(self, in_dim=14, hidden=64, levels=4, kernel=3, dropout=0.2):
        super().__init__()
        layers=[]
        for i in range(levels):
            dil = 2**i
            layers.append(TCNBlock(in_dim if i==0 else hidden, hidden, kernel, dil, dropout))
        self.tcn = nn.Sequential(*layers)
        self.head_reg = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
        self.head_cls = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
    def forward(self, x):
        # x [B, L, F] -> [B, F, L]
        y = x.transpose(1,2)
        y = self.tcn(y)  # [B, hidden, L]
        # take last time step
        last = y[:, :, -1]  # [B, hidden]
        reg = self.head_reg(last).squeeze(-1)  # [B]
        logit = self.head_cls(last).squeeze(-1)
        return reg, logit

class LSTMForecast(nn.Module):
    def __init__(self, in_dim=14, hidden=64, layers=1, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(in_dim, hidden, num_layers=layers, batch_first=True, dropout=dropout if layers>1 else 0)
        self.head_reg = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
        self.head_cls = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
    def forward(self, x):
        out,_ = self.lstm(x)  # [B, L, hidden]
        last = out[:, -1, :]
        reg = self.head_reg(last).squeeze(-1)
        logit = self.head_cls(last).squeeze(-1)
        return reg, logit

class TransformerForecast(nn.Module):
    def __init__(self, in_dim=14, hidden=64, nhead=4, layers=2, dropout=0.2):
        super().__init__()
        self.in_proj = nn.Linear(in_dim, hidden)
        enc_layer = nn.TransformerEncoderLayer(d_model=hidden, nhead=nhead, dim_feedforward=hidden*2, dropout=dropout, batch_first=True)
        self.enc = nn.TransformerEncoder(enc_layer, num_layers=layers)
        self.head_reg = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
        self.head_cls = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Dropout(dropout), nn.Linear(32,1))
    def forward(self, x):
        y = self.in_proj(x)  # [B,L,H]
        y = self.enc(y)  # [B,L,H]
        last = y[:, -1, :]
        reg = self.head_reg(last).squeeze(-1)
        logit = self.head_cls(last).squeeze(-1)
        return reg, logit
