"""Feature engineering — 60-day window, ~18 dims, z-score by train stats."""

from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_COLS = [
    "ret_1",
    "ret_5",
    "ret_20",
    "log_vol",
    "vol_ratio_20",
    "amount",
    "high_20_ratio",
    "low_20_ratio",
    "atr_20",
    "rsi_14",
    "ma20_bias",
    "ma20_slope",
    "close_to_ma60",
    "volatility_20",
    # feat+ 4
    "rs_rank",
    "score_norm",
    "log_mv",
    "turnover",
]

def rsi(series: pd.Series, n: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(n).mean()
    loss = -delta.clip(upper=0).rolling(n).mean()
    rs = gain / (loss + 1e-9)
    return float(100 - 100 / (1 + rs.iloc[-1])) if pd.notna(rs.iloc[-1]) else 50.0


def build_feature_tensor(
    bars: pd.DataFrame,
    calendar: list[str],
    samples: pd.DataFrame,
    L: int = 60,
    rs_map: dict | None = None,
    score_map: dict | None = None,
    mv_map: dict | None = None,
    turnover_map: dict | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, list[str], list[str]]:
    """Return X [N, L, F], y_reg [N], y_cls [N], plus index lists for backtest join.

    bars: daily bars DataFrame with ts_code, trade_date, open, high, low, close, vol, amount
    samples: from data.build_samples (trade_date, ts_code, label_reg, label_cls)
    """
    # index bars by ts
    bars_by_ts: dict[str, pd.DataFrame] = {}
    for ts, g in bars.groupby("ts_code"):
        g = g.sort_values("trade_date")
        g = g.set_index("trade_date")
        bars_by_ts[ts] = g

    # map sample -> window
    X_list = []
    y_reg_list = []
    y_cls_list = []
    days = []
    tss = []

    cal_set = set(calendar)
    # pre-sort samples
    for _, row in samples.iterrows():
        day = row["trade_date"]
        ts = row["ts_code"]
        g = bars_by_ts.get(ts)
        if g is None:
            continue
        # window days inclusive day, L days
        # find position of day in calendar
        # use bars index to get window: need L bars ending at day
        try:
            loc = g.index.get_loc(day)
        except KeyError:
            continue
        if loc < L - 1:
            continue
        window = g.iloc[loc - L + 1 : loc + 1]
        if len(window) < L:
            continue
        # feature per step: compute rolling stats within window using only window data (no leakage beyond window)
        closes = window["close"].astype(float).values
        vols = window["vol"].astype(float).values
        highs = window["high"].astype(float).values
        lows = window["low"].astype(float).values
        amounts = window["amount"].astype(float).values

        seq = []
        for i in range(L):
            c = closes[i]
            # ret features need history inside window
            ret_1 = (closes[i] / closes[i-1] - 1) if i >= 1 else 0.0
            ret_5 = (closes[i] / closes[i-5] - 1) if i >= 5 else 0.0
            ret_20 = (closes[i] / closes[i-20] - 1) if i >= 20 else 0.0
            log_vol = float(np.log1p(vols[i]))
            vol20 = float(np.mean(vols[max(0, i-19):i+1]))
            vol_ratio_20 = float(vols[i] / (vol20 + 1e-9))
            high20 = float(np.max(highs[max(0, i-19):i+1]))
            low20 = float(np.min(lows[max(0, i-19):i+1]))
            high_20_ratio = float(c / (high20 + 1e-9))
            low_20_ratio = float(c / (low20 + 1e-9))
            # ATR20 proxy
            tr = highs[max(1,i):i+1] - lows[max(1,i):i+1]  # simplified
            atr_20 = float(np.mean(np.abs(highs[max(1,i-19):i+1] - lows[max(1,i-19):i+1]))) / (c+1e-9) if i>=1 else 0.0
            # RSI14 on closes up to i
            if i >= 14:
                s = pd.Series(closes[max(0,i-14):i+1])
                rsi_14 = rsi(s, 14)
            else:
                rsi_14 = 50.0
            ma20 = float(np.mean(closes[max(0,i-19):i+1]))
            ma20_bias = float(c / (ma20+1e-9) - 1)
            ma20_prev = float(np.mean(closes[max(0,i-20):i])) if i>=1 else ma20
            ma20_slope = float(ma20 / (ma20_prev+1e-9) - 1)
            ma60 = float(np.mean(closes))
            close_to_ma60 = float(c / (ma60+1e-9) - 1)
            volatility_20 = float(np.std(closes[max(0,i-19):i+1]) / (c+1e-9)) if i>=19 else 0.0
            # feat+ extra: look up by bar's trade_date
            cur_day = str(window.index[i]) if hasattr(window.index[i], "strftime") else str(window.index[i])
            # window.index is trade_date string, use it
            try:
                cur_day_str = cur_day[:10]
            except:
                cur_day_str = str(cur_day)
            rs_rank = 0.5
            if rs_map is not None:
                try: rs_rank = float(rs_map.get(cur_day_str, {}).get(ts, 0.5))
                except: rs_rank = 0.5
                if not (0 <= rs_rank <= 1): rs_rank = 0.5
            score_norm = 0.5
            if score_map is not None:
                try:
                    sc = score_map.get(cur_day_str, {}).get(ts)
                    if sc is not None: score_norm = float(sc)/100.0
                except: pass
                score_norm = float(np.clip(score_norm, 0, 1))
            log_mv = 0.0
            if mv_map is not None:
                try:
                    mv = mv_map.get(cur_day_str, {}).get(ts)
                    if mv is not None and mv>0: log_mv = float(np.log1p(mv/1e5)) # total_mv is 万元, /1e5 ≈ 亿元
                except: log_mv = 0.0
            turnover = 0.0
            if turnover_map is not None:
                try:
                    tr = turnover_map.get(cur_day_str, {}).get(ts)
                    if tr is not None: turnover = float(tr)/10.0 # turnover_rate ~ 0-20 -> 0-2
                except: turnover = 0.0

            seq.append([ret_1, ret_5, ret_20, log_vol, vol_ratio_20, float(amounts[i]/1e7), high_20_ratio, low_20_ratio, atr_20, rsi_14/100, ma20_bias, ma20_slope, close_to_ma60, volatility_20, rs_rank, score_norm, log_mv, turnover])
        X_list.append(np.array(seq, dtype=np.float32))
        y_reg_list.append(float(row["label_reg"]))
        y_cls_list.append(int(row["label_cls"]))
        days.append(day)
        tss.append(ts)

    if not X_list:
        return np.empty((0, L, len(FEATURE_COLS)), dtype=np.float32), np.empty((0,), dtype=np.float32), np.empty((0,), dtype=np.int64), [], []
    X = np.stack(X_list)
    # sanitize: replace inf/nan, clip extreme returns
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    # clip returns to [-0.3,0.3] (first 3 cols) to avoid split artifacts
    X[:, :, 0] = np.clip(X[:, :, 0], -0.22, 0.22)
    X[:, :, 1] = np.clip(X[:, :, 1], -0.5, 0.5)
    X[:, :, 2] = np.clip(X[:, :, 2], -0.8, 0.8)
    X[:, :, 4] = np.clip(X[:, :, 4], 0, 5)
    X[:, :, 6] = np.clip(X[:, :, 6], 0.5, 1.5)
    X[:, :, 7] = np.clip(X[:, :, 7], 0.5, 1.5)
    y_reg = np.array(y_reg_list, dtype=np.float32)
    # clip label to avoid 500% outliers pulling MSE
    y_reg = np.clip(y_reg, -30, 100).astype(np.float32)
    y_cls = np.array(y_cls_list, dtype=np.int64)
    return X, y_reg, y_cls, days, tss


def compute_norm_stats(X_train: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    # X_train [N, L, F]
    mean = X_train.mean(axis=(0, 1))
    std = X_train.std(axis=(0, 1)) + 1e-6
    # rsi already 0-1, keep
    return mean, std


def apply_norm(X: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    return (X - mean) / std
