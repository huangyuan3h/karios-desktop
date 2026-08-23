"""Morphology factor detectors (independent of S-3).

Validated signal per docs/designs/pattern-factor-validation.md §2.4:
  strong_scoop_exhaustion — a rounded pullback ("scoop") in a STRONG stock that
  breaks down on distribution volume is an exhaustion-top / bearish signal
  (out-of-sample 82-92% hit, +10-15% R on the short side, 2021-2026 chunks).
"""
from __future__ import annotations
import numpy as np


def rollmean(a: np.ndarray, w: int) -> np.ndarray:
    """Trailing mean ending at index i (no future leak). NaN before w-1."""
    a = np.asarray(a, float)
    cs = np.cumsum(np.concatenate([[0.0], a]))
    out = np.full(len(a), np.nan)
    idx = np.arange(w - 1, len(a))
    out[idx] = (cs[idx + 1] - cs[idx - w + 1]) / w
    return out


def detect_scoop(close, high, low, vol, scoop_win: int = 20, pre_win: int = 20,
                 depth_min: float = 0.05, depth_max: float = 0.18) -> dict:
    """Per-day scoop detection in an uptrend."""
    c = np.asarray(close, float); h = np.asarray(high, float)
    l = np.asarray(low, float); v = np.asarray(vol, float)
    m = len(c)
    ma20 = rollmean(c, 20); ma60 = rollmean(c, 60)
    scoop = np.zeros(m, bool)
    bottom = np.full(m, np.nan); pre_high = np.full(m, np.nan)
    depth = np.full(m, np.nan); ret60 = np.full(m, np.nan); vol_ratio = np.full(m, np.nan)
    for t in range(90, m - 20):
        if not (ma20[t] > ma60[t] and c[t - 30] > ma60[t - 30]):
            continue
        ph = h[t - 40:t - 20].max()
        bw = l[t - 20:t + 1]; bot = bw.min(); bi = t - 20 + int(np.argmin(bw))
        dep = (ph - bot) / ph if ph > 0 else 0
        if not (depth_min <= dep <= depth_max):
            continue
        if not (c[t] >= bot * 1.03 and c[t] >= ma20[t] * 0.99):
            continue
        if bi < t - 15:
            continue
        scoop[t] = True
        bottom[t] = bot; pre_high[t] = ph; depth[t] = dep
        ret60[t] = c[t] / c[t - 60] - 1 if t >= 60 else np.nan
        sv = v[t - 20:t + 1].mean()
        vol_ratio[t] = v[t] / sv if sv > 0 else 1.0
    return dict(scoop=scoop, bottom=bottom, pre_high=pre_high, depth=depth,
                ret60=ret60, vol_ratio=vol_ratio)


def strong_scoop_exhaustion(close, high, low, vol, ret60_thresh: float = 0.40,
                            vol_confirm: bool = True) -> np.ndarray:
    """Boolean signal: exhaustion-top scoop in a strong stock."""
    d = detect_scoop(close, high, low, vol)
    sig = d["scoop"] & (d["ret60"] > ret60_thresh)
    if vol_confirm:
        sig = sig & (d["vol_ratio"] > 1.2)
    return sig
