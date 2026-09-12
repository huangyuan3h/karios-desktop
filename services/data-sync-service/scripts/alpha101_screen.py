#!/usr/bin/env python3
"""Alpha101 (WorldQuant) L0 cross-sectional RankIC screen — CN A-shares.

Read-only diagnostic (no DB writes, no Live changes).
Pre-registration: docs/designs/alpha101-screen-prereg-2026-09-12.md

Design:
  * Panel = Date x asset wide DataFrames (pandas), one per field.
  * Cross-sectional rank = df.rank(axis=1); time-series ops = df.rolling().
  * Alphas are evaluated one at a time and discarded (memory bound).
  * vwap is put on the same qfq basis as close:
        vwap_qfq(t) = (amount*10/vol) * adj_factor(t) / adj_factor(latest)
    because `daily` OHLC is qfq while amount/vol are raw (data-consistency-2026-09-11).
  * volume/adv use turnover (amount) so alpha7's `adv20 < volume` is unit-consistent.

Usage:
  PYTHONPATH=src python3 scripts/alpha101_screen.py --windows OOS2,train,valid
  PYTHONPATH=src python3 scripts/alpha101_screen.py --windows valid --alphas A1,A5,A101
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

np.seterr(invalid="ignore", divide="ignore")

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "holdout": ("2026-08-08", "2027-02-08"),
}
HORIZONS = [1, 5, 10]
PRIMARY_H = 5
COST_ROUNDTRIP = 0.003  # 30bp (see prereg §4)
LIQ_FLOOR_QIAN = 70000.0  # 0.7亿元 in 千元
MIN_STOCKS_PER_DAY = 30
LOOKBACK_DAYS = 420
FWD_BUFFER_DAYS = 40
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

# --------------------------------------------------------------------------
# Operators (all operate on Date x asset DataFrames)
# --------------------------------------------------------------------------


def rank(df: pd.DataFrame) -> pd.DataFrame:
    return df.rank(axis=1, pct=True)


def scale(df: pd.DataFrame, a: float = 1.0) -> pd.DataFrame:
    s = df.abs().sum(axis=1)
    return df.div(s.replace(0, np.nan), axis=0) * a


def delay(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.shift(d)


def delta(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df - df.shift(d)


def correlation(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
    return x.rolling(d).corr(y)


def covariance(x: pd.DataFrame, y: pd.DataFrame, d: int) -> pd.DataFrame:
    return x.rolling(d).cov(y)


def ts_min(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.rolling(d).min()


def ts_max(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.rolling(d).max()


def ts_sum(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.rolling(d).sum()


def stddev(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.rolling(d).std()


def ts_rank(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return df.rolling(d).rank(pct=True)


def product(df: pd.DataFrame, d: int) -> pd.DataFrame:
    if d <= 1:
        return df.copy()
    return df.rolling(d).apply(np.prod, raw=True)


def signedpower(df: pd.DataFrame, a: float) -> pd.DataFrame:
    return np.sign(df) * (df.abs() ** a)


def _sliding_extreme(df: pd.DataFrame, d: int, mode: str) -> pd.DataFrame:
    from numpy.lib.stride_tricks import sliding_window_view

    arr = df.to_numpy(dtype=float)
    T, N = arr.shape
    out = np.full((T, N), np.nan)
    if T < d:
        return pd.DataFrame(out, index=df.index, columns=df.columns)
    fill = -np.inf if mode == "max" else np.inf
    for j in range(0, N, 512):
        sub = arr[:, j : j + 512]
        sw = sliding_window_view(sub, d, axis=0)  # (T-d+1, nb, d)
        filled = np.where(np.isnan(sw), fill, sw)
        idx = np.argmax(filled, axis=2) if mode == "max" else np.argmin(filled, axis=2)
        block = idx.astype(float) + 1.0
        all_nan = np.isnan(sw).all(axis=2)
        block[all_nan] = np.nan
        out[d - 1 :, j : j + 512] = block
    return pd.DataFrame(out, index=df.index, columns=df.columns)


def ts_argmax(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return _sliding_extreme(df, d, "max")


def ts_argmin(df: pd.DataFrame, d: int) -> pd.DataFrame:
    return _sliding_extreme(df, d, "min")


def decay_linear(df: pd.DataFrame, d: int) -> pd.DataFrame:
    from numpy.lib.stride_tricks import sliding_window_view

    arr = df.to_numpy(dtype=float)
    T, N = arr.shape
    out = np.full((T, N), np.nan)
    if T < d:
        return pd.DataFrame(out, index=df.index, columns=df.columns)
    w = np.arange(1, d + 1, dtype=float)
    wsum = w.sum()
    for j in range(0, N, 512):
        sub = arr[:, j : j + 512]
        sw = sliding_window_view(sub, d, axis=0)
        valid = ~np.isnan(sw).any(axis=2)
        block = np.nansum(sw * w, axis=2) / wsum
        block[~valid] = np.nan
        out[d - 1 :, j : j + 512] = block
    return pd.DataFrame(out, index=df.index, columns=df.columns)


# --------------------------------------------------------------------------
# Alpha definitions (canonical WorldQuant formulas)
# --------------------------------------------------------------------------


def alpha101_lib(d: dict[str, pd.DataFrame], groups: pd.Series) -> dict[str, pd.DataFrame]:
    o, h, l, c = d["open"], d["high"], d["low"], d["close"]  # noqa: E741
    v = d["volume"]
    r = d["returns"]
    vwap = d["vwap"]
    cap = d["cap"]

    def adv(n: int) -> pd.DataFrame:
        return d["amount"].rolling(n).mean()

    def neut(df: pd.DataFrame) -> pd.DataFrame:
        g = groups.reindex(df.columns)
        g = g.fillna("UNKNOWN")
        return df.sub(df.T.groupby(g).transform("mean").T)

    def W(cond, a, b):  # cond ? a : b
        if not isinstance(a, pd.DataFrame):
            a = pd.DataFrame(a, index=cond.index, columns=cond.columns)
        return a.where(cond, b)

    def cmp_lt(a, b):  # boolean -> float 0/1
        return (a < b).astype(float)

    A: dict[str, pd.DataFrame] = {}

    A["A1"] = rank(ts_argmax(signedpower(stddev(r, 20).where(r < 0, c), 2), 5)) - 0.5
    A["A2"] = -1 * correlation(rank(delta(np.log(v), 2)), rank((c - o) / o), 6)
    A["A3"] = -1 * correlation(rank(o), rank(v), 10)
    A["A4"] = -1 * ts_rank(rank(l), 9)
    A["A5"] = rank(o - ts_sum(vwap, 10) / 10) * (-1 * rank(c - vwap).abs())
    A["A6"] = -1 * correlation(o, v, 10)
    A["A7"] = W(adv(20) < v, -1 * ts_rank(delta(c, 7).abs(), 60) * np.sign(delta(c, 7)), -1.0)
    A["A8"] = -1 * rank(
        (ts_sum(o, 5) * ts_sum(r, 5)) - delay(ts_sum(o, 5) * ts_sum(r, 5), 10)
    )
    d1 = delta(c, 1)
    A["A9"] = W(
        ts_min(d1, 5) > 0, d1, W(ts_max(d1, 5) < 0, d1, -1 * d1)
    )
    A["A10"] = rank(
        W(ts_min(d1, 4) > 0, d1, W(ts_max(d1, 4) < 0, d1, -1 * d1))
    )
    A["A11"] = (rank(ts_max(vwap - c, 3)) + rank(ts_min(vwap - c, 3))) * rank(delta(v, 3))
    A["A12"] = np.sign(delta(v, 1)) * (-1 * delta(c, 1))
    A["A13"] = -1 * rank(covariance(rank(c), rank(v), 5))
    A["A14"] = (-1 * rank(delta(r, 3))) * correlation(o, v, 10)
    A["A15"] = -1 * ts_sum(rank(correlation(rank(h), rank(v), 3)), 3)
    A["A16"] = -1 * rank(covariance(rank(h), rank(v), 5))
    A["A17"] = (
        (-1 * rank(ts_rank(c, 10)))
        * rank(delta(delta(c, 1), 1))
        * rank(ts_rank(v / adv(20), 5))
    )
    A["A18"] = -1 * rank(stddev((c - o).abs(), 5) + (c - o) + correlation(c, o, 10))
    A["A19"] = (
        (-1 * np.sign((c - delay(c, 7)) + delta(c, 7)))
        * (1 + rank(1 + ts_sum(r, 250)))
    )
    A["A20"] = (
        (-1 * rank(o - delay(h, 1)))
        * rank(o - delay(c, 1))
        * rank(o - delay(l, 1))
    )
    A["A21"] = W(
        (ts_sum(c, 8) / 8 + stddev(c, 8)) < ts_sum(c, 2) / 2,
        -1.0,
        W(
            ts_sum(c, 2) / 2 < (ts_sum(c, 8) / 8 - stddev(c, 8)),
            1.0,
            W((1 < v / adv(20)) | (v / adv(20) == 1), 1.0, -1.0),
        ),
    )
    A["A22"] = -1 * (delta(correlation(h, v, 5), 5) * rank(stddev(c, 20)))
    A["A23"] = W(ts_sum(h, 20) / 20 < h, -1 * delta(h, 2), 0.0)
    A["A24"] = W(
        delta(ts_sum(c, 100) / 100, 100) / delay(c, 100) <= 0.05,
        -1 * (c - ts_min(c, 100)),
        -1 * delta(c, 3),
    )
    A["A25"] = rank(((-1 * r) * adv(20) * vwap) * (h - c))
    A["A26"] = -1 * ts_max(correlation(ts_rank(v, 5), ts_rank(h, 5), 5), 3)
    A["A27"] = W(rank(ts_sum(correlation(rank(v), rank(vwap), 6), 2) / 2.0) > 0.5, -1.0, 1.0)
    A["A28"] = scale(correlation(adv(20), l, 5) + (h + l) / 2 - c)
    t29 = rank(rank(-1 * rank(delta(c - 1, 5))))
    t29 = scale(np.log(ts_min(t29, 2)))
    A["A29"] = ts_min(product(rank(rank(t29)), 1), 5) + ts_rank(delay(-1 * r, 6), 5)
    A["A30"] = (
        1.0
        - rank(
            np.sign(c - delay(c, 1))
            + np.sign(delay(c, 1) - delay(c, 2))
            + np.sign(delay(c, 2) - delay(c, 3))
        )
    ) * (ts_sum(v, 5) / ts_sum(v, 20))
    A["A31"] = (
        rank(rank(rank(decay_linear(-1 * rank(rank(delta(c, 10))), 10))))
        + rank(-1 * delta(c, 3))
        + np.sign(scale(correlation(adv(20), l, 12)))
    )
    A["A32"] = scale(ts_sum(c, 7) / 7 - c) + 20 * scale(
        correlation(vwap, delay(c, 5), 230)
    )
    A["A33"] = rank(-1 * (1 - o / c))
    A["A34"] = rank(
        (1 - rank(stddev(r, 2) / stddev(r, 5))) + (1 - rank(delta(c, 1)))
    )
    A["A35"] = (
        ts_rank(v, 32) * (1 - ts_rank(c + h - l, 16)) * (1 - ts_rank(r, 32))
    )
    A["A36"] = (
        2.21 * rank(correlation(c - o, delay(v, 1), 15))
        + 0.7 * rank(o - c)
        + 0.73 * rank(ts_rank(delay(-1 * r, 6), 5))
        + rank(correlation(vwap, adv(20), 6).abs())
        + 0.6 * rank((ts_sum(c, 200) / 200 - o) * (c - o))
    )
    A["A37"] = rank(correlation(delay(o - c, 1), c, 200)) + rank(o - c)
    A["A38"] = (-1 * rank(ts_rank(c, 10))) * rank(c / o)
    A["A39"] = (
        (-1 * rank(delta(c, 7) * (1 - rank(decay_linear(v / adv(20), 9)))))
        * (1 + rank(ts_sum(r, 250)))
    )
    A["A40"] = (-1 * rank(stddev(h, 10))) * correlation(h, v, 10)
    A["A41"] = signedpower(h * l, 0.5) - vwap
    A["A42"] = rank(vwap - c) / rank(vwap + c)
    A["A43"] = ts_rank(v / adv(20), 20) * ts_rank(-1 * delta(c, 7), 8)
    A["A44"] = -1 * correlation(h, rank(v), 5)
    A["A45"] = -1 * (
        rank(ts_sum(delay(c, 5), 20) / 20)
        * correlation(c, v, 2)
        * rank(correlation(ts_sum(c, 5), ts_sum(c, 20), 2))
    )
    A["A46"] = W(
        0.25 < ((delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10),
        -1.0,
        W(
            ((delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10) < 0,
            1.0,
            -1 * (c - delay(c, 1)),
        ),
    )
    A["A47"] = (
        (rank(1 / c) * v / adv(20))
        * (h * rank(h - c) / (ts_sum(h, 5) / 5))
        - rank(vwap - delay(vwap, 5))
    )
    A["A48"] = neut(
        correlation(delta(c, 1), delta(delay(c, 1), 1), 250) * delta(c, 1) / c
    ) / ts_sum(signedpower(delta(c, 1) / delay(c, 1), 2), 250)
    A["A49"] = W(
        ((delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10) < -0.1,
        1.0,
        -1 * (c - delay(c, 1)),
    )
    A["A50"] = -1 * ts_max(rank(correlation(rank(v), rank(vwap), 5)), 5)
    A["A51"] = W(
        ((delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10) < -0.05,
        1.0,
        -1 * (c - delay(c, 1)),
    )
    A["A52"] = (
        (-1 * ts_min(l, 5) + delay(ts_min(l, 5), 5))
        * rank((ts_sum(r, 240) - ts_sum(r, 20)) / 220)
    ) * ts_rank(v, 5)
    A["A53"] = -1 * delta(((c - l) - (h - c)) / (c - l), 9)
    A["A54"] = (-1 * ((l - c) * signedpower(o, 5))) / ((l - h) * signedpower(c, 5))
    A["A55"] = -1 * correlation(
        rank((c - ts_min(l, 12)) / (ts_max(h, 12) - ts_min(l, 12))), rank(v), 6
    )
    A["A56"] = -1 * (
        rank(ts_sum(r, 10) / ts_sum(ts_sum(r, 2), 3)) * rank(r * cap)
    )
    A["A57"] = -1 * ((c - vwap) / decay_linear(rank(ts_argmax(c, 30)), 2))
    A["A58"] = -1 * ts_rank(
        decay_linear(correlation(neut(vwap), v, 4), 8), 6
    )
    A["A59"] = -1 * ts_rank(
        decay_linear(correlation(neut(vwap), v, 4), 16), 8
    )
    A["A60"] = -1 * (
        2 * scale(rank(((c - l) - (h - c)) / (h - l) * v))
        - scale(rank(ts_argmax(c, 10)))
    )
    A["A61"] = cmp_lt(rank(vwap - ts_min(vwap, 16)), rank(correlation(vwap, adv(180), 18)))
    A["A62"] = -1 * cmp_lt(
        rank(correlation(vwap, ts_sum(adv(20), 22), 10)),
        rank((rank(o) + rank(o)) < (rank((h + l) / 2) + rank(h))),
    )
    A["A63"] = -1 * (
        rank(decay_linear(delta(neut(c), 2), 8))
        - rank(
            decay_linear(
                correlation(vwap * 0.318108 + o * (1 - 0.318108), ts_sum(adv(180), 37), 13),
                12,
            )
        )
    )
    A["A64"] = -1 * cmp_lt(
        rank(
            correlation(
                ts_sum(o * 0.178404 + l * (1 - 0.178404), 13), ts_sum(adv(120), 13), 17
            )
        ),
        rank(delta((h + l) / 2 * 0.178404 + vwap * (1 - 0.178404), 4)),
    )
    A["A65"] = -1 * cmp_lt(
        rank(
            correlation(
                o * 0.00817205 + vwap * (1 - 0.00817205), ts_sum(adv(60), 9), 6
            )
        ),
        rank(o - ts_min(o, 14)),
    )
    A["A66"] = -1 * (
        rank(decay_linear(delta(vwap, 4), 7))
        + ts_rank(decay_linear((l - vwap) / (o - (h + l) / 2), 11), 7)
    )
    A["A67"] = -1 * (
        rank(h - ts_min(h, 2)) ** rank(correlation(neut(vwap), neut(adv(20)), 6))
    )
    A["A68"] = -1 * cmp_lt(
        ts_rank(correlation(rank(h), rank(adv(15)), 9), 14),
        rank(delta(c * 0.518371 + l * (1 - 0.518371), 1)),
    )
    A["A69"] = -1 * (
        rank(ts_max(delta(neut(vwap), 3), 5))
        ** ts_rank(correlation(c * 0.490655 + vwap * (1 - 0.490655), adv(20), 5), 9)
    )
    A["A70"] = -1 * (
        rank(delta(vwap, 1)) ** ts_rank(correlation(neut(c), adv(50), 18), 18)
    )
    A["A71"] = np.maximum(
        ts_rank(decay_linear(correlation(ts_rank(c, 3), ts_rank(adv(180), 12), 18), 4), 15),
        ts_rank(decay_linear(rank((l + o) - (vwap + vwap)) ** 2, 16), 4),
    )
    A["A72"] = rank(decay_linear(correlation((h + l) / 2, adv(40), 9), 10)) / rank(
        decay_linear(correlation(ts_rank(vwap, 4), ts_rank(v, 19), 7), 3)
    )
    A["A73"] = -1 * np.maximum(
        rank(decay_linear(delta(vwap, 5), 3)),
        ts_rank(
            decay_linear(
                -1
                * (
                    delta(o * 0.147155 + l * (1 - 0.147155), 2)
                    / (o * 0.147155 + l * (1 - 0.147155))
                ),
                3,
            ),
            17,
        ),
    )
    A["A74"] = -1 * cmp_lt(
        rank(correlation(c, ts_sum(adv(30), 37), 15)),
        rank(
            correlation(rank(h * 0.0261661 + vwap * (1 - 0.0261661)), rank(v), 11)
        ),
    )
    A["A75"] = cmp_lt(
        rank(correlation(vwap, v, 4)), rank(correlation(rank(l), rank(adv(50)), 12))
    )
    A["A76"] = -1 * np.maximum(
        rank(decay_linear(delta(vwap, 1), 12)),
        ts_rank(
            decay_linear(
                ts_rank(correlation(neut(l), adv(81), 8), 20), 17
            ),
            19,
        ),
    )
    A["A77"] = np.minimum(
        rank(decay_linear(((h + l) / 2 + h) - (vwap + h), 20)),
        rank(decay_linear(correlation((h + l) / 2, adv(40), 3), 6)),
    )
    A["A78"] = rank(
        correlation(ts_sum(l * 0.352233 + vwap * (1 - 0.352233), 20), ts_sum(adv(40), 20), 7)
    ) ** rank(correlation(rank(vwap), rank(v), 6))
    A["A79"] = cmp_lt(
        rank(delta(neut(c * 0.60733 + o * (1 - 0.60733)), 1)),
        rank(correlation(ts_rank(vwap, 4), ts_rank(adv(150), 9), 15)),
    )
    A["A80"] = -1 * (
        rank(np.sign(delta(neut(o * 0.868128 + h * (1 - 0.868128)), 4)))
        ** ts_rank(correlation(h, adv(10), 5), 6)
    )
    A["A81"] = -1 * cmp_lt(
        rank(np.log(product(rank(rank(correlation(vwap, ts_sum(adv(10), 50), 8)) ** 4), 15))),
        rank(correlation(rank(vwap), rank(v), 5)),
    )
    A["A82"] = -1 * np.minimum(
        rank(decay_linear(delta(o, 1), 15)),
        ts_rank(decay_linear(correlation(neut(v), o, 17), 7), 13),
    )
    A["A83"] = (
        rank(delay((h - l) / (ts_sum(c, 5) / 5), 2)) * rank(rank(v))
    ) / ((h - l) / (ts_sum(c, 5) / 5) / (vwap - c))
    A["A84"] = -1 * cmp_lt(
        ts_rank(vwap - ts_max(vwap, 15), 20),
        rank(correlation(c, ts_sum(adv(60), 5), 17)),
    )
    A["A85"] = rank(
        correlation(h * 0.876703 + c * (1 - 0.876703), adv(30), 10)
    ) ** rank(correlation(ts_rank((h + l) / 2, 4), ts_rank(v, 10), 7))
    A["A86"] = -1 * cmp_lt(
        ts_rank(correlation(c, ts_sum(adv(20), 15), 6), 20),
        rank((o + c) - (vwap + o)),
    )
    A["A87"] = -1 * np.maximum(
        rank(decay_linear(delta(c * 0.369701 + vwap * (1 - 0.369701), 2), 3)),
        ts_rank(
            decay_linear(correlation(neut(adv(81)), c, 13).abs(), 5), 14
        ),
    )
    A["A88"] = np.minimum(
        rank(decay_linear((rank(o) + rank(l)) - (rank(h) + rank(c)), 8)),
        ts_rank(decay_linear(correlation(ts_rank(c, 8), ts_rank(adv(60), 21), 8), 7), 3),
    )
    A["A89"] = ts_rank(decay_linear(correlation(l, adv(10), 7), 6), 4) - ts_rank(
        decay_linear(delta(neut(vwap), 3), 10), 15
    )
    A["A90"] = -1 * (
        rank(c - ts_max(c, 5)) ** ts_rank(correlation(neut(adv(40)), l, 5), 3)
    )
    A["A91"] = -1 * (
        ts_rank(
            decay_linear(decay_linear(correlation(neut(c), v, 10), 16), 4), 5
        )
        - rank(decay_linear(correlation(vwap, adv(30), 4), 3))
    )
    A["A92"] = np.minimum(
        ts_rank(decay_linear(((h + l) / 2 + c) < (l + o), 15).astype(float), 19),
        ts_rank(decay_linear(correlation(rank(l), rank(adv(30)), 8), 7), 7),
    )
    A["A93"] = ts_rank(decay_linear(correlation(neut(vwap), adv(81), 17), 20), 8) / rank(
        decay_linear(delta(c * 0.524434 + vwap * (1 - 0.524434), 3), 16)
    )
    A["A94"] = -1 * (
        rank(vwap - ts_min(vwap, 12))
        ** ts_rank(correlation(ts_rank(vwap, 20), ts_rank(adv(60), 4), 18), 3)
    )
    A["A95"] = cmp_lt(
        rank(o - ts_min(o, 12)),
        ts_rank(
            rank(correlation(ts_sum((h + l) / 2, 19), ts_sum(adv(40), 19), 13)) ** 5,
            12,
        ),
    )
    A["A96"] = -1 * np.maximum(
        ts_rank(decay_linear(correlation(rank(vwap), rank(v), 4), 4), 8),
        ts_rank(
            decay_linear(
                ts_argmax(
                    correlation(ts_rank(c, 8), ts_rank(adv(60), 4), 4), 13
                ),
                14,
            ),
            13,
        ),
    )
    A["A97"] = -1 * (
        rank(
            decay_linear(
                delta(neut(l * 0.721001 + vwap * (1 - 0.721001)), 3), 20
            )
        )
        - ts_rank(
            decay_linear(
                ts_rank(correlation(ts_rank(l, 8), ts_rank(adv(60), 17), 5), 19), 16
            ),
            7,
        )
    )
    A["A98"] = rank(
        decay_linear(correlation(vwap, ts_sum(adv(5), 26), 5), 7)
    ) - rank(
        decay_linear(
            ts_rank(ts_argmin(correlation(rank(o), rank(adv(15)), 21), 9), 7), 8
        )
    )
    A["A99"] = -1 * cmp_lt(
        rank(correlation(ts_sum((h + l) / 2, 20), ts_sum(adv(60), 20), 9)),
        rank(correlation(l, v, 6)),
    )
    A["A100"] = (
        -1
        * (
            1.5
            * scale(neut(neut(rank(((c - l) - (h - c)) / (h - l) * v))))
            - scale(
                neut(correlation(c, rank(adv(20)), 5) - rank(ts_argmin(c, 30)))
            )
        )
        * (v / adv(20))
    )
    A["A101"] = (c - o) / ((h - l) + 0.001)

    # sanitize inf
    for k, val in A.items():
        A[k] = val.replace([np.inf, -np.inf], np.nan)
    return A


# --------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------


def _universe() -> set[str]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code FROM stock_basic
            WHERE market IN ('主板','创业板','科创板')
              AND (name IS NULL OR name NOT ILIKE '%%ST%%')
            """
        )
        return {str(r[0]) for r in cur.fetchall()}


def _adj_latest() -> dict[str, float]:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT ON (ts_code) ts_code, adj_factor FROM daily "
            "ORDER BY ts_code, trade_date DESC"
        )
        return {str(ts): float(a) for ts, a in cur.fetchall() if a}


def _industry_groups() -> pd.Series:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT ts_code, industry_name FROM stock_eastmoney_industry")
        return pd.Series({str(ts): str(ind) for ts, ind in cur.fetchall()})


def load_panel(start: str, end: str):
    load_start = (date.fromisoformat(start) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    load_end = (date.fromisoformat(end) + timedelta(days=FWD_BUFFER_DAYS)).isoformat()
    universe = _universe()
    adj_latest = _adj_latest()
    groups = _industry_groups()

    print(f"  loading daily {load_start}..{load_end}", flush=True)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, ts_code, open, high, low, close, vol, amount, adj_factor
            FROM daily
            WHERE trade_date >= %s AND trade_date <= %s
            ORDER BY trade_date
            """,
            (load_start, load_end),
        )
        rows = cur.fetchall()
    df = pd.DataFrame(
        rows,
        columns=[
            "trade_date", "ts_code", "open", "high", "low", "close",
            "vol", "amount", "adj_factor",
        ],
    )
    df = df[df["ts_code"].isin(universe)].copy()
    for col in ["open", "high", "low", "close", "vol", "amount", "adj_factor"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_date"] = df["trade_date"].astype(str)
    df["adj_latest"] = df["ts_code"].map(adj_latest)
    df = df[df["adj_latest"].notna() & (df["vol"] > 0) & (df["amount"] > 0)]
    # qfq vwap: amount(千元)*10 / vol(手) = raw price; scale to qfq basis
    df["vwap"] = (df["amount"] * 10.0 / df["vol"]) * (df["adj_factor"] / df["adj_latest"])
    print(f"  daily rows in universe: {len(df)}", flush=True)

    def wide(col: str) -> pd.DataFrame:
        return df.pivot(index="trade_date", columns="ts_code", values=col).sort_index()

    panel: dict[str, pd.DataFrame] = {
        "open": wide("open"),
        "high": wide("high"),
        "low": wide("low"),
        "close": wide("close"),
        "vwap": wide("vwap"),
        "amount": wide("amount"),
    }
    panel["volume"] = panel["amount"]
    panel["returns"] = panel["close"] / panel["close"].shift(1) - 1

    # market cap (total_mv, 万元)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, ts_code, total_mv FROM stock_dailybasic "
            "WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL",
            (load_start, load_end),
        )
        mv_rows = cur.fetchall()
    mv = pd.DataFrame(mv_rows, columns=["trade_date", "ts_code", "total_mv"])
    mv["trade_date"] = mv["trade_date"].astype(str)
    mv["total_mv"] = pd.to_numeric(mv["total_mv"], errors="coerce")
    cap = mv.pivot(index="trade_date", columns="ts_code", values="total_mv").sort_index()
    panel["cap"] = cap.reindex(index=panel["close"].index, columns=panel["close"].columns)

    return panel, groups


# --------------------------------------------------------------------------
# Evaluation
# --------------------------------------------------------------------------


def _forward_returns(close: pd.DataFrame, h: int) -> pd.DataFrame:
    return close.shift(-h) / close - 1


def evaluate(
    factor: pd.DataFrame,
    fwd: pd.DataFrame,
    tradable: pd.DataFrame,
    win_dates: list[str],
) -> dict:
    f = factor.reindex(win_dates).where(tradable.reindex(win_dates))
    y = fwd.reindex(win_dates).where(tradable.reindex(win_dates))
    n = (f.notna() & y.notna()).sum(axis=1)
    ic = f.rank(axis=1).corrwith(y.rank(axis=1), axis=1)
    ic = ic[n >= MIN_STOCKS_PER_DAY].dropna()
    if len(ic) == 0:
        return {"n_days": 0, "ic_mean": None, "ic_ir": None, "hit": None,
                "q_avg": None, "spread": None, "mono": False, "avg_n": 0,
                "turnover": None, "net_spread": None}

    ic_mean = float(ic.mean())
    ic_std = float(ic.std(ddof=1)) if len(ic) > 1 else 0.0
    ic_ir = float(ic_mean / ic_std) if ic_std and ic_std > 0 else None
    hit = float((ic > 0).mean())

    # quintiles + turnover
    q = f.rank(axis=1, pct=True)
    bucket = np.ceil(q * 5).clip(1, 5)
    sums = {b: 0.0 for b in range(1, 6)}
    cnts = {b: 0 for b in range(1, 6)}
    w = pd.DataFrame(0.0, index=f.index, columns=f.columns)
    for t in f.index:
        yv = y.loc[t]
        fb = bucket.loc[t]
        for b in range(1, 6):
            m = (fb == b) & yv.notna()
            if m.any():
                sums[b] += float(yv[m].mean())
                cnts[b] += 1
        q5 = fb == 5
        q1 = fb == 1
        n5, n1 = int(q5.sum()), int(q1.sum())
        if n5:
            w.loc[t, q5] = 1.0 / n5
        if n1:
            w.loc[t, q1] = -1.0 / n1
    q_avg = [sums[b] / cnts[b] if cnts[b] else None for b in range(1, 6)]
    spread = (q_avg[4] - q_avg[0]) if (q_avg[4] is not None and q_avg[0] is not None) else None
    mono = False
    if all(x is not None for x in q_avg):
        inc = all(q_avg[i] <= q_avg[i + 1] for i in range(4))
        dec = all(q_avg[i] >= q_avg[i + 1] for i in range(4))
        mono = inc or dec
    turnover = float(w.diff().abs().sum(axis=1).mean() / 2.0)
    net_spread = (
        float(spread - turnover * COST_ROUNDTRIP * PRIMARY_H) if spread is not None else None
    )
    return {
        "n_days": int(len(ic)),
        "ic_mean": ic_mean,
        "ic_std": float(ic_std),
        "ic_ir": ic_ir,
        "hit": hit,
        "q_avg": q_avg,
        "spread": float(spread) if spread is not None else None,
        "mono": bool(mono),
        "avg_n": float(n[n >= MIN_STOCKS_PER_DAY].mean()),
        "turnover": turnover,
        "net_spread": net_spread,
    }


def run_window(name: str, start: str, end: str, only: set[str] | None):
    print(f"[{name}] {start}..{end}", flush=True)
    panel, groups = load_panel(start, end)
    close = panel["close"]
    fwd = {h: _forward_returns(close, h) for h in HORIZONS}
    avg20 = panel["amount"].rolling(20, min_periods=10).mean()
    tradable = (avg20 >= LIQ_FLOOR_QIAN) & close.notna()
    win_dates = [d for d in close.index if start <= d <= end]
    print(f"[{name}] window days {len(win_dates)}, assets {close.shape[1]}", flush=True)

    alphas = alpha101_lib(panel, groups)
    out: dict[str, dict] = {}
    t0 = time.time()
    for i, (aname, afac) in enumerate(alphas.items()):
        if only and aname not in only:
            continue
        per_h = {}
        for h in HORIZONS:
            per_h[str(h)] = evaluate(afac, fwd[h], tradable, win_dates)
        out[aname] = per_h
        if i % 10 == 0 or i == len(alphas) - 1:
            print(
                f"  [{name}] {i + 1}/{len(alphas)} {aname} "
                f"h5 IC={per_h['5']['ic_mean']} IR={per_h['5']['ic_ir']} "
                f"({round(time.time() - t0, 1)}s)",
                flush=True,
            )
        del afac
    return out


def _verdict(window_metrics: dict[str, dict]) -> dict:
    names: set[str] = set()
    for m in window_metrics.values():
        names.update(m)
    out = {}
    for a in names:
        recs = {w: window_metrics[w].get(a, {}).get(str(PRIMARY_H), {}) for w in window_metrics}
        ics = [recs[w].get("ic_mean") for w in recs]
        if any(x is None for x in ics):
            out[a] = {"verdict": "NO-DATA", "windows": recs}
            continue
        signs = [1 if x > 0 else (-1 if x < 0 else 0) for x in ics]
        sign_ok = len(set(s for s in signs if s != 0)) == 1 and 0 not in signs
        irs = [recs[w].get("ic_ir") for w in recs]
        irs_abs = [abs(x) for x in irs if x is not None]
        mono = [recs[w].get("mono") for w in recs]
        ndays = [recs[w].get("n_days", 0) for w in recs]
        avgn = [recs[w].get("avg_n", 0) or 0 for w in recs]
        nets = [recs[w].get("net_spread") for w in recs]
        big = len(irs_abs) == len(recs) and all(x >= 0.5 for x in irs_abs)
        big_cand = len(irs_abs) == len(recs) and all(x >= 0.3 for x in irs_abs)
        mono_ok = sum(1 for m in mono if m) >= 2
        enough = all(n >= 100 for n in ndays) and all(n >= 100 for n in avgn)
        net_ok = all(x is not None and x > 0 for x in nets)
        if sign_ok and big and mono_ok and enough and net_ok:
            v = "PASS"
        elif sign_ok and (big_cand or (mono_ok and enough)):
            v = "CANDIDATE"
        else:
            v = "REJECT"
        out[a] = {
            "verdict": v,
            "windows": recs,
            "ic_irs": irs,
            "sign_ok": sign_ok,
            "mono_count": sum(1 for m in mono if m),
        }
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--alphas", default="", help="comma subset e.g. A1,A5,A101")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]
    only = {a.strip() for a in args.alphas.split(",") if a.strip()} or None

    allw: dict[str, dict] = {}
    for w in wins:
        if w not in WINDOWS:
            print(f"unknown window {w}", file=sys.stderr)
            return 2
        allw[w] = run_window(w, *WINDOWS[w], only)

    verdict = _verdict(allw)
    counts: dict[str, int] = {}
    for v in verdict.values():
        counts[v["verdict"]] = counts.get(v["verdict"], 0) + 1

    payload = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").UTC
        ).isoformat(),
        "windows": wins,
        "horizons": HORIZONS,
        "primary_horizon": PRIMARY_H,
        "universe": "CN 主板/创业板/科创板, ex-ST/BJ/HK, amount20d>=0.7亿",
        "counts": counts,
        "verdict": verdict,
        "raw": allw,
    }
    out_path = Path(args.json) if args.json else REPORT_DIR / "alpha101_screen_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}")
    print(f"counts: {counts}\n")

    # markdown table sorted by |median ICIR|
    rows = []
    for a, rec in verdict.items():
        irs = [abs(x) for x in rec.get("ic_irs", []) if x is not None]
        score = float(np.median(irs)) if irs else -1.0
        rows.append((score, a, rec))
    rows.sort(reverse=True, key=lambda x: x[0])
    print("| alpha | verdict | ICIRA/ICIRB/ICIRC | IC(OOS2/train/valid) | mono | net |")
    print("|-------|---------|--------------------|----------------------|------|-----|")
    for _score, a, rec in rows[:40]:
        recs = rec["windows"]
        irs = "/".join(
            f"{recs[w].get('ic_ir'):+.2f}" if recs[w].get("ic_ir") is not None else "na"
            for w in wins
        )
        ics = "/".join(
            f"{recs[w].get('ic_mean'):+.3f}" if recs[w].get("ic_mean") is not None else "na"
            for w in wins
        )
        nets = "/".join(
            f"{recs[w].get('net_spread'):+.4f}"
            if recs[w].get("net_spread") is not None
            else "na"
            for w in wins
        )
        print(f"| {a} | {rec['verdict']} | {irs} | {ics} | {rec.get('mono_count')}/3 | {nets} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
