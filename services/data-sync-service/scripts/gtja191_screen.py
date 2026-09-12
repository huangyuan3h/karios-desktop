#!/usr/bin/env python3
"""GTJA 191 factor L0 cross-sectional RankIC screen (read-only).

Reference: 国泰君安《基于短周期价量特征的多因子选股体系》(191 alphas).
Formulas adapted to the panel engine from the widely-circulated reference
implementation, with obvious transcription bugs fixed. Not exactly the same as
Alpha101 (but ~20-30 overlap). Pre-registration:
docs/designs/gtja191-screen-prereg-2026-09-12.md

Usage:
  PYTHONPATH=src python3 scripts/gtja191_screen.py --windows OOS2,train,valid
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alpha101_screen as base  # noqa: E402

REPORT_DIR = base.REPORT_DIR
HORIZONS = base.HORIZONS
PRIMARY_H = base.PRIMARY_H

# operator aliases
rank = base.rank
scale = base.scale
delay = base.delay
delta = base.delta
corr = base.correlation
cov = base.covariance
ts_min = base.ts_min
ts_max = base.ts_max
ts_sum = base.ts_sum
ts_std = base.stddev
ts_rank = base.ts_rank
decay = base.decay_linear


def tmean(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return df.rolling(n).mean()


def sma(df: pd.DataFrame, n: int, m: int) -> pd.DataFrame:
    return df.ewm(alpha=m / n, adjust=False).mean()


def _pos_extreme(df: pd.DataFrame, n: int, mode: str) -> pd.DataFrame:
    from numpy.lib.stride_tricks import sliding_window_view

    arr = df.to_numpy(dtype=float)
    T, N = arr.shape
    out = np.full((T, N), np.nan)
    if T < n:
        return pd.DataFrame(out, index=df.index, columns=df.columns)
    fill = -np.inf if mode == "max" else np.inf
    for j in range(0, N, 512):
        sub = arr[:, j : j + 512]
        sw = sliding_window_view(sub, n, axis=0)
        filled = np.where(np.isnan(sw), fill, sw)
        idx = np.argmax(filled, axis=2) if mode == "max" else np.argmin(filled, axis=2)
        block = (n - idx).astype(float)
        block[np.isnan(sw).all(axis=2)] = np.nan
        out[n - 1 :, j : j + 512] = block
    return pd.DataFrame(out, index=df.index, columns=df.columns)


def highday(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return _pos_extreme(df, n, "max")


def lowday(df: pd.DataFrame, n: int) -> pd.DataFrame:
    return _pos_extreme(df, n, "min")


def load_benchmark(dates, code: str = "000300.SH") -> dict[str, pd.Series]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        bm = pd.read_sql(
            f"SELECT to_char(trade_date,'YYYY-MM-DD') AS d, open, close FROM index_daily "
            f"WHERE ts_code='{code}' AND close>0",
            conn,
        )
    bm = bm.set_index("d")
    return {"open": bm["open"].reindex(dates).astype(float),
            "close": bm["close"].reindex(dates).astype(float)}


def _W(cond, a, b):
    if not isinstance(a, pd.DataFrame):
        a = pd.DataFrame(a, index=cond.index, columns=cond.columns)
    return a.where(cond, b)


def _join(recs, wins, key, fmt):
    return "/".join(
        format(recs[w][key], fmt) if recs[w].get(key) is not None else "na" for w in wins
    )


def gtja_lib(d: dict[str, pd.DataFrame], bm: dict[str, pd.Series]) -> dict[str, pd.DataFrame]:
    o, h, l, c = d["open"], d["high"], d["low"], d["close"]  # noqa: E741
    v = d["volume"]
    ap = d["vwap"]
    amt = d["amount"]
    pc = d["prev_close"]
    r = d["returns"]
    cap = d["cap"]
    A: dict[str, pd.DataFrame] = {}
    t_idx = pd.DataFrame(
        np.arange(len(c)).reshape(-1, 1).repeat(c.shape[1], axis=1),
        index=c.index, columns=c.columns,
    ).astype(float)
    up = (c > pc).astype(float)
    bmo = pd.DataFrame(np.tile(bm["open"].to_numpy()[:, None], (1, c.shape[1])),
                       index=c.index, columns=c.columns)
    bmc = pd.DataFrame(np.tile(bm["close"].to_numpy()[:, None], (1, c.shape[1])),
                       index=c.index, columns=c.columns)
    bm_dn = (bmc < bmo)
    mav = tmean(v, 20)

    A["GTJA001"] = -corr(rank(delta(np.log(v), 1)), rank((c - o) / o), 6)
    A["GTJA002"] = -delta(((c - l) - (h - c)) / (h - l), 1)
    A["GTJA003"] = ts_sum(
        (c - np.minimum(pc, l)).where(c > pc, 0) + (c - np.maximum(pc, l)).where(c < pc, 0), 6
    )
    cond1 = ts_std(c, 8) < ts_sum(c, 2) / 2
    cond2 = ts_sum(c, 2) / 2 < (ts_sum(c, 8) / 8 - ts_std(c, 8))
    cond3 = v / mav >= 1
    A["GTJA004"] = ts_sum(c, 8) / 8 + _W(cond1, -1.0, _W(cond2, 1.0, _W(cond3, 1.0, -1.0)))
    A["GTJA005"] = -ts_max(corr(ts_rank(v, 5), ts_rank(h, 5), 5), 3)
    A["GTJA006"] = -rank(np.sign(delta(o * 0.85 + h * 0.15, 4)))
    A["GTJA007"] = (rank(ts_max(ap - c, 3)) + rank(ts_min(ap - c, 3))) * rank(delta(v, 3))
    A["GTJA008"] = -rank(delta((h + l) * 0.2 / 2 + ap * 0.8, 4))
    A["GTJA009"] = sma(((h + l) * 0.5 - (h.shift() + l.shift()) * 0.5 * (h - l) / v), 7, 2)
    A["GTJA010"] = rank(np.maximum(
        _W(r < 0, ts_std(r, 20), c) ** 2, 5
    ))
    A["GTJA011"] = ts_sum(((c - l) - (h - c)) / (h - l) * v, 6)
    A["GTJA012"] = rank(o - ts_sum(ap, 10) / 10) * (-1 * rank((c - ap).abs()))
    A["GTJA013"] = (h - l) ** 0.5 - ap
    A["GTJA014"] = c - delay(c, 5)
    A["GTJA015"] = o / pc - 1
    A["GTJA016"] = -ts_max(corr(rank(v), rank(ap), 5), 5)
    A["GTJA017"] = rank(ts_max(ap, 15) - c) ** delta(c, 5)
    A["GTJA018"] = c / delay(c, 5)
    A["GTJA019"] = (
        ((c - delay(c, 5)) / delay(c, 5)).where(c < delay(c, 5), 0)
        + ((c - delay(c, 5)) / c).where(c > delay(c, 5), 0)
    )
    A["GTJA020"] = (c - delay(c, 6)) * 100 / delay(c, 6)
    A["GTJA021"] = cov(sma(c, 6, 1), t_idx, 6) / 2.9167
    A["GTJA022"] = sma((c - tmean(c, 6)) / tmean(c, 6) - delay((c - tmean(c, 6)) / tmean(c, 6), 3), 12, 1)
    A["GTJA023"] = (
        sma(ts_std(c, 20).where(c > pc, 0), 20, 1) * 100
        / (sma(ts_std(c, 20).where(c > pc, 0), 20, 1) + sma(ts_std(c, 20).where(c <= pc, 0), 20, 1))
    )
    A["GTJA024"] = sma(c - delay(c, 5), 5, 1)
    A["GTJA025"] = (
        -rank(delta(c, 7))
        * (1 - rank(decay(v / mav, 9)))
        * (1 + rank(ts_sum(r, 250)))
    )
    A["GTJA026"] = ts_sum(c, 7) / 7 - c + corr(ap, delay(c, 5), 230)
    A["GTJA028"] = 3 * sma(100 * (c - ts_min(l, 9)) / (ts_max(h, 9) - ts_min(l, 9)), 3, 1) - 2 * sma(
        sma(100 * (c - ts_min(l, 9)) / (ts_max(h, 9) - ts_min(l, 9)), 3, 1), 3, 1
    )
    A["GTJA029"] = (c - delay(c, 6)) * v / delay(c, 6)
    A["GTJA031"] = (c - tmean(c, 12)) * 100 / tmean(c, 12)
    A["GTJA032"] = -ts_sum(rank(corr(rank(h), rank(v), 3)), 3)
    A["GTJA033"] = (
        delay(ts_min(l, 5), 5) - ts_min(l, 5)
        + rank((ts_sum(r, 240) - ts_sum(r, 20)) / 220)
        + ts_rank(v, 5)
    )
    A["GTJA034"] = tmean(c, 12) / c
    A["GTJA035"] = np.minimum(
        rank(decay(delta(o, 1), 15)),
        -rank(decay(corr(o * 0.65 + ap * 0.35, v, 17), 7)),
    )
    A["GTJA036"] = rank(ts_sum(corr(rank(v), rank(ap), 6), 2))
    A["GTJA037"] = -rank(ts_sum(o, 5) * ts_sum(r, 5)) - delay(ts_sum(o, 5) * ts_sum(r, 5), 10)
    A["GTJA038"] = _W(tmean(h, 20) < h, -delta(h, 2), 0.0)
    A["GTJA039"] = rank(decay(delta(c, 2), 8)) - rank(
        decay(corr(ap * 0.3 + o * 0.7, ts_sum(tmean(v, 180), 37), 14), 12)
    )
    A["GTJA040"] = 100 * ts_sum(v.where(c > pc, 0), 26) / ts_sum(v.where(c <= pc, 0), 26)
    A["GTJA041"] = -rank(np.maximum(delta(ap, 3), 5))
    A["GTJA042"] = -corr(h, v, 10) * rank(ts_std(h, 10))
    A["GTJA043"] = ts_sum(v.where(c > pc, 0) - v.where(c < pc, 0), 6)
    A["GTJA044"] = rank(decay(corr(l, tmean(v, 10), 7), 6)) + rank(decay(delta(ap, 3), 10))
    A["GTJA045"] = rank(delta(c * 0.6 + o * 0.4, 1)) * rank(corr(ap, tmean(v, 150), 15))
    A["GTJA046"] = (tmean(c, 3) + tmean(c, 6) + tmean(c, 12) + tmean(c, 24)) * 0.25 / c
    A["GTJA047"] = sma(100 * (ts_max(h, 6) - c) / (ts_max(h, 6) - ts_min(l, 6)), 9, 1)
    A["GTJA048"] = -(
        np.sign(c - delay(c, 1)) + np.sign(delay(c, 1) - delay(c, 2)) + np.sign(delay(c, 2) - delay(c, 3))
    ) * ts_sum(v, 5) / ts_sum(v, 20)
    A["GTJA049"] = (
        ts_sum(np.maximum((h - delay(h, 1)).abs(), (l - delay(l, 1)).abs()).where(
            h + l < delay(h, 1) + delay(l, 1), 0), 12)
        / (
            ts_sum(np.maximum((h - delay(h, 1)).abs(), (l - delay(l, 1)).abs()).where(
                h + l < delay(h, 1) + delay(l, 1), 0), 12)
            + ts_sum(np.maximum((h - delay(h, 1)).abs(), (l - delay(l, 1)).abs()).where(
                h + l > delay(h, 1) + delay(l, 1), 0), 12)
        )
    )
    A["GTJA052"] = ts_sum(
        np.maximum(h - delay((h + l + c) / 3, 1), 0) + np.maximum(delay((h + l + c) / 3, 1) - l, 0), 26
    )
    A["GTJA053"] = ts_sum((c > pc).astype(float), 12) * 100 / 12
    A["GTJA054"] = -rank(((c - o).abs().rolling(10).std() + (c - o)) + corr(c, o, 10))
    A["GTJA056"] = -rank(ts_sum(r, 10) / ts_sum(ts_sum(r, 2), 3)) * rank(r * cap)
    A["GTJA057"] = sma(100 * (c - ts_min(l, 9)) / (ts_max(h, 9) - ts_min(l, 9)), 3, 1)
    A["GTJA058"] = ts_sum((c > pc).astype(float), 20) * 100 / 20
    A["GTJA059"] = ts_sum(
        c - (np.minimum(l, pc)).where(c > pc, 0) - (np.maximum(h, pc)).where(c < pc, 0), 20
    )
    A["GTJA060"] = ts_sum(v * ((c - l) - (h - c)) / (h - l), 20)
    A["GTJA061"] = np.maximum(
        rank(decay(delta(ap, 1), 12)),
        -rank(decay(rank(corr(l, tmean(v, 80), 8)), 17)),
    )
    A["GTJA062"] = -corr(h, rank(v), 5)
    A["GTJA063"] = sma(np.maximum(c - pc, 0), 6, 1) * 100 / sma((c - pc).abs(), 6, 1)
    A["GTJA064"] = np.maximum(
        rank(decay(corr(rank(ap), rank(v), 4), 4)),
        -rank(decay(np.maximum(corr(rank(c), tmean(v, 60), 4), 13), 14)),
    )
    A["GTJA065"] = tmean(c, 6) / c
    A["GTJA066"] = (c - tmean(c, 6)) / tmean(c, 6)
    A["GTJA067"] = sma(np.maximum(c - pc, 0), 24, 1) * 100 / sma((c - pc).abs(), 24, 1)
    A["GTJA068"] = sma(
        ((h + l) / 2 - delay(h, 1) + 0.5 * delay(l, 1) * (h - l) / v) * 100, 15, 2
    )
    A["GTJA070"] = ts_std(amt, 6)
    A["GTJA071"] = (c - tmean(c, 24)) / tmean(c, 24) * 100
    A["GTJA072"] = sma(100 * (ts_max(h, 6) - c) / (ts_max(h, 6) - ts_min(l, 6)), 15, 1)
    A["GTJA074"] = rank(corr(ts_sum(l * 0.35 + ap * 0.65, 20), tmean(v, 40), 7)) + rank(
        corr(rank(ap), rank(v), 6)
    )
    A["GTJA075"] = 1 - ts_sum((up * bm_dn.astype(float)), 50) / ts_sum(bm_dn.astype(float), 50)
    A["GTJA076"] = ts_std((c / pc - 1).abs() / v, 20) / tmean((c / pc - 1).abs() / v, 20)
    A["GTJA077"] = np.minimum(
        rank(decay((h + l) / 2 + h - (ap + h), 20)),
        rank(decay(corr((h + l) / 2, tmean(v, 40), 3), 6)),
    )
    tri = (h + l + c) / 3
    A["GTJA078"] = (tri - tmean(tri, 12)) / (0.015 * tmean((c - tmean(tri, 12)).abs(), 12))
    A["GTJA079"] = sma(np.maximum(c - pc, 0), 12, 1) * 100 / sma((c - pc).abs(), 12, 1)
    A["GTJA080"] = (v - delay(v, 5)) / delay(v, 5) * 100
    A["GTJA081"] = sma(v, 21, 2)
    A["GTJA082"] = sma(100 * (ts_max(h, 6) - c) / (ts_max(h, 6) - ts_min(l, 6)), 20, 1)
    A["GTJA083"] = -corr(ts_rank(h, 5), ts_rank(v, 5), 5)
    A["GTJA084"] = ts_sum(v.where(c > pc, 0) - v.where(c < pc, 0), 20)
    A["GTJA085"] = ts_rank(v / mav, 20) * ts_rank(-delta(c, 7), 8)
    A["GTJA086"] = _W(
        (delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10 > 0.25,
        -1.0,
        _W((delay(c, 20) - delay(c, 10)) / 10 - (delay(c, 10) - c) / 10 < 0, 1.0,
           -(c - delay(c, 1))),
    )
    A["GTJA087"] = rank(decay(delta(ap, 4), 7)) + rank(
        decay(-(l - ap) / (o - 0.5 * (h + l)), 11)
    )
    A["GTJA088"] = (c - delay(c, 20)) / delay(c, 20) * 100
    e13 = sma(c, 13, 2)
    e27 = sma(c, 27, 2)
    A["GTJA089"] = 2 * (e13 - e27 - sma(e13 - e27, 10, 2))
    A["GTJA090"] = -rank(corr(rank(ap), rank(v), 5))
    A["GTJA091"] = -rank(c - ts_max(c, 5)) * rank(corr(tmean(v, 40), l, 5))
    A["GTJA092"] = -np.maximum(
        rank(decay(delta(c * 0.35 + ap * 0.65, 2), 3)),
        ts_rank(decay(corr(tmean(v, 180), c, 13).abs(), 5), 15),
    )
    A["GTJA093"] = ts_sum(
        np.maximum(np.maximum(o - l, o - delay(o, 1)), 0).where(o < delay(o, 1), 0), 20
    )
    A["GTJA094"] = ts_sum(v.where(c > pc, 0) - v.where(c < pc, 0), 30)
    A["GTJA095"] = ts_std(amt, 20)
    A["GTJA096"] = sma(sma(100 * (c - ts_min(l, 9)) / (ts_max(h, 9) - ts_min(l, 9)), 3, 1), 3, 1)
    A["GTJA097"] = ts_std(v, 10)
    A["GTJA098"] = _W(
        delta(ts_sum(c, 100) / 100, 100) / delay(c, 100) <= 0.05,
        -(c - ts_min(c, 100)),
        -delta(c, 3),
    )
    A["GTJA099"] = -rank(cov(rank(c), rank(v), 5))
    A["GTJA100"] = ts_std(v, 20)
    A["GTJA101"] = -(
        rank(corr(c, ts_sum(tmean(v, 30), 37), 15))
        < rank(corr(rank(h * 0.1 + ap * 0.9), rank(v), 11))
    ).astype(float)
    A["GTJA102"] = sma(np.maximum(v - delay(v, 1), 0), 6, 1) * 100 / sma((v - delay(v, 1)).abs(), 6, 1)
    A["GTJA103"] = (20 - lowday(l, 20)) / 20 * 100
    A["GTJA104"] = -delta(corr(h, v, 5), 5) * rank(ts_std(c, 20))
    A["GTJA105"] = -corr(rank(o), rank(v), 10)
    A["GTJA106"] = c - delay(c, 20)
    A["GTJA107"] = -rank(o - delay(h, 1)) * rank(o - delay(c, 1)) * rank(o - delay(l, 1))
    A["GTJA108"] = -(rank(h - ts_min(h, 2)) ** rank(corr(ap, tmean(v, 120), 6)))
    A["GTJA109"] = sma(h - l, 10, 2) / sma(sma(h - l, 10, 2), 10, 2)
    A["GTJA110"] = ts_sum(np.maximum(h - pc, 0), 20) / ts_sum(np.maximum(pc - l, 0), 20) * 100
    A["GTJA111"] = sma(v * ((c - l) - (h - c)) / (h - l), 11, 2) - sma(
        v * ((c - l) - (h - c)) / (h - l), 4, 2
    )
    gp = np.maximum(c - pc, 0)
    gl = np.maximum(pc - c, 0)
    A["GTJA112"] = (ts_sum(gp, 12) - ts_sum(gl, 12)) / (ts_sum(gp, 12) + ts_sum(gl, 12)) * 100
    A["GTJA113"] = (
        -rank(tmean(delay(c, 5), 20)) * corr(c, v, 2) * rank(corr(ts_sum(c, 5), ts_sum(c, 20), 2))
    )
    A["GTJA114"] = (
        rank(delay((h - l) / (ts_sum(c, 5) / 5), 2)) * rank(rank(v))
        / ((h - l) / (ts_sum(c, 5) / 5) / (ap - c))
    )
    A["GTJA115"] = rank(corr(h * 0.9 + c * 0.1, tmean(v, 30), 10)) ** rank(
        corr(ts_rank((h + l) / 2, 4), ts_rank(v, 10), 7)
    )
    A["GTJA116"] = corr(c, t_idx, 20)
    A["GTJA117"] = ts_rank(v, 32) * (1 - ts_rank(c + h - l, 16)) * (1 - ts_rank(r, 32))
    A["GTJA118"] = ts_sum(h - o, 20) / ts_sum(o - l, 20) * 100
    A["GTJA119"] = rank(decay(corr(ap, ts_sum(tmean(v, 5), 26), 5), 7)) - rank(
        decay(ts_rank(ts_min(corr(rank(o), rank(tmean(v, 15)), 21), 9), 7), 8)
    )
    A["GTJA120"] = rank(ap - c) / rank(ap + c)
    A["GTJA122"] = sma(sma(sma(np.log(c), 13, 2), 13, 2), 13, 2) / delay(
        sma(sma(sma(np.log(c), 13, 2), 13, 2), 13, 2), 1
    ) - 1
    A["GTJA123"] = -(rank(corr(ts_sum((h + l) / 2, 20), ts_sum(tmean(v, 60), 20), 9))
                     < rank(corr(l, v, 6))).astype(float)
    A["GTJA124"] = (c - ap) / decay(rank(ts_max(c, 30)), 2)
    A["GTJA125"] = rank(decay(corr(ap, tmean(v, 80), 17), 20)) / rank(
        decay(delta(c * 0.5 + ap * 0.5, 3), 16)
    )
    A["GTJA126"] = (c + h + l) / 3
    A["GTJA129"] = ts_sum(np.maximum(pc - c, 0), 12)
    A["GTJA130"] = rank(decay(corr((h + l) / 2, tmean(v, 40), 9), 10)) / rank(
        decay(corr(rank(ap), rank(v), 7), 3)
    )
    A["GTJA132"] = tmean(amt, 20)
    A["GTJA133"] = (20 - highday(h, 20)) / 20 * 100 - (20 - lowday(l, 20)) / 20 * 100
    A["GTJA134"] = (c / delay(c, 12) - 1) * v
    A["GTJA135"] = sma(delay(c / delay(c, 20), 1), 20, 1)
    A["GTJA136"] = -rank(delta(r, 3)) * corr(o, v, 10)
    A["GTJA138"] = -(
        rank(decay(delta(l * 0.7 + ap * 0.3, 3), 20))
        - ts_rank(decay(ts_rank(corr(ts_rank(l, 8), ts_rank(tmean(v, 60), 17), 5), 19), 16), 7)
    )
    A["GTJA139"] = -corr(o, v, 10)
    A["GTJA141"] = -rank(corr(rank(h), rank(tmean(v, 15)), 9))
    A["GTJA142"] = (
        -rank(ts_rank(c, 10)) * rank(delta(delta(c, 1), 1)) * rank(ts_rank(v / mav, 5))
    )
    A["GTJA144"] = ts_sum(((c / pc - 1).abs() / amt).where(c < pc, 0), 20) / ts_sum(
        (c < pc).astype(float), 20
    )
    A["GTJA145"] = (tmean(v, 9) - tmean(v, 26)) / tmean(v, 12) * 100
    A["GTJA148"] = -(rank(corr(o, ts_sum(tmean(v, 60), 9), 6)) < rank(o - ts_min(o, 14))).astype(float)
    A["GTJA150"] = (c + h + l) / 3 * v
    A["GTJA152"] = sma(
        tmean(delay(sma(delay(c / delay(c, 9), 1), 9, 1), 1), 12)
        - tmean(delay(sma(delay(c / delay(c, 9), 1), 9, 1), 1), 26),
        9, 1,
    )
    A["GTJA153"] = (tmean(c, 3) + tmean(c, 6) + tmean(c, 12) + tmean(c, 24)) / 4
    A["GTJA154"] = (ap - ts_min(ap, 16)) < corr(ap, tmean(v, 180), 18)
    A["GTJA155"] = sma(v, 13, 2) - sma(v, 27, 2) - sma(sma(v, 13, 2) - sma(v, 27, 2), 10, 2)
    A["GTJA156"] = -np.maximum(
        rank(decay(delta(ap, 5), 3)),
        rank(decay(-delta(o * 0.15 + l * 0.85, 2) / (o * 0.15 + l * 0.85), 3)),
    )
    A["GTJA157"] = (
        np.minimum(rank(rank(np.log(ts_min(rank(rank(-rank(delta(c - 1, 5)))), 2)))), 5)
        + ts_rank(delay(-r, 6), 5)
    )
    A["GTJA158"] = ((h - sma(c, 15, 2)) - (l - sma(c, 15, 2))) / c
    A["GTJA159"] = (
        (c - ts_sum(np.minimum(l, pc), 6)) / ts_sum(np.maximum(h, pc) - np.minimum(l, pc), 6) * 12 * 24
        + (c - ts_sum(np.minimum(l, pc), 12)) / ts_sum(np.maximum(h, pc) - np.minimum(l, pc), 12) * 6 * 24
        + (c - ts_sum(np.minimum(l, pc), 24)) / ts_sum(np.maximum(h, pc) - np.minimum(l, pc), 24) * 6 * 24
    ) * 100 / (6 * 12 + 12 * 24 + 6 * 24)
    A["GTJA160"] = sma(ts_std(c, 20).where(c <= delay(c, 1), 0), 20, 1)
    A["GTJA161"] = tmean(
        np.maximum(np.maximum(h - l, (pc - h).abs()), (pc - l).abs()), 12
    )
    A["GTJA162"] = (
        sma(np.maximum(c - pc, 0), 12, 1) / sma((c - pc).abs(), 12, 1) * 100
        - np.minimum(sma(np.maximum(c - pc, 0), 12, 1) / sma((c - pc).abs(), 12, 1) * 100, 12)
    ) / (
        np.maximum(sma(np.maximum(c - pc, 0), 12, 1) / sma((c - pc).abs(), 12, 1) * 100, 12)
        - np.minimum(sma(np.maximum(c - pc, 0), 12, 1) / sma((c - pc).abs(), 12, 1) * 100, 12)
    )
    A["GTJA163"] = rank(-r * tmean(v, 20) * ap * (h - c))
    A["GTJA164"] = (
        sma(
            _W(c > pc, 1 / (c - pc), 1.0)
            - np.minimum(_W(c > pc, 1 / (c / pc), 1.0), 12) / (h - l) * 100,
            13, 2,
        )
    )
    A["GTJA167"] = ts_sum(np.maximum(c - pc, 0), 12)
    A["GTJA168"] = -v / tmean(v, 20)
    A["GTJA169"] = sma(
        tmean(delay(sma(c - pc, 9, 1), 1), 12) - tmean(delay(sma(c - pc, 9, 1), 1), 26), 10, 1
    )
    A["GTJA170"] = (
        (rank(1 / c) * v / mav) * (h * rank(h - c) / (ts_sum(h, 5) / 5))
        - rank(ap - delay(ap, 5))
    )
    A["GTJA171"] = -1 * (l - c) * (o ** 5) / ((c - h) * (c ** 5))
    hd = h - delay(h, 1)
    ld = delay(l, 1) - l
    tr = np.maximum(np.maximum(h - l, (h - delay(c, 1)).abs()), (l - delay(c, 1)).abs())
    sum_tr = ts_sum(tr, 14)
    dm_plus = hd.where((hd > 0) & (hd > ld), 0) * 100 / sum_tr
    dm_minus = ld.where((ld > 0) & (ld > hd), 0) * 100 / sum_tr
    dmi_ratio = (ts_sum(dm_plus, 14) - ts_sum(dm_minus, 14)).abs() / (
        ts_sum(dm_plus, 14) + ts_sum(dm_minus, 14)
    ) * 100
    A["GTJA172"] = tmean(dmi_ratio, 6)
    A["GTJA173"] = 3 * sma(c, 13, 2) - 2 * sma(sma(c, 13, 2), 13, 2) + sma(
        sma(sma(np.log(c), 13, 2), 13, 2), 13, 2
    )
    A["GTJA174"] = sma(ts_std(c, 20).where(c > pc, 0), 20, 1)
    A["GTJA175"] = tmean(np.maximum(np.maximum(h - l, (pc - h).abs()), (pc - l).abs()), 6)
    A["GTJA176"] = corr(
        rank((c - ts_min(l, 12)) / (ts_max(h, 12) - ts_min(l, 12))), rank(v), 6
    )
    A["GTJA177"] = (20 - highday(h, 20)) / 20 * 100
    A["GTJA178"] = (c - pc) / pc * v
    A["GTJA179"] = rank(corr(ap, v, 4)) * rank(corr(rank(l), rank(tmean(v, 50)), 12))
    A["GTJA180"] = _W(
        mav < v,
        -ts_rank(delta(c, 7).abs(), 60) * np.sign(delta(c, 7)),
        -v,
    )
    A["GTJA182"] = ts_sum(
        (((c > o) & (bmc > bmo)) | ((c < o) & (bmc < bmo))).astype(float), 20
    ) / 20
    A["GTJA184"] = rank(corr(delay(o - c, 1), c, 200)) + rank(o - c)
    A["GTJA185"] = rank(-((1 - o / c) ** 2))
    A["GTJA186"] = (tmean(dmi_ratio, 6) + delay(tmean(dmi_ratio, 6), 6)) / 2
    A["GTJA187"] = ts_sum(
        np.maximum(np.maximum(h - o, o - delay(o, 1)), 0).where(o > delay(o, 1), 0), 20
    )
    A["GTJA188"] = ((h - l) - sma(h - l, 11, 2)) / sma(h - l, 11, 2) * 100
    A["GTJA189"] = tmean((c - tmean(c, 6)).abs(), 6)
    A["GTJA191"] = corr(tmean(v, 20), l, 5) + (h + l) / 2 - c

    for k, val in list(A.items()):
        A[k] = val.replace([np.inf, -np.inf], np.nan)
        if isinstance(A[k], pd.Series):
            A[k] = A[k].to_frame()
    return A


def run_window(name: str, start: str, end: str, only: set[str] | None):
    print(f"[{name}] {start}..{end}", flush=True)
    panel, groups = base.load_panel(start, end)
    close = panel["close"]
    panel["prev_close"] = close.shift(1)
    fwd = {h: base._forward_returns(close, h) for h in HORIZONS}
    avg20 = panel["amount"].rolling(20, min_periods=10).mean()
    tradable = (avg20 >= base.LIQ_FLOOR_QIAN) & close.notna()
    win_dates = [d for d in close.index if start <= d <= end]
    bm = load_benchmark(close.index)
    alphas = gtja_lib(panel, bm)
    print(f"[{name}] window days {len(win_dates)}, alphas {len(alphas)}", flush=True)
    out: dict[str, dict] = {}
    t0 = time.time()
    for i, (aname, afac) in enumerate(alphas.items()):
        if only and aname not in only:
            continue
        out[aname] = {str(h): base.evaluate(afac, fwd[h], tradable, win_dates) for h in HORIZONS}
        if i % 20 == 0 or i == len(alphas) - 1:
            print(f"  [{name}] {i + 1}/{len(alphas)} {aname} "
                  f"h5 IC={out[aname][str(PRIMARY_H)]['ic_mean']} "
                  f"({round(time.time() - t0, 1)}s)", flush=True)
        del afac
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--alphas", default="")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]
    only = {a.strip() for a in args.alphas.split(",") if a.strip()} or None

    allw = {}
    for w in wins:
        if w not in base.WINDOWS:
            print(f"unknown window {w}", file=sys.stderr)
            return 2
        allw[w] = run_window(w, *base.WINDOWS[w], only)

    verdict = base._verdict(allw)
    counts: dict[str, int] = {}
    for v in verdict.values():
        counts[v["verdict"]] = counts.get(v["verdict"], 0) + 1
    payload = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").UTC
        ).isoformat(),
        "windows": wins, "horizons": HORIZONS, "primary_horizon": PRIMARY_H,
        "universe": "CN 主板/创业板/科创板, ex-ST/BJ/HK, amount20d>=0.7亿",
        "benchmark": "000300.SH", "counts": counts, "verdict": verdict, "raw": allw,
    }
    out_path = Path(args.json) if args.json else REPORT_DIR / "gtja191_screen_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}\ncounts: {counts}\n")

    rows = []
    for a, rec in verdict.items():
        irs = [abs(x) for x in rec.get("ic_irs", []) if x is not None]
        rows.append((float(np.median(irs)) if irs else -1.0, a, rec))
    rows.sort(reverse=True, key=lambda x: x[0])
    print("| alpha | verdict | ICIR O/T/V | IC O/T/V | mono | net O/T/V |")
    print("|-------|---------|-----------|----------|------|-----------|")
    for _s, a, rec in rows[:40]:
        recs = rec["windows"]
        print(f"| {a} | {rec['verdict']} | {_join(recs, wins, 'ic_ir', '+.2f')} | "
              f"{_join(recs, wins, 'ic_mean', '+.3f')} | "
              f"{rec.get('mono_count')}/3 | {_join(recs, wins, 'net_spread', '+.4f')} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
