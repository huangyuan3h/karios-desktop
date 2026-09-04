"""State-bucket sliced stock leg — per-state independent slots + weighted NAV blend.

Replaces union (shared-slot OR) with R10 slice structure for S-3 STOCK-leg experiments.
Executable口径: skip_t1_limit + ST/BJ/delist filter (via state_bucket_track loaders).

Design: docs/designs/state-bucket-slice-stock-leg.md
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import numpy as np
import psycopg

from data_sync_service.config import get_settings
from data_sync_service.service.state_bucket_track import (
    COSTS_ROUNDTRIP,
    POSITION_PCT,
    R_WIDE_THRESHOLD,
    WARMUP_CAL_DAYS,
    _load_calendar,
    _load_mv,
    _load_rows,
    _t1_limit_locked,
)

# Per-state frozen params (R11/R12 · state-bucket-algo §7)
STATE_PARAMS: dict[str, dict[str, int]] = {
    "S-limit": {"body": 3, "bucket_q": 2, "max_pos": 10},
    "S-gap": {"body": 3, "bucket_q": 3, "max_pos": 15},
    "S-shrink": {"body": 15, "bucket_q": 2, "max_pos": 15},
    "S-fresh": {"body": 15, "bucket_q": 5, "max_pos": 10},
}

ALL_STATES = ("S-limit", "S-gap", "S-shrink", "S-fresh")

# Named slice variants for compare script / Phase 2 API.
SLICE_VARIANTS: dict[str, tuple[tuple[str, ...], tuple[float, ...]]] = {
    "L": (("S-limit",), (1.0,)),
    "G": (("S-gap",), (1.0,)),
    "S": (("S-shrink",), (1.0,)),
    "F": (("S-fresh",), (1.0,)),
    "slice2_LG": (("S-limit", "S-gap"), (0.5, 0.5)),
    "slice3_LGS": (("S-limit", "S-gap", "S-shrink"), (1 / 3, 1 / 3, 1 / 3)),
    "slice2_L70": (("S-limit", "S-gap"), (0.7, 0.3)),
    "slice2_L60": (("S-limit", "S-gap"), (0.6, 0.4)),
    "slice2_L50": (("S-limit", "S-gap"), (0.5, 0.5)),
    "slice2_L40": (("S-limit", "S-gap"), (0.4, 0.6)),
    "slice2_L30": (("S-limit", "S-gap"), (0.3, 0.7)),
}


def _load_list_dates() -> dict[str, Any]:
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute("SELECT ts_code, list_date FROM stock_basic")
    out = {str(r[0]): r[1] for r in cur.fetchall()}
    conn.close()
    return out


def _limit_ratio(ts: str) -> float:
    if ts.endswith((".SH", ".SZ")) and (ts.startswith("60") or ts.startswith("00")):
        return 1.095
    return 1.195


def _day_state_fv(
    per_ts: dict[str, list[dict[str, Any]]],
    mv_map: dict[str, dict[str, float]],
    list_dates: dict[str, Any],
    day: str,
    date_idx: dict[str, dict[str, int]],
) -> tuple[dict[str, dict[str, float]], float]:
    """Map state -> {ts: amplitude} for stocks in that state on ``day``."""
    day_all: dict[str, dict[str, Any]] = {}
    day_fv: dict[str, float] = {}
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 0 or idx < 20:
            continue
        mv = mv_map.get(day, {}).get(ts)
        if mv is None:
            continue
        cur = series[idx]
        if not cur["close"] or not cur["high"] or not cur["low"] or cur["close"] <= 0:
            continue
        amp = (cur["high"] - cur["low"]) / cur["close"]
        amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        turn = (cur["amount"] / avg20) if avg20 and avg20 > 0 else np.nan
        pc = series[idx - 1]["close"] if idx > 0 else None
        gap = (cur["open"] / pc - 1) if cur["open"] and pc and pc > 0 else np.nan
        states: set[str] = set()
        if pc and pc > 0 and cur["close"]:
            if cur["close"] >= pc * _limit_ratio(ts) - 1e-6:
                states.add("S-limit")
        if gap == gap and gap > 0.03:
            states.add("S-gap")
        if idx < 250:
            states.add("S-fresh")
        day_fv[ts] = amp
        day_all[ts] = {"amp": amp, "turn": turn, "states": states}

    if len(day_fv) > 30:
        amp_sorted = sorted(v["amp"] for v in day_all.values() if v["amp"] == v["amp"])
        amp_q10 = float(np.percentile(amp_sorted, 10)) if amp_sorted else 0.0
        turn_vals = [v["turn"] for v in day_all.values() if v["turn"] == v["turn"]]
        turn_q30 = float(np.percentile(turn_vals, 30)) if turn_vals else 1.0
        for _ts, d in day_all.items():
            if (
                d["amp"] == d["amp"]
                and d["amp"] <= amp_q10
                and d["turn"] == d["turn"]
                and d["turn"] <= turn_q30
            ):
                d["states"].add("S-shrink")

    state_fv: dict[str, dict[str, float]] = defaultdict(dict)
    for ts, d in day_all.items():
        amp = day_fv.get(ts)
        if amp is None or not np.isfinite(amp):
            continue
        for st in d["states"]:
            if st in STATE_PARAMS:
                state_fv[st][ts] = amp

    tot = 0
    above = 0
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 20 or ts not in mv_map.get(day, {}):
            continue
        closes = [r["close"] for r in series[idx - 19: idx + 1] if r["close"]]
        if len(closes) < 20:
            continue
        tot += 1
        if series[idx]["close"] > sum(closes) / 20:
            above += 1
    breadth = (above / tot) if tot else 0.0
    return state_fv, breadth


def simulate_state_nav(
    state: str,
    cal: list[str],
    per_ts: dict[str, list[dict[str, Any]]],
    mv_map: dict[str, dict[str, float]],
    list_dates: dict[str, Any],
    date_idx: dict[str, dict[str, int]],
    *,
    window_start: str,
    skip_t1_limit: bool = True,
    limit_fallback: bool = False,
) -> list[float]:
    """Independent-slot replay for one state; returns daily NAV series aligned to ``cal``."""
    params = STATE_PARAMS[state]
    body = params["body"]
    bucket_q = params["bucket_q"]
    max_pos = params["max_pos"]
    idx_by_day = {d: i for i, d in enumerate(cal)}
    cal_set = set(cal)
    close_by_ts: dict[str, dict[str, float]] = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m:
            close_by_ts[ts] = m

    positions: dict[str, dict[str, Any]] = {}
    realized = 0.0
    nav_curve: list[float] = []

    for day in cal:
        if day < window_start:
            continue
        _, breadth = _day_state_fv(per_ts, mv_map, list_dates, day, date_idx)
        r_wide = breadth > R_WIDE_THRESHOLD
        to_close: list[str] = []
        for ts, p in list(positions.items()):
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= body:
                to_close.append(ts)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT

        if r_wide and day in idx_by_day and idx_by_day[day] > 0:
            prev_day = cal[idx_by_day[day] - 1]
            sig_fv, _ = _day_state_fv(per_ts, mv_map, list_dates, prev_day, date_idx)
            fmap = sig_fv.get(state, {})
            if fmap:
                ranked = sorted(fmap.items(), key=lambda kv: kv[1])
                qn = max(1, len(ranked) // bucket_q)
                pool = ranked if (skip_t1_limit and limit_fallback) else ranked[:qn]
                for ts, _fv in pool:
                    if ts in positions or len(positions) >= max_pos:
                        continue
                    if skip_t1_limit and _t1_limit_locked(per_ts, date_idx, prev_day, ts):
                        continue
                    series = per_ts.get(ts)
                    di = date_idx.get(ts, {}).get(day, -1)
                    open_px = series[di]["open"] if di >= 0 else None
                    if open_px and open_px > 0:
                        positions[ts] = {"entry_date": day, "entry_price": open_px}

        mtm = 0.0
        for ts, p in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += POSITION_PCT * (cc / p["entry_price"]) if cc and p["entry_price"] else POSITION_PCT
        nav_curve.append(1.0 + realized + (mtm - len(positions) * POSITION_PCT))

    last = cal[-1] if cal else window_start
    for ts, p in list(positions.items()):
        cc = close_by_ts.get(ts, {}).get(last)
        if cc and p["entry_price"]:
            realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
    if nav_curve:
        nav_curve[-1] = 1.0 + realized
    return nav_curve


def combo_sliced_nav(navs: list[list[float]], weights: list[float]) -> list[float]:
    """Blend daily returns with normalized weights."""
    if not navs:
        return [1.0]
    w_sum = sum(weights)
    ws = [w / w_sum for w in weights]
    rets = []
    for nav in navs:
        rets.append([nav[i] / nav[i - 1] - 1.0 for i in range(1, len(nav)) if nav[i - 1] > 0])
    n = min(len(r) for r in rets) if rets else 0
    out = [1.0]
    for i in range(n):
        dr = sum(w * r[i] for w, r in zip(ws, rets, strict=True))
        out.append(out[-1] * (1.0 + dr))
    return out


def nav_metrics(nav: list[float]) -> dict[str, float | int | None]:
    if not nav:
        return {"total_pct": 0.0, "max_dd": 0.0, "sharpe": None, "n_days": 0}
    peak = nav[0]
    max_dd = 0.0
    for v in nav:
        peak = max(peak, v)
        if peak > 0:
            max_dd = max(max_dd, (peak - v) / peak)
    rets = [nav[i] / nav[i - 1] - 1.0 for i in range(1, len(nav)) if nav[i - 1] > 0]
    sharpe: float | None = None
    if len(rets) >= 2:
        mean_r = float(np.mean(rets))
        std_r = float(np.std(rets, ddof=1))
        if std_r > 0:
            sharpe = round(mean_r / std_r * (252**0.5), 2)
    return {
        "total_pct": round((nav[-1] / nav[0] - 1.0) * 100.0, 2),
        "max_dd": round(max_dd * 100.0, 1),
        "sharpe": sharpe,
        "n_days": len(nav),
    }


def _warm_context(start: str, end: str) -> tuple[
    list[str],
    dict[str, list[dict[str, Any]]],
    dict[str, dict[str, float]],
    dict[str, Any],
    dict[str, dict[str, int]],
]:
    w_start = (date.fromisoformat(start) - timedelta(days=WARMUP_CAL_DAYS)).isoformat()
    per_ts = _load_rows(w_start, end)
    mv_map = _load_mv(w_start, end)
    list_dates = _load_list_dates()
    cal = _load_calendar(w_start, end)
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    return cal, per_ts, mv_map, list_dates, date_idx


def run_state_nav(
    state: str,
    *,
    start: str,
    end: str,
    skip_t1_limit: bool = True,
) -> list[float]:
    cal, per_ts, mv_map, list_dates, date_idx = _warm_context(start, end)
    return simulate_state_nav(
        state,
        cal,
        per_ts,
        mv_map,
        list_dates,
        date_idx,
        window_start=start,
        skip_t1_limit=skip_t1_limit,
    )


def run_slice_variant(
    variant: str,
    *,
    start: str,
    end: str,
    skip_t1_limit: bool = True,
) -> dict[str, Any]:
    """Run a named slice variant; returns metrics + per-state NAV if blended."""
    if variant not in SLICE_VARIANTS:
        raise ValueError(f"unknown variant {variant!r}; valid={list(SLICE_VARIANTS)}")
    states, weights = SLICE_VARIANTS[variant]
    cal, per_ts, mv_map, list_dates, date_idx = _warm_context(start, end)
    navs: list[list[float]] = []
    per_state: dict[str, dict[str, float | int | None]] = {}
    for st in states:
        nav = simulate_state_nav(
            st,
            cal,
            per_ts,
            mv_map,
            list_dates,
            date_idx,
            window_start=start,
            skip_t1_limit=skip_t1_limit,
        )
        navs.append(nav)
        per_state[st] = nav_metrics(nav)
    if len(states) == 1:
        blended = navs[0]
    else:
        blended = combo_sliced_nav(navs, list(weights))
    return {
        "variant": variant,
        "states": list(states),
        "weights": list(weights),
        "metrics": nav_metrics(blended),
        "per_state": per_state,
    }
