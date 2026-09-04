#!/usr/bin/env python3
"""R2 per-state Scout: within each state, long amplitude Q1 (low-vol tail), R-wide gate.

States from R1. Negative amplitude IC in hot states => low-amplitude tail outperforms.
Long-only: rank amplitude ascending within state, take Q1 (bottom 20%).
Hold 10, 10% x10, next_open. Breadth gate: fraction of A stocks close>MA20 > 0.5 (R-wide).
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection  # noqa: E402

WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"), "valid": ("2026-03-01", "2026-08-07")}
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
HOLD = 10
POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.003
STATES = ["S-limit", "S-gap", "S-shrink", "S-breakout", "S-fresh", "S-stress", "S-all"]


def _load_calendar(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (s, e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]


def _load_daily(s, e):
    s2 = max(date.fromisoformat(s) - timedelta(days=120), date(1998, 1, 1)).isoformat()
    e2 = (date.fromisoformat(e) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, pre_close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s2, e2))
            rows = cur.fetchall()
    per_ts = defaultdict(list)
    for d, ts, o, h, l, c, pc, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts[str(ts)].append({"date": ds, "open": float(o) if o is not None else None, "high": float(h) if h is not None else None, "low": float(l) if l is not None else None, "close": float(c) if c is not None else None, "pre_close": float(pc) if pc is not None else None, "amount": float(amt) if amt is not None else None})
    return per_ts


def _load_mv_map(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s, e))
            rows = cur.fetchall()
    out = defaultdict(dict)
    for d, ts, mv in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out[ds][str(ts)] = float(mv) / 10000.0
    return out


def _load_list_dates():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, list_date FROM stock_basic")
            return {str(r[0]): r[1] for r in cur.fetchall()}


def _compute_day_states_and_amp(per_ts, mv_map, list_dates, day):
    """Return (state -> ts -> amplitude), and universe breadth flag."""
    day_amp = {}; day_turn = {}; day_all = {}
    for ts, series in per_ts.items():
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
        if idx < 0 or idx < 20:
            continue
        mv = mv_map.get(day, {}).get(ts)
        if mv is None:
            continue
        cur = series[idx]
        if not cur["close"] or not cur["high"] or not cur["low"] or cur["close"] <= 0:
            continue
        pc = series[idx - 1]["close"] if idx > 0 else None
        ret1 = (cur["close"] / pc - 1) if pc and pc > 0 else np.nan
        gap = (cur["open"] / pc - 1) if cur["open"] and pc and pc > 0 else np.nan
        amp = (cur["high"] - cur["low"]) / cur["close"]
        amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        turn_spike = (cur["amount"] / avg20) if avg20 and avg20 > 0 else np.nan
        highs20 = [r["high"] for r in series[idx - 19: idx + 1] if r["high"]]
        new_high = (cur["close"] >= max(highs20)) if highs20 else False
        ld = list_dates.get(ts)
        fresh = False
        if ld:
            try:
                if idx < 250:
                    fresh = True
            except Exception:
                fresh = False
        day_amp[ts] = amp
        day_turn[ts] = turn_spike
        states = []
        if pc and pc > 0 and cur["close"]:
            lim = 1.095 if (ts.endswith((".SH", ".SZ")) and (ts.startswith("60") or ts.startswith("00"))) else 1.195
            if cur["close"] >= pc * lim - 1e-6:
                states.append("S-limit")
        if gap and gap > 0.03:
            states.append("S-gap")
        if new_high and turn_spike and turn_spike > 2:
            states.append("S-breakout")
        if fresh:
            states.append("S-fresh")
        day_all[ts] = {"amp": amp, "turn": turn_spike, "states": states}
    # cross-sectional shrink Q10 / turn Q30 and stress
    if len(day_amp) > 30:
        amp_sorted = sorted(day_amp.values())
        amp_q10 = np.percentile(amp_sorted, 10)
        amp_q70 = np.percentile(amp_sorted, 70)
        turn_q30 = np.percentile(sorted(day_turn.values()), 30)
        for ts, d in day_all.items():
            if d["amp"] <= amp_q10 and d["turn"] is not None and d["turn"] <= turn_q30:
                d["states"].append("S-shrink")
            if d["turn"] is not None and d["turn"] > 2 and d["amp"] > amp_q70:
                d["states"].append("S-stress")
        for ts in day_all:
            day_all[ts]["states"].append("S-all")
    # breadth: fraction close>MA20 (need MA20 per ts)
    above = 0; tot = 0
    for ts, series in per_ts.items():
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
        if idx < 20:
            continue
        if ts not in mv_map.get(day, {}):
            continue
        closes = [r["close"] for r in series[idx - 19: idx + 1] if r["close"]]
        if len(closes) < 20:
            continue
        ma20 = sum(closes) / 20
        tot += 1
        if series[idx]["close"] > ma20:
            above += 1
    breadth = (above / tot) if tot else 0.0
    # build state -> ts -> amp
    state_amp = defaultdict(dict)
    for ts, d in day_all.items():
        for st in d["states"]:
            state_amp[st][ts] = d["amp"]
    return state_amp, breadth


def simulate(wname, list_dates):
    s, e = WINDOWS[wname]
    per_ts = _load_daily(s, e)
    mv_map = _load_mv_map(s, e)
    cal = _load_calendar(s, e)
    date_to_idx = {d: i for i, d in enumerate(cal)}
    close_by_ts = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in set(cal) and r["close"]}
        if m:
            close_by_ts[ts] = m
    # state -> results
    nav_state = {st: [] for st in STATES}
    realized_state = {st: 0.0 for st in STATES}
    positions_state = {st: {} for st in STATES}

    for day in cal:
        state_amp, breadth = _compute_day_states_and_amp(per_ts, mv_map, list_dates, day)
        r_wide = breadth > 0.5
        for st in STATES:
            positions = positions_state[st]
            # close expired
            to_close = []
            for ts, pos in list(positions.items()):
                ei = date_to_idx.get(pos["entry_date"], -1); ci = date_to_idx.get(day, -1)
                held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
                if held >= HOLD:
                    to_close.append(ts)
            for ts in to_close:
                pos = positions.pop(ts)
                cur_close = close_by_ts.get(ts, {}).get(day)
                if cur_close and pos["entry_price"]:
                    net = (cur_close / pos["entry_price"] - 1) - COSTS_ROUNDTRIP
                    realized_state[st] += net * POSITION_PCT
            # open (only if R-wide)
            if r_wide and day in date_to_idx and date_to_idx[day] > 0:
                sig_day = cal[date_to_idx[day] - 1]
                sig_state_amp, _ = _compute_day_states_and_amp(per_ts, mv_map, list_dates, sig_day)
                fmap = sig_state_amp.get(st, {})
                if fmap and len(positions) < MAX_POSITIONS:
                    sorted_ts = sorted(fmap.items(), key=lambda kv: kv[1])  # ascending amp -> Q1 tail
                    q = max(1, len(sorted_ts) // 5)
                    q1 = [ts for ts, _ in sorted_ts[:q]]
                    cands = []
                    for ts in q1:
                        if ts in positions:
                            continue
                        series = per_ts.get(ts)
                        open_px = None
                        for r in series:
                            if r["date"] == day:
                                open_px = r["open"]
                                break
                        if not open_px or open_px <= 0:
                            continue
                        cands.append((ts, open_px))
                    slots = MAX_POSITIONS - len(positions)
                    for ts, open_px in cands[:slots]:
                        positions[ts] = {"entry_date": day, "entry_price": open_px}
            # nav
            mtm = 0.0
            for ts, pos in positions.items():
                cc = close_by_ts.get(ts, {}).get(day)
                if cc and pos["entry_price"]:
                    mtm += POSITION_PCT * (cc / pos["entry_price"])
                else:
                    mtm += POSITION_PCT
            nav = 1.0 + realized_state[st] + (mtm - len(positions) * POSITION_PCT)
            nav_state[st].append(nav)
    # close remaining
    last = cal[-1]
    for st in STATES:
        for ts, pos in list(positions_state[st].items()):
            cur_close = close_by_ts.get(ts, {}).get(last)
            if cur_close and pos["entry_price"]:
                realized_state[st] += ((cur_close / pos["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
    res = {}
    n_days = len(cal)
    for st in STATES:
        nav_end = nav_state[st][-1] if nav_state[st] else 1.0
        total_pct = realized_state[st] * 100
        curve = nav_state[st]
        peak = curve[0] if curve else 1
        max_dd = 0
        for v in curve:
            if v > peak:
                peak = v
            dd = (peak - v) / peak * 100 if peak else 0
            if dd > max_dd:
                max_dd = dd
        rets = [curve[i] / curve[i - 1] - 1 for i in range(1, len(curve)) if curve[i - 1] > 0]
        sharpe = float(np.mean(rets) / np.std(rets) * (252 ** 0.5)) if len(rets) > 10 and np.std(rets) > 0 else 0
        daily = total_pct / n_days if n_days else 0
        res[st] = {"total_pct": total_pct, "daily": daily, "max_dd": max_dd, "sharpe": sharpe, "nav_end": nav_end}
    return res


def main():
    list_dates = _load_list_dates()
    all_res = {}
    for w in WINDOWS:
        print(f"[{w}] simulate...", flush=True)
        all_res[w] = simulate(w, list_dates)
    out = REPORT_DIR / "state_scout_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"windows": list(WINDOWS.keys()), "states": STATES, "hold": HOLD, "results": all_res}, ensure_ascii=False, indent=2, default=str))
    print("\n| state | OOS2 total/daily | train total/daily | valid total/daily | valid dd | valid sharpe |")
    print("|-------|------------------|-------------------|-------------------|---------|-------------|")
    for st in STATES:
        o = all_res["OOS2"][st]; t = all_res["train"][st]; v = all_res["valid"][st]
        print(f"| {st:10s} | {o['total_pct']:+.1f}%/{o['daily']:+.4f} | {t['total_pct']:+.1f}%/{t['daily']:+.4f} | {v['total_pct']:+.1f}%/{v['daily']:+.4f} | {v['max_dd']:.1f} | {v['sharpe']:.2f} |")
    print(f"report -> {out}")


if __name__ == "__main__":
    raise SystemExit(main())
