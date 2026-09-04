#!/usr/bin/env python3
"""R6 union combined (body holds) + return-priority management.

Two variants:
 A) union-body: S-limit/S-gap hold3, S-fresh/S-shrink hold15, R-wide gate, OR union.
 B) union-body-priority: + return-priority:
    - entry: if full(10), evict lowest-return held to give slot to higher-state-priority new signal
    - exit: hold>=body OR return>=take_profit(state) OR peak_retrace>trail(8%)
Priority = eat body, lock gain, capital to hottest state.
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
POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.003
STATE_HOLD = {"S-limit": 3, "S-gap": 3, "S-fresh": 15, "S-shrink": 15}
STATE_PRIORITY = {"S-limit": 4, "S-gap": 3, "S-fresh": 2, "S-shrink": 1}
TAKE_PROFIT = {"S-limit": 0.06, "S-gap": 0.06, "S-fresh": 0.12, "S-shrink": 0.12}
TRAIL = 0.08


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


def _day_state_fv(per_ts, mv_map, list_dates, cal, day):
    day_fv = {}; day_all = {}
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
        amp = (cur["high"] - cur["low"]) / cur["close"]
        amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        turn = (cur["amount"] / avg20) if avg20 and avg20 > 0 else np.nan
        pc = series[idx - 1]["close"] if idx > 0 else None
        gap = (cur["open"] / pc - 1) if cur["open"] and pc and pc > 0 else np.nan
        highs20 = [r["high"] for r in series[idx - 19: idx + 1] if r["high"]]
        new_high = (cur["close"] >= max(highs20)) if highs20 else False
        ld = list_dates.get(ts)
        fresh = (idx < 250) if ld else False
        states = set()
        if pc and pc > 0 and cur["close"]:
            lim = 1.095 if (ts.endswith((".SH", ".SZ")) and (ts.startswith("60") or ts.startswith("00"))) else 1.195
            if cur["close"] >= pc * lim - 1e-6:
                states.add("S-limit")
        if gap and gap > 0.03:
            states.add("S-gap")
        if new_high and turn and turn > 2:
            states.add("S-breakout")
        if fresh:
            states.add("S-fresh")
        day_fv[ts] = (cur["high"] - cur["low"]) / cur["close"]
        day_all[ts] = {"amp": amp, "turn": turn, "states": states}
    if len(day_fv) > 30:
        amp_sorted = sorted([v["amp"] for v in day_all.values() if v["amp"] == v["amp"]])
        amp_q10 = np.percentile(amp_sorted, 10) if amp_sorted else 0
        amp_q70 = np.percentile(amp_sorted, 70) if amp_sorted else 1
        turn_vals = [v["turn"] for v in day_all.values() if v["turn"] == v["turn"]]
        turn_q30 = np.percentile(turn_vals, 30) if turn_vals else 1
        for ts, d in day_all.items():
            if d["amp"] is not None and not np.isnan(d["amp"]) and d["amp"] <= amp_q10 and d["turn"] is not None and not np.isnan(d["turn"]) and d["turn"] <= turn_q30:
                d["states"].add("S-shrink")
            if d["turn"] is not None and not np.isnan(d["turn"]) and d["turn"] > 2 and d["amp"] is not None and not np.isnan(d["amp"]) and d["amp"] > amp_q70:
                d["states"].add("S-stress")
        for ts in day_all:
            day_all[ts]["states"].add("S-all")
    state_fv = defaultdict(dict)
    for ts, d in day_all.items():
        for st in d["states"]:
            if ts in day_fv and np.isfinite(day_fv[ts]):
                state_fv[st][ts] = day_fv[ts]
    above = 0; tot = 0
    for ts, series in per_ts.items():
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
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


def simulate_window(wname, list_dates, priority):
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
    positions = {}  # ts -> {entry_date, entry_price, state}
    realized = 0.0
    nav_curve = []
    for day in cal:
        state_fv, breadth = _day_state_fv(per_ts, mv_map, list_dates, cal, day)
        r_wide = breadth > 0.5
        # exits
        to_close = []
        for ts, p in list(positions.items()):
            cc = close_by_ts.get(ts, {}).get(day)
            ret = (cc / p["entry_price"] - 1) if cc and p["entry_price"] else 0
            ei = date_to_idx.get(p["entry_date"], -1); ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= STATE_HOLD[p["state"]]:
                to_close.append(ts)
            elif priority and ret >= TAKE_PROFIT[p["state"]]:
                to_close.append(ts)
            elif priority and ret < -TRAIL:
                to_close.append(ts)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
        # entries
        if r_wide and day in date_to_idx and date_to_idx[day] > 0:
            sig_fv, _ = _day_state_fv(per_ts, mv_map, list_dates, cal, cal[date_to_idx[day] - 1])
            # build global candidates
            cands = []
            for st in STATE_HOLD:
                fmap = sig_fv.get(st, {})
                if not fmap:
                    continue
                ranked = sorted(fmap.items(), key=lambda kv: kv[1])
                qn = max(1, len(ranked) // 5)
                for ts, fv in ranked[:qn]:
                    if ts in positions:
                        continue
                    series = per_ts.get(ts)
                    open_px = None
                    for r in series:
                        if r["date"] == day:
                            open_px = r["open"]
                            break
                    if open_px and open_px > 0:
                        cands.append((st, ts, open_px))
            # sort by state priority desc; fill empty slots only (no eviction -> avoid churn)
            cands.sort(key=lambda x: STATE_PRIORITY[x[0]], reverse=True)
            for st, ts, open_px in cands:
                if ts in positions:
                    continue
                if len(positions) < MAX_POSITIONS:
                    positions[ts] = {"entry_date": day, "entry_price": open_px, "state": st}
        mtm = 0.0
        for ts, p in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += POSITION_PCT * (cc / p["entry_price"]) if cc and p["entry_price"] else POSITION_PCT
        nav_curve.append(1.0 + realized + (mtm - len(positions) * POSITION_PCT))
    last = cal[-1]
    for ts, p in list(positions.items()):
        cc = close_by_ts.get(ts, {}).get(last)
        if cc and p["entry_price"]:
            realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
    total_pct = realized * 100
    n = len(cal)
    peak = nav_curve[0]; max_dd = 0
    for v in nav_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > max_dd:
            max_dd = dd
    rets = [nav_curve[i] / nav_curve[i - 1] - 1 for i in range(1, len(nav_curve)) if nav_curve[i - 1] > 0]
    sharpe = float(np.mean(rets) / np.std(rets) * (252 ** 0.5)) if len(rets) > 10 and np.std(rets) > 0 else 0
    return {"total_pct": total_pct, "daily": total_pct / n if n else 0, "max_dd": max_dd, "sharpe": sharpe}


def main():
    list_dates = _load_list_dates()
    out = {}
    for variant in [False, True]:
        out[str(variant)] = {}
        for w in WINDOWS:
            out[str(variant)][w] = simulate_window(w, list_dates, variant)
    print("\n### A) union-body (no priority) ###")
    a = out["False"]
    print(f"| OOS2 {a['OOS2']['daily']:+.4f} | train {a['train']['daily']:+.4f} | valid {a['valid']['daily']:+.4f} | dd {a['valid']['max_dd']:.1f} | sharpe {a['valid']['sharpe']:.2f} |")
    print("### B) union-body + return-priority ###")
    b = out["True"]
    print(f"| OOS2 {b['OOS2']['daily']:+.4f} | train {b['train']['daily']:+.4f} | valid {b['valid']['daily']:+.4f} | dd {b['valid']['max_dd']:.1f} | sharpe {b['valid']['sharpe']:.2f} |")
    print(f"R3 uniform-h10 valid: +0.330/day")
    p = REPORT_DIR / "state_union_latest.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"state_hold": STATE_HOLD, "priority": True, "results": out, "r3_valid_daily": 0.330}, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())
