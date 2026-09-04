#!/usr/bin/env python3
"""R3 state_regime_or: union S-limit + S-gap low-vol-tail signals, R-wide gated, vs frozen baseline.

Signal: within state S-limit OR S-gap, take amplitude Q1 (low-vol tail). R-wide (breadth>0.5) gate.
Union of both states' Q1 picks. Compare to frozen baseline valid +0.100%/day.
Also report S-limit-only, S-gap-only, and union, to see if or-synthesis adds or dilutes.
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
WIN_STATES = ["S-limit", "S-gap"]


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


def _compute_day(per_ts, mv_map, list_dates, day):
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
        fresh = (idx < 250) if ld else False
        day_amp[ts] = amp; day_turn[ts] = turn_spike
        states = set()
        if pc and pc > 0 and cur["close"]:
            lim = 1.095 if (ts.endswith((".SH", ".SZ")) and (ts.startswith("60") or ts.startswith("00"))) else 1.195
            if cur["close"] >= pc * lim - 1e-6:
                states.add("S-limit")
        if gap and gap > 0.03:
            states.add("S-gap")
        if new_high and turn_spike and turn_spike > 2:
            states.add("S-breakout")
        if fresh:
            states.add("S-fresh")
        day_all[ts] = {"amp": amp, "turn": turn_spike, "states": states}
    if len(day_amp) > 30:
        amp_sorted = sorted(day_amp.values())
        amp_q10 = np.percentile(amp_sorted, 10)
        amp_q70 = np.percentile(amp_sorted, 70)
        turn_q30 = np.percentile(sorted(day_turn.values()), 30)
        for ts, d in day_all.items():
            if d["amp"] <= amp_q10 and d["turn"] is not None and d["turn"] <= turn_q30:
                d["states"].add("S-shrink")
            if d["turn"] is not None and d["turn"] > 2 and d["amp"] > amp_q70:
                d["states"].add("S-stress")
        for ts in day_all:
            day_all[ts]["states"].add("S-all")
    breadth_above = 0; breadth_tot = 0
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
        breadth_tot += 1
        if series[idx]["close"] > sum(closes) / 20:
            breadth_above += 1
    breadth = (breadth_above / breadth_tot) if breadth_tot else 0.0
    # per-state amplitude map for WIN_STATES
    state_amp = {st: {} for st in WIN_STATES}
    for ts, d in day_all.items():
        for st in WIN_STATES:
            if st in d["states"]:
                state_amp[st][ts] = d["amp"]
    return state_amp, breadth


def simulate(wname, list_dates, union):
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
    positions = {}; realized = 0.0; nav_curve = []
    for day in cal:
        state_amp, breadth = _compute_day(per_ts, mv_map, list_dates, day)
        r_wide = breadth > 0.5
        # close
        to_close = []
        for ts, pos in list(positions.items()):
            ei = date_to_idx.get(pos["entry_date"], -1); ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= HOLD:
                to_close.append(ts)
        for ts in to_close:
            pos = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and pos["entry_price"]:
                realized += ((cc / pos["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
        # open (R-wide only)
        if r_wide and day in date_to_idx and date_to_idx[day] > 0:
            sig_state_amp, _ = _compute_day(per_ts, mv_map, list_dates, cal[date_to_idx[day] - 1])
            # union candidate pool: Q1 in each winning state, union
            cand = []
            for st in WIN_STATES:
                fmap = sig_state_amp.get(st, {})
                if not fmap:
                    continue
                q1 = sorted(fmap.items(), key=lambda kv: kv[1])[: max(1, len(fmap) // 5)]
                for ts, amp in q1:
                    cand.append((ts, amp))
            # dedup keep lowest amp
            seen = {}
            for ts, amp in cand:
                if ts not in seen or amp < seen[ts]:
                    seen[ts] = amp
            if union and len(positions) < MAX_POSITIONS:
                slots = MAX_POSITIONS - len(positions)
                for ts, amp in sorted(seen.items(), key=lambda kv: kv[1])[:slots]:
                    if ts in positions:
                        continue
                    series = per_ts.get(ts)
                    open_px = None
                    for r in series:
                        if r["date"] == day:
                            open_px = r["open"]
                            break
                    if open_px and open_px > 0:
                        positions[ts] = {"entry_date": day, "entry_price": open_px}
        mtm = 0.0
        for ts, pos in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += POSITION_PCT * (cc / pos["entry_price"]) if cc and pos["entry_price"] else POSITION_PCT
        nav_curve.append(1.0 + realized + (mtm - len(positions) * POSITION_PCT))
    last = cal[-1]
    for ts, pos in list(positions.items()):
        cc = close_by_ts.get(ts, {}).get(last)
        if cc and pos["entry_price"]:
            realized += ((cc / pos["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
    total_pct = realized * 100
    peak = nav_curve[0]; max_dd = 0
    for v in nav_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > max_dd:
            max_dd = dd
    rets = [nav_curve[i] / nav_curve[i - 1] - 1 for i in range(1, len(nav_curve)) if nav_curve[i - 1] > 0]
    sharpe = float(np.mean(rets) / np.std(rets) * (252 ** 0.5)) if len(rets) > 10 and np.std(rets) > 0 else 0
    return {"total_pct": total_pct, "daily": total_pct / len(cal) if cal else 0, "max_dd": max_dd, "sharpe": sharpe, "nav_end": nav_curve[-1] if nav_curve else 1}


def main():
    list_dates = _load_list_dates()
    # union only (S-limit U S-gap). Also single-state for comparison via separate simulate with one state.
    # Quick: re-run union; compare to frozen baseline valid +0.100%/day
    all_res = {}
    for w in WINDOWS:
        print(f"[{w}] union simulate...", flush=True)
        all_res[w] = simulate(w, list_dates, union=True)
    out = REPORT_DIR / "state_or_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"mode": "S-limit U S-gap Q1, R-wide gate", "windows": list(WINDOWS.keys()), "hold": HOLD, "results": all_res}, ensure_ascii=False, indent=2, default=str))
    print("\n| window | union total/daily | dd | sharpe |")
    print("|--------|-------------------|----|--------|")
    for w in WINDOWS:
        d = all_res[w]
        print(f"| {w:6s} | {d['total_pct']:+.1f}%/{d['daily']:+.4f} | {d['max_dd']:.1f} | {d['sharpe']:.2f} |")
    print("frozen baseline valid: +11.3% / +0.100%/day (20-150 amp_q10 10d breadth>0.5)")
    print(f"report -> {out}")


if __name__ == "__main__":
    raise SystemExit(main())
