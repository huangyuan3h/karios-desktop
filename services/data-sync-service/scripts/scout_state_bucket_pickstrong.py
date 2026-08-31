#!/usr/bin/env python3
"""R8: replace S-3 stock leg with State-Bucket in the pick-strong architecture.

Combined engine = State-Bucket (A-share alpha, replaces S-3) + third-asset sleeve
(gold/oil/nasdaq/bond, mom60 + MA200 gate + trail8 exit) + REPO fallback.
Two allocation structures tested:
  - satellite: State-Bucket is the core; idle cash (1 - invested frac) -> trail8 sleeve
  - argmax:    pick-strong style 100% switch over {STOCK=State-Bucket NAV, ETFs, REPO}
Run on 3 discipline windows (OOS2/train/valid) + long window + past_year.
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection  # noqa: E402

POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.003
STATE_HOLD = {"S-limit": 3, "S-gap": 3, "S-fresh": 15, "S-shrink": 15}
ETF_CODES = {"GOLD": "518880.SH", "OIL": "513350.SH", "NASDAQ": "513100.SH", "BOND": "511260.SH"}
LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}


def _load_calendar(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (s, e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]


def _load_daily(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, pre_close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s, e))
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


def _load_etf(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, close FROM daily WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (list(ETF_CODES.values()), s, e))
            rows = cur.fetchall()
    per = {c: [] for c in ETF_CODES.values()}
    for d, ts, c in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        if c is not None:
            per[str(ts)].append((ds, float(c)))
    for c in per:
        per[c].sort(key=lambda x: x[0])
    return per


def _day_state_fv(per_ts, mv_map, list_dates, cal, day, date_idx):
    day_fv = {}; day_all = {}
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


_STATE_FV_CACHE: dict = {}


def _day_state_fv_cached(per_ts, mv_map, list_dates, cal, day, date_idx):
    key = (cal[0], cal[-1], day)
    hit = _STATE_FV_CACHE.get(key)
    if hit is None:
        hit = _day_state_fv(per_ts, mv_map, list_dates, cal, day, date_idx)
        _STATE_FV_CACHE[key] = hit
    return hit


def simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx,
                          state_filter=None, counters=None,
                          bucket_q=5, max_pos=10, hold_map=None):
    date_to_idx = {d: i for i, d in enumerate(cal)}
    hold = {**STATE_HOLD, **(hold_map or {})}
    close_by_ts = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in set(cal) and r["close"]}
        if m:
            close_by_ts[ts] = m
    positions = {}
    realized = 0.0
    nav_curve = []
    frac_start = []
    for day in cal:
        frac_start.append(min(1.0, len(positions) * POSITION_PCT))
        state_fv, breadth = _day_state_fv_cached(per_ts, mv_map, list_dates, cal, day, date_idx)
        r_wide = breadth > 0.5
        to_close = []
        for ts, p in list(positions.items()):
            ei = date_to_idx.get(p["entry_date"], -1); ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= hold[p["state"]]:
                to_close.append(ts)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
        if r_wide and day in date_to_idx and date_to_idx[day] > 0:
            sig_fv, _ = _day_state_fv_cached(per_ts, mv_map, list_dates, cal, cal[date_to_idx[day] - 1], date_idx)
            cands = []
            for st in (state_filter or STATE_HOLD):
                fmap = sig_fv.get(st, {})
                if not fmap:
                    continue
                ranked = sorted(fmap.items(), key=lambda kv: kv[1])
                qn = max(1, len(ranked) // bucket_q)
                for ts, fv in ranked[:qn]:
                    if ts in positions:
                        continue
                    series = per_ts.get(ts)
                    di = date_idx.get(ts, {}).get(day, -1)
                    open_px = series[di]["open"] if di >= 0 else None
                    if open_px and open_px > 0:
                        cands.append((st, ts, open_px))
                        if counters is not None:
                            counters["cands"][st] = counters["cands"].get(st, 0) + 1
            cands.sort(key=lambda x: {"S-limit": 4, "S-gap": 3, "S-fresh": 2, "S-shrink": 1}[x[0]], reverse=True)
            for st, ts, open_px in cands:
                if ts in positions:
                    continue
                if len(positions) < max_pos:
                    positions[ts] = {"entry_date": day, "entry_price": open_px, "state": st}
                    if counters is not None:
                        counters["fills"][st] = counters["fills"].get(st, 0) + 1
                elif counters is not None:
                    counters["blocked"][st] = counters["blocked"].get(st, 0) + 1
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
    nav_curve[-1] = 1.0 + realized
    return nav_curve, frac_start


def sleeve_ret_trail8(cal, per_etf, pos_map):
    ret = {}
    held = None; peak = 0.0
    for i, day in enumerate(cal):
        if i == 0:
            ret[day] = 0.0
            continue
        prev = cal[i - 1]
        if held is not None:
            ip = pos_map[held].get(prev); ic = pos_map[held].get(day)
            if ip is None or ic is None:
                held = None; peak = 0.0; ret[day] = 0.0
                continue
            c_now = per_etf[held][ic][1]
            if c_now < peak * 0.92:
                held = None; peak = 0.0; ret[day] = 0.0
                continue
            peak = max(peak, c_now)
            ret[day] = c_now / per_etf[held][ip][1] - 1
            continue
        best = None; bestmom = -1e9
        for ts, series in per_etf.items():
            ip = pos_map[ts].get(prev); ic = pos_map[ts].get(day)
            if ip is None or ic is None or ip < 200:
                continue
            closes = [c for _, c in series[max(0, ip - 200): ip + 1]]
            if len(closes) < 200:
                continue
            ma200 = sum(closes[-200:]) / 200.0
            cp = series[ip][1]
            if cp < ma200:
                continue
            if ip < 60:
                continue
            mom = cp / series[ip - 60][1] - 1
            if mom > bestmom:
                bestmom = mom; best = ts
        if best is None:
            ret[day] = 0.0
        else:
            ip = pos_map[best][prev]; ic = pos_map[best][day]
            held = best; peak = per_etf[best][ic][1]
            ret[day] = per_etf[best][ic][1] / per_etf[best][ip][1] - 1
    return ret


def combine_satellite(cal, nav_sb, frac_start, ret_sleeve):
    nav = [1.0]
    for i in range(1, len(cal)):
        sb_pnl = nav_sb[i] - nav_sb[i - 1]
        cash_slice = 1 - frac_start[i]
        nav.append(nav[i - 1] + sb_pnl + cash_slice * nav[i - 1] * ret_sleeve[cal[i]])
    return nav


def combine_argmax(cal, nav_sb, per_etf, pos_map):
    return combine_argmax_momlb(cal, nav_sb, per_etf, pos_map, 60)


def combine_argmax_momlb(cal, nav_sb, per_etf, pos_map, lb):
    nav = [1.0]
    for i in range(1, len(cal)):
        prev = cal[i - 1]; day = cal[i]
        if i >= lb:
            sb_mom = nav_sb[i - 1] / nav_sb[i - 1 - lb] - 1
        else:
            sb_mom = nav_sb[i - 1] / nav_sb[0] - 1
        best = None; bestmom = -1e9
        if sb_mom is not None and sb_mom > bestmom:
            bestmom = sb_mom; best = "STOCK"
        for ts, series in per_etf.items():
            ip = pos_map[ts].get(prev); ic = pos_map[ts].get(day)
            if ip is None or ic is None or ip < 200:
                continue
            closes = [c for _, c in series[max(0, ip - 200): ip + 1]]
            if len(closes) < 200:
                continue
            ma200 = sum(closes[-200:]) / 200.0
            cp = series[ip][1]
            if cp < ma200:
                continue
            if ip < 60:
                continue
            mom = cp / series[ip - 60][1] - 1
            if mom > bestmom:
                bestmom = mom; best = ts
        if best is None:
            r = 0.0
        elif best == "STOCK":
            r = nav_sb[i] / nav_sb[i - 1] - 1
        else:
            ip = pos_map[best][prev]; ic = pos_map[best][day]
            r = per_etf[best][ic][1] / per_etf[best][ip][1] - 1
        nav.append(nav[-1] * (1 + r))
    return nav


def stats(cal, nav):
    n = len(nav)
    total = (nav[-1] / nav[0] - 1) * 100
    cagr = ((nav[-1] / nav[0]) ** (252 / n) - 1) * 100 if n else 0
    peak = nav[0]; mdd = 0
    for v in nav:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > mdd:
            mdd = dd
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = float(np.mean(rets) / np.std(rets) * (252 ** 0.5)) if len(rets) > 10 and np.std(rets) > 0 else 0
    return {"n_days": n, "total_pct": total, "cagr": cagr, "max_dd": mdd, "sharpe": sharpe}


def main():
    print("loading all data (once)...")
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    per_etf = _load_etf(LOAD_S, LOAD_E)
    pos_map = {ts: {d: i for i, (d, _) in enumerate(series)} for ts, series in per_etf.items()}
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    out = {}
    for wname, (s, e) in WINDOWS.items():
        cal = _load_calendar(s, e)
        nav_sb, frac_start = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx)
        ret_sl = sleeve_ret_trail8(cal, per_etf, pos_map)
        nav_sat = combine_satellite(cal, nav_sb, frac_start, ret_sl)
        nav_arg = combine_argmax(cal, nav_sb, per_etf, pos_map)
        arg_variants = {}
        for lb in (60, 120, 250):
            nv = combine_argmax_momlb(cal, nav_sb, per_etf, pos_map, lb)
            arg_variants[lb] = stats(cal, nv)
        sb = stats(cal, nav_sb)
        sat = stats(cal, nav_sat)
        arg = stats(cal, nav_arg)
        out[wname] = {"state_bucket": sb, "combined_satellite_trail8": sat, "combined_argmax_lb60": arg, "combined_argmax_smoothed": {str(k): v for k, v in arg_variants.items()}}
        print(f"\n=== {wname} ({s}~{e}, {len(cal)}d) ===")
        print(f"  状态分桶(替S-3) : CAGR {sb['cagr']:+.1f}%  dd {sb['max_dd']:.1f}  sharpe {sb['sharpe']:.2f}  total {sb['total_pct']:+.1f}%")
        print(f"  联合-卫星+trail8 : CAGR {sat['cagr']:+.1f}%  dd {sat['max_dd']:.1f}  sharpe {sat['sharpe']:.2f}  total {sat['total_pct']:+.1f}%")
        print(f"  联合-argmax lb60 : CAGR {arg['cagr']:+.1f}%  dd {arg['max_dd']:.1f}  sharpe {arg['sharpe']:.2f}  total {arg['total_pct']:+.1f}%")
        for lb in (120, 250):
            v = arg_variants[lb]
            print(f"  联合-argmax lb{lb}: CAGR {v['cagr']:+.1f}%  dd {v['max_dd']:.1f}  sharpe {v['sharpe']:.2f}  total {v['total_pct']:+.1f}%")
    p = Path("data/backtest_reports/state_bucket_pickstrong_latest.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())
