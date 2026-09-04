#!/usr/bin/env python3
"""Combine State-Bucket (A-share alpha) with third-asset sleeve (gold/oil/nasdaq/bond mom60 + REPO).

State-bucket invests its slice (10% x N, up to 100% of init capital); idle cash (1 - invested
fraction) is deployed into the third-asset sleeve decided at t-1 close. Mirrors pick-strong's
REPO fallback but upgraded to an active multi-asset sleeve. Tests whether "state-bucket + 第三类资产择强"
improves return/dd vs state-bucket alone.
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


def _load_etf(s, e):
    s2 = max(date.fromisoformat(s) - timedelta(days=400), date(1998, 1, 1)).isoformat()
    e2 = (date.fromisoformat(e) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, close FROM daily WHERE ts_code = ANY(%s) AND trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (list(ETF_CODES.values()), s2, e2))
            rows = cur.fetchall()
    per = {c: [] for c in ETF_CODES.values()}
    for d, ts, c in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        if c is not None:
            per[str(ts)].append((ds, float(c)))
    for c in per:
        per[c].sort(key=lambda x: x[0])
    return per


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


def simulate_state_bucket(wname, s, e, list_dates):
    per_ts = _load_daily(s, e)
    mv_map = _load_mv_map(s, e)
    cal = _load_calendar(s, e)
    date_to_idx = {d: i for i, d in enumerate(cal)}
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
        state_fv, breadth = _day_state_fv(per_ts, mv_map, list_dates, cal, day)
        r_wide = breadth > 0.5
        to_close = []
        for ts, p in list(positions.items()):
            ei = date_to_idx.get(p["entry_date"], -1); ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= STATE_HOLD[p["state"]]:
                to_close.append(ts)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
        if r_wide and day in date_to_idx and date_to_idx[day] > 0:
            sig_fv, _ = _day_state_fv(per_ts, mv_map, list_dates, cal, cal[date_to_idx[day] - 1])
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
            cands.sort(key=lambda x: {"S-limit": 4, "S-gap": 3, "S-fresh": 2, "S-shrink": 1}[x[0]], reverse=True)
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
    nav_curve[-1] = 1.0 + realized
    return cal, nav_curve, frac_start


def sleeve_ret_series(cal, per_etf):
    pos_map = {ts: {d: i for i, (d, _) in enumerate(series)} for ts, series in per_etf.items()}
    ret = {}
    for i, day in enumerate(cal):
        if i == 0:
            ret[day] = 0.0
            continue
        prev = cal[i - 1]
        best_ts = None
        best_mom = -1e9
        for ts, series in per_etf.items():
            ip = pos_map[ts].get(prev)
            ic = pos_map[ts].get(day)
            if ip is None or ic is None or ip < 200:
                continue
            closes = [c for _, c in series[max(0, ip - 200): ip + 1]]
            if len(closes) < 200:
                continue
            ma200 = sum(closes[-200:]) / 200.0
            close_prev = series[ip][1]
            if close_prev < ma200:
                continue
            if ip < 60:
                continue
            mom60 = close_prev / series[ip - 60][1] - 1.0
            if mom60 > best_mom:
                best_mom = mom60
                best_ts = ts
        if best_ts is None:
            ret[day] = 0.0  # REPO
        else:
            ip = pos_map[best_ts][prev]
            ic = pos_map[best_ts][day]
            ret[day] = series[ic][1] / series[ip][1] - 1.0
    return ret


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


def combine(cal, nav_sb, frac_start, ret_sleeve):
    nav = [1.0]
    for i in range(1, len(cal)):
        sb_pnl = nav_sb[i] - nav_sb[i - 1]
        cash_slice = 1 - frac_start[i]
        sleeve_pnl = cash_slice * nav[i - 1] * ret_sleeve[cal[i]]
        nav.append(nav[i - 1] + sb_pnl + sleeve_pnl)
    return nav


def combine_argmax(cal, nav_sb, per_etf, pos_map):
    nav = [1.0]
    for i in range(1, len(cal)):
        prev = cal[i - 1]; day = cal[i]
        sb_mom = nav_sb[i - 1] / nav_sb[i - 61] - 1 if i >= 61 else None
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
            r = series[ic][1] / series[ip][1] - 1
        nav.append(nav[-1] * (1 + r))
    return nav


def sleeve_ret_series_trail8(cal, per_etf, pos_map):
    ret = {}
    held = None
    peak = 0.0
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
            c_prev = per_etf[held][ip][1]; c_now = per_etf[held][ic][1]
            if c_now < peak * 0.92:  # trail8 -> REPO
                held = None; peak = 0.0; ret[day] = 0.0
                continue
            peak = max(peak, c_now)
            ret[day] = c_now / c_prev - 1
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
            c_prev = per_etf[best][ip][1]; c_now = per_etf[best][ic][1]
            held = best; peak = c_now
            ret[day] = c_now / c_prev - 1
    return ret


def run_window(wname, s, e, list_dates):
    per_etf = _load_etf(s, e)
    cal, nav_sb, frac_start = simulate_state_bucket(wname, s, e, list_dates)
    ret_sleeve = sleeve_ret_series(cal, per_etf)
    nav_comb = combine(cal, nav_sb, frac_start, ret_sleeve)
    pos_map = {ts: {d: i for i, (d, _) in enumerate(series)} for ts, series in per_etf.items()}
    nav_argmax = combine_argmax(cal, nav_sb, per_etf, pos_map)
    ret_trail8 = sleeve_ret_series_trail8(cal, per_etf, pos_map)
    nav_comb_t8 = combine(cal, nav_sb, frac_start, ret_trail8)
    nav_sl_t8 = [1.0]
    for i in range(1, len(cal)):
        nav_sl_t8.append(nav_sl_t8[-1] * (1 + ret_trail8[cal[i]]))
    sb = stats(cal, nav_sb)
    sl = stats(cal, [1.0] + [1.0 * (1 + ret_sleeve[cal[i]]) for i in range(1, len(cal))])
    sl_t8 = stats(cal, nav_sl_t8)
    cb = stats(cal, nav_comb)
    cb_t8 = stats(cal, nav_comb_t8)
    ca = stats(cal, nav_argmax)
    return {"window": wname, "state_bucket": sb, "sleeve_alone": sl, "sleeve_alone_trail8": sl_t8, "combined_satellite": cb, "combined_satellite_trail8": cb_t8, "combined_argmax": ca}


def main():
    list_dates = _load_list_dates()
    windows = {
        "past_year": ("2025-08-01", "2026-08-07"),
        "valid": ("2026-03-01", "2026-08-07"),
    }
    out = {}
    for wname, (s, e) in windows.items():
        out[wname] = run_window(wname, s, e, list_dates)
        r = out[wname]
        print(f"\n=== {wname} ===")
        print(f"  状态分桶 : total {r['state_bucket']['total_pct']:+.1f}%  CAGR {r['state_bucket']['cagr']:+.1f}%  dd {r['state_bucket']['max_dd']:.1f}  sharpe {r['state_bucket']['sharpe']:.2f}")
        print(f"  第三类袖(无trail): total {r['sleeve_alone']['total_pct']:+.1f}%  CAGR {r['sleeve_alone']['cagr']:+.1f}%  dd {r['sleeve_alone']['max_dd']:.1f}  sharpe {r['sleeve_alone']['sharpe']:.2f}")
        print(f"  第三类袖(+trail8): total {r['sleeve_alone_trail8']['total_pct']:+.1f}%  CAGR {r['sleeve_alone_trail8']['cagr']:+.1f}%  dd {r['sleeve_alone_trail8']['max_dd']:.1f}  sharpe {r['sleeve_alone_trail8']['sharpe']:.2f}")
        print(f"  联合-卫星(无trail): total {r['combined_satellite']['total_pct']:+.1f}%  CAGR {r['combined_satellite']['cagr']:+.1f}%  dd {r['combined_satellite']['max_dd']:.1f}  sharpe {r['combined_satellite']['sharpe']:.2f}")
        print(f"  联合-卫星(+trail8): total {r['combined_satellite_trail8']['total_pct']:+.1f}%  CAGR {r['combined_satellite_trail8']['cagr']:+.1f}%  dd {r['combined_satellite_trail8']['max_dd']:.1f}  sharpe {r['combined_satellite_trail8']['sharpe']:.2f}")
        print(f"  联合-argmax(忠实择强): total {r['combined_argmax']['total_pct']:+.1f}%  CAGR {r['combined_argmax']['cagr']:+.1f}%  dd {r['combined_argmax']['max_dd']:.1f}  sharpe {r['combined_argmax']['sharpe']:.2f}")
    p = Path("data/backtest_reports/state_bucket_sleeve_latest.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())
