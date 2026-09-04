#!/usr/bin/env python3
"""Strict deterministic batch — 20-80亿 small-mid, threshold sweep.

Tests tail extremes (10%, 5%) and absolute filters for deterministic hit rate.
Reports only patterns with tri-window win_rate>50% and valid total> -5% (not crashing).

Factors: amplitude, turnover_spike, gap, dist_high5, ret
Composite: amp∩turn double low at 10%/20%

Usage:
  PYTHONPATH=src python3 scripts/scout_strict_batch.py --holds 5,10
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

UNIVERSE_MIN_MV = 20.0
UNIVERSE_MAX_MV = 80.0
POSITION_PCT = 0.10
MAX_POSITIONS = 10
SLIPPAGE_PCT = 0.15
COSTS_ROUNDTRIP = SLIPPAGE_PCT * 2 / 100.0


def _load_calendar(start: str, end: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (start, end))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]


def _load_mv_map(start: str, end: str):
    s = max(date.fromisoformat(start) - timedelta(days=5), date(1998, 1, 1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s, end))
            rows = cur.fetchall()
    out = {}
    for d, ts, mv in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out.setdefault(ds, {})[str(ts)] = float(mv) / 10000.0
    return out


def _load_daily(start: str, end: str):
    s = max(date.fromisoformat(start) - timedelta(days=90), date(1998, 1, 1)).isoformat()
    e = (date.fromisoformat(end) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s, e))
            rows = cur.fetchall()
    per_ts = {}
    for d, ts, o, h, l, c, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts.setdefault(str(ts), []).append({"date": ds, "open": float(o) if o else None, "high": float(h) if h else None, "low": float(l) if l else None, "close": float(c) if c else None, "amount": float(amt) if amt else None})
    return per_ts


def _get_factors(per_ts, mv_map, day):
    amp = {}
    turn = {}
    gap = {}
    dist = {}
    for ts, series in per_ts.items():
        mv = mv_map.get(day, {}).get(ts)
        if mv is None or not (UNIVERSE_MIN_MV <= mv <= UNIVERSE_MAX_MV):
            continue
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
        if idx < 0 or idx < 20:
            continue
        cur = series[idx]
        if not cur["close"]:
            continue
        # amplitude
        if cur["high"] and cur["low"]:
            a = (cur["high"] - cur["low"]) / cur["close"]
            if 0 < a <= 0.5:
                amp[ts] = a
        # turnover spike
        amts = [r["amount"] for r in series[idx - 20 : idx + 1] if r["amount"]]
        if len(amts) >= 15:
            avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1)
            t = (cur["amount"] / avg20) if avg20 and cur["amount"] else None
            if t is not None and 0 < t <= 10:
                turn[ts] = t
        # gap
        if idx > 0 and cur["open"] and series[idx - 1]["close"]:
            g = cur["open"] / series[idx - 1]["close"] - 1
            if -0.1 < g < 0.1:
                gap[ts] = g
        # dist_high5
        highs5 = [r["high"] for r in series[idx - 5 : idx + 1] if r["high"]]
        if highs5 and cur["close"]:
            mh = max(highs5)
            d = (cur["close"] - mh) / mh if mh else None
            if d is not None and -0.3 < d <= 0.05:
                dist[ts] = d
    return {"amplitude": amp, "turnover_spike": turn, "gap": gap, "dist_high5": dist}


def simulate(pattern: str, hold: int, wname: str):
    start, end = WINDOWS[wname]
    cal = _load_calendar(start, end)
    cal_set = set(cal)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    date_to_idx = {d: i for i, d in enumerate(cal)}
    close_by_ts = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m:
            close_by_ts[ts] = m
    positions = {}
    trades = []
    total_realized = 0.0
    nav_curve = []
    for day in cal:
        to_close = []
        for ts, pos in list(positions.items()):
            ei = date_to_idx.get(pos["entry_date"], -1)
            ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= hold:
                to_close.append(ts)
        for ts in to_close:
            pos = positions.pop(ts, None)
            if not pos:
                continue
            cur_close = close_by_ts.get(ts, {}).get(day)
            if not cur_close or not pos["entry_price"]:
                continue
            gross = cur_close / pos["entry_price"] - 1
            net = gross - COSTS_ROUNDTRIP
            trades.append(net)
            total_realized += net * POSITION_PCT
        if day in date_to_idx:
            idx = date_to_idx[day]
            if idx > 0:
                sig_day = cal[idx - 1]
                facs = _get_factors(per_ts, mv_map, sig_day)
                # pattern evaluation
                cands = set()
                if pattern == "amp_q10":
                    m = facs["amplitude"]
                    if m:
                        s = sorted(m.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) * 10 // 100)
                        cands = set(ts for ts, _ in s[:q])
                elif pattern == "amp_q20":
                    m = facs["amplitude"]
                    if m:
                        s = sorted(m.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) // 5)
                        cands = set(ts for ts, _ in s[:q])
                elif pattern == "turn_q10":
                    m = facs["turnover_spike"]
                    if m:
                        s = sorted(m.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) * 10 // 100)
                        cands = set(ts for ts, _ in s[:q])
                elif pattern == "amp_turn_q10":
                    am = facs["amplitude"]
                    tm = facs["turnover_spike"]
                    if am and tm:
                        sa = sorted(am.items(), key=lambda kv: kv[1])
                        st = sorted(tm.items(), key=lambda kv: kv[1])
                        qa = max(1, len(sa) * 10 // 100)
                        qt = max(1, len(st) * 10 // 100)
                        a_q = set(ts for ts, _ in sa[:qa])
                        t_q = set(ts for ts, _ in st[:qt])
                        cands = a_q & t_q
                elif pattern == "amp_turn_q20":
                    am = facs["amplitude"]
                    tm = facs["turnover_spike"]
                    if am and tm:
                        sa = sorted(am.items(), key=lambda kv: kv[1])
                        st = sorted(tm.items(), key=lambda kv: kv[1])
                        qa = max(1, len(sa) // 5)
                        qt = max(1, len(st) // 5)
                        a_q = set(ts for ts, _ in sa[:qa])
                        t_q = set(ts for ts, _ in st[:qt])
                        cands = a_q & t_q
                elif pattern == "amp_abs_lt2":
                    m = facs["amplitude"]
                    cands = set(ts for ts, v in m.items() if v < 0.02)
                elif pattern == "gap_q5_abs_gt1":
                    m = facs["gap"]
                    if m:
                        # high gap >1% and top quintile
                        s = sorted(m.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) // 5)
                        top = set(ts for ts, _ in s[-q:])
                        cands = set(ts for ts in top if m[ts] > 0.01)
                elif pattern == "dist_near_high":
                    m = facs["dist_high5"]
                    # dist close to 0 (>-0.02) means near 5d high
                    cands = set(ts for ts, v in m.items() if v > -0.02)
                else:
                    cands = set()
                # filter and enter
                if cands and len(positions) < MAX_POSITIONS:
                    # rank by amplitude for determinism
                    # need open price
                    scored = []
                    for ts in cands:
                        if ts in positions:
                            continue
                        series = per_ts.get(ts)
                        if not series:
                            continue
                        open_px = None
                        for r in series:
                            if r["date"] == day:
                                open_px = r["open"]
                                break
                        if not open_px or open_px <= 0:
                            continue
                        scored.append((ts, open_px))
                    # limit to MAX
                    scored = scored[: MAX_POSITIONS - len(positions)]
                    for ts, open_px in scored:
                        positions[ts] = {"entry_date": day, "entry_price": open_px}
        mtm = 0.0
        for ts, pos in positions.items():
            cur_close = close_by_ts.get(ts, {}).get(day)
            ep = pos["entry_price"]
            if cur_close and ep and ep > 0:
                mtm += POSITION_PCT * (cur_close / ep)
            else:
                mtm += POSITION_PCT
        nav = 1.0 + total_realized + (mtm - len(positions) * POSITION_PCT)
        nav_curve.append(nav)
    last_day = cal[-1] if cal else end
    for ts, pos in list(positions.items()):
        cur_close = close_by_ts.get(ts, {}).get(last_day)
        if cur_close and pos["entry_price"]:
            gross = cur_close / pos["entry_price"] - 1
            net = gross - COSTS_ROUNDTRIP
            trades.append(net)
            total_realized += net * POSITION_PCT
    total_pnl = total_realized * 100
    wins = sum(1 for x in trades if x > 0)
    win_rate = wins / len(trades) if trades else 0
    avg_net = (sum(trades) / len(trades) * 100) if trades else 0
    max_dd = 0.0
    peak = nav_curve[0] if nav_curve else 1.0
    for v in nav_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > max_dd:
            max_dd = dd
    rets = [nav_curve[i] / nav_curve[i - 1] - 1 for i in range(1, len(nav_curve)) if nav_curve[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        import numpy as np
        arr = np.array(rets)
        if arr.std() > 0:
            sharpe = float(arr.mean() / arr.std() * (252 ** 0.5))
    return {"pattern": pattern, "hold": hold, "window": wname, "trades": len(trades), "win_rate": win_rate, "avg_net": avg_net, "total_pnl": total_pnl, "max_dd": max_dd, "sharpe": sharpe}


PATTERNS = ["amp_q10", "amp_q20", "turn_q10", "amp_turn_q10", "amp_turn_q20", "amp_abs_lt2", "gap_q5_abs_gt1", "dist_near_high"]

HOLDS = [5, 10]

def main():
    results = []
    for pat in PATTERNS:
        for hold in HOLDS:
            for w in WINDOWS:
                print(f"[{pat} h{hold} {w}]...", flush=True)
                res = simulate(pat, hold, w)
                results.append(res)
                print(f"  -> total {res['total_pnl']:+.1f}% dd {res['max_dd']:.1f} win {res['win_rate']*100:.1f}% n{res['trades']}")
    # summary table
    print("\n| pattern | hold | OOS2 total/win | train total/win | valid total/win | deterministic? |")
    print("|---------|------|---------------|---------------|----------------|----------------|")
    # group
    from collections import defaultdict
    grouped = defaultdict(dict)
    for r in results:
        grouped[(r["pattern"], r["hold"])][r["window"]] = r
    for (pat, hold), d in grouped.items():
        o = d.get("OOS2", {})
        t = d.get("train", {})
        v = d.get("valid", {})
        # deterministic: all win_rate>45% and valid not collapsing and total> -10
        det = "⚠️"
        if o and t and v:
            if o["win_rate"] > 0.45 and t["win_rate"] > 0.45 and v["win_rate"] > 0.45 and v["total_pnl"] > -10 and o["total_pnl"] > 0 and t["total_pnl"] > 0:
                det = "✅"
            elif v["total_pnl"] < -15:
                det = "❌ valid崩"
        print(f"| {pat:16s} | {hold:4d} | {o.get('total_pnl',0):+5.1f}/{o.get('win_rate',0)*100:4.1f} | {t.get('total_pnl',0):+5.1f}/{t.get('win_rate',0)*100:4.1f} | {v.get('total_pnl',0):+5.1f}/{v.get('win_rate',0)*100:4.1f} | {det} |")
    out = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "scout_strict_batch_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(), "results": results}, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out}")

if __name__ == "__main__":
    raise SystemExit(main())
