#!/usr/bin/env python3
"""Breadth gate: % of 20-80 stocks close>MA20 > 0.5 to trade."""

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

UNIVERSE_MIN_MV = 20.0
UNIVERSE_MAX_MV = 80.0
POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.003


def _load_calendar(s, e):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (s, e))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]


def _load_mv_map(s, e):
    s2 = max(date.fromisoformat(s) - timedelta(days=5), date(1998, 1, 1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s2, e))
            rows = cur.fetchall()
    out = {}
    for d, ts, mv in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out.setdefault(ds, {})[str(ts)] = float(mv) / 10000.0
    return out


def _load_daily(s, e):
    s2 = max(date.fromisoformat(s) - timedelta(days=90), date(1998, 1, 1)).isoformat()
    e2 = (date.fromisoformat(e) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s2, e2))
            rows = cur.fetchall()
    per_ts = {}
    for d, ts, o, h, l, c, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts.setdefault(str(ts), []).append({"date": ds, "open": float(o) if o else None, "high": float(h) if h else None, "low": float(l) if l else None, "close": float(c) if c else None, "amount": float(amt) if amt else None})
    return per_ts


def _breadth_by_day(per_ts, mv_map, cal):
    # for each day, % of 20-80 stocks where close>MA20
    # need MA20 per stock
    breadth = {}
    for day in cal:
        cnt = 0
        tot = 0
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
            if idx < 20:
                continue
            closes = [r["close"] for r in series[idx - 20 : idx + 1] if r["close"]]
            if len(closes) < 20:
                continue
            ma20 = sum(closes[:-1]) / 20 if len(closes) == 21 else sum(closes) / len(closes)
            # actually closes includes current day, need MA20 of prior 20 days excluding current? Use 20 days including current vs prior?
            # Simplify: MA20 = avg of last 20 closes including current day
            ma20 = sum(closes[-20:]) / 20
            cur_close = series[idx]["close"]
            if cur_close and ma20:
                tot += 1
                if cur_close > ma20:
                    cnt += 1
        breadth[day] = cnt / tot if tot else 0
    return breadth


def simulate(wname, thresh: float):
    start, end = WINDOWS[wname]
    cal = _load_calendar(start, end)
    cal_set = set(cal)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    breadth = _breadth_by_day(per_ts, mv_map, cal)
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
    pos_counts = []
    blocked = 0
    for day in cal:
        to_close = []
        for ts, pos in list(positions.items()):
            ei = date_to_idx.get(pos["entry_date"], -1)
            ci = date_to_idx.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= 10:
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
                if breadth.get(sig_day, 0) < thresh:
                    blocked += 1
                else:
                    # amp_q10
                    amp = {}
                    for ts, series in per_ts.items():
                        mv = mv_map.get(sig_day, {}).get(ts)
                        if mv is None or not (UNIVERSE_MIN_MV <= mv <= UNIVERSE_MAX_MV):
                            continue
                        s_idx = -1
                        for i, r in enumerate(series):
                            if r["date"] == sig_day:
                                s_idx = i
                                break
                            if r["date"] > sig_day:
                                break
                        if s_idx < 0:
                            continue
                        cur = series[s_idx]
                        if not cur["close"] or not cur["high"] or not cur["low"]:
                            continue
                        a = (cur["high"] - cur["low"]) / cur["close"] if cur["close"] else None
                        if a is None or not (0 < a <= 0.5):
                            continue
                        amp[ts] = a
                    if amp and len(positions) < MAX_POSITIONS:
                        s = sorted(amp.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) * 10 // 100)
                        cands = [ts for ts, _ in s[:q] if ts not in positions]
                        scored = []
                        for ts in cands:
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
        pos_counts.append(len(positions))
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
        arr = __import__("numpy").array(rets)
        if arr.std() > 0:
            sharpe = float(arr.mean() / arr.std() * (252 ** 0.5))
    avg_pos = sum(pos_counts) / len(pos_counts) if pos_counts else 0
    hold_ratio = avg_pos / MAX_POSITIONS * 100
    return {"thresh": thresh, "window": wname, "trades": len(trades), "win_rate": win_rate, "avg_net": avg_net, "total_pnl": total_pnl, "max_dd": max_dd, "sharpe": sharpe, "hold_ratio": hold_ratio, "blocked": blocked}


for thresh in [0.4, 0.5, 0.6]:
    print(f"\n=== breadth > {thresh} ===")
    for w in ["OOS2", "train", "valid"]:
        res = simulate(w, thresh)
        print(f"{w:6s} total {res['total_pnl']:+6.1f}% dd {res['max_dd']:4.1f} win {res['win_rate']*100:4.1f}% hold {res['hold_ratio']:4.1f}% blocked {res['blocked']}")
