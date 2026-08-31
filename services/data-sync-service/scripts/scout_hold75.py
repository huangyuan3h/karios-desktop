#!/usr/bin/env python3
"""Hold ratio 75% test — amp_q10 10d with sparser gates.

Gates:
  flow_any_positive (sectorOutflowBlock): at least one industry net inflow >0
  ma60_905: 000905 close>MA60 (stricter than MA20)
  combo: flow & ma60

Goal: reduce hold_ratio 98% -> ~75% and see valid improvement.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}

UNIVERSE_MIN_MV = 20.0
UNIVERSE_MAX_MV = 80.0
POSITION_PCT = 0.10
MAX_POSITIONS = 10
COSTS_ROUNDTRIP = 0.15 * 2 / 100.0


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


def _load_flow_any(start, end):
    s2 = max(date.fromisoformat(start) - timedelta(days=5), date(2024, 1, 1)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT date, net_inflow FROM market_cn_industry_fund_flow_daily WHERE date >= %s AND date <= %s", (s2, end))
            rows = cur.fetchall()
    by_date = {}
    for d, v in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        try:
            fv = float(v or 0)
        except:
            fv = 0
        by_date.setdefault(ds, []).append(fv)
    out = {}
    for ds, vals in by_date.items():
        out[ds] = any(x > 0 for x in vals)
    return out


def _load_ma60():
    # preload all 000905 closes for MA60
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, close FROM index_daily WHERE ts_code='000905.SH' ORDER BY trade_date")
            rows = cur.fetchall()
    closes = [(d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d), float(c)) for d, c in rows if c]
    ma = {}
    for i in range(len(closes)):
        if i >= 59:
            window = [v for _, v in closes[i - 59 : i + 1]]
            ma[closes[i][0]] = sum(window) / 60
    close_map = {d: v for d, v in closes}
    return close_map, ma


IDX_CLOSE, IDX_MA60 = _load_ma60()


def _factors_amp(per_ts, mv_map, day):
    amp = {}
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
        if not cur["close"] or not cur["high"] or not cur["low"]:
            continue
        a = (cur["high"] - cur["low"]) / cur["close"] if cur["close"] else None
        if a is None or not (0 < a <= 0.5):
            continue
        amp[ts] = a
    return amp


def simulate(wname, gate: str):
    # gate: none, flow, ma60, flow_ma60
    start, end = WINDOWS[wname]
    cal = _load_calendar(start, end)
    cal_set = set(cal)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    flow_any = _load_flow_any(start, end) if "flow" in gate else {}
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
        # close
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
        # gate check for signal day
        if day in date_to_idx:
            idx = date_to_idx[day]
            if idx > 0:
                sig_day = cal[idx - 1]
                # gate
                ok = True
                if gate == "flow":
                    ok = flow_any.get(sig_day, True)  # fail-open if missing (pre-2024)
                    # but for 2024+ flow exists from 2024-01, so ok is meaningful
                    if sig_day < "2024-01-01":
                        ok = True
                elif gate == "ma60":
                    c = IDX_CLOSE.get(sig_day)
                    ma = IDX_MA60.get(sig_day)
                    ok = (c is not None and ma is not None and c > ma)
                elif gate == "flow_ma60":
                    c = IDX_CLOSE.get(sig_day)
                    ma = IDX_MA60.get(sig_day)
                    ok1 = (c is not None and ma is not None and c > ma)
                    ok2 = flow_any.get(sig_day, True)
                    if sig_day < "2024-01-01":
                        ok2 = True
                    ok = ok1 and ok2
                if not ok:
                    blocked += 1
                else:
                    amp = _factors_amp(per_ts, mv_map, sig_day)
                    if amp and len(positions) < MAX_POSITIONS:
                        s = sorted(amp.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) * 10 // 100)
                        cands = [ts for ts, _ in s[:q] if ts not in positions]
                        # need open price
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
        # nav
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
    # close remaining
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
    return {"gate": gate, "window": wname, "trades": len(trades), "win_rate": win_rate, "avg_net": avg_net, "total_pnl": total_pnl, "max_dd": max_dd, "sharpe": sharpe, "hold_ratio": hold_ratio, "blocked_days": blocked}


GATES = ["none", "flow", "ma60", "flow_ma60"]

for gate in GATES:
    print(f"\n=== gate {gate} ===")
    for w in ["OOS2", "train", "valid", "past_year", "long"]:
        res = simulate(w, gate)
        print(f"{w:10s} total {res['total_pnl']:+6.1f}% dd {res['max_dd']:4.1f} sharpe {res['sharpe']:5.2f} n {res['trades']:3d} win {res['win_rate']*100:4.1f}% hold {res['hold_ratio']:4.1f}% blocked {res['blocked_days']}")
