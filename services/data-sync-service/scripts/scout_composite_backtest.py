#!/usr/bin/env python3
"""Composite low-vol: amplitude Q1 ∩ turnover_spike Q1  →  20-80亿 small-mid.

Signal: both factors bottom 20% (double low-vol) on signal day
Entry: next_open, 10%×10, 0.15% slippage, holds 5/10
Also tests: amplitude Q1 ∩ gap filter, etc. For now double low-vol only.

Windows: OOS2/train/valid
"""
from __future__ import annotations

import argparse
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

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
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


def _factors_for_day(per_ts, mv_map, day):
    amp = {}
    turn = {}
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
        # amplitude
        a = (cur["high"] - cur["low"]) / cur["close"] if cur["close"] else None
        if a is None or a <= 0 or a > 0.5:
            continue
        # turnover spike
        amts = [r["amount"] for r in series[idx - 20 : idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1)
        t = (cur["amount"] / avg20) if avg20 and cur["amount"] else None
        if t is None or t <= 0 or t > 10:
            continue
        amp[ts] = a
        turn[ts] = t
    return amp, turn


def simulate_window(wname: str, hold: int):
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
        # close
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
            trades.append({"net": net})
            total_realized += net * POSITION_PCT
        # open: signal = prev day, composite Q1 ∩ Q1
        if day in date_to_idx:
            idx = date_to_idx[day]
            if idx > 0:
                sig_day = cal[idx - 1]
                amp_map, turn_map = _factors_for_day(per_ts, mv_map, sig_day)
                if amp_map and turn_map and len(positions) < MAX_POSITIONS:
                    # Q1 thresholds 20%
                    def q1_set(m):
                        s = sorted(m.items(), key=lambda kv: kv[1])
                        q = max(1, len(s) // 5)
                        return set(ts for ts, _ in s[:q])
                    amp_q1 = q1_set(amp_map)
                    turn_q1 = q1_set(turn_map)
                    inter = amp_q1 & turn_q1
                    # if inter empty, skip (no trade that day) — this yields low trading time ~25% as user noted
                    if inter:
                        # rank composite by amplitude+turn normalized rank sum
                        # compute ranks
                        amp_sorted = sorted(amp_map.items(), key=lambda kv: kv[1])
                        amp_rank = {ts: i for i, (ts, _) in enumerate(amp_sorted)}
                        turn_sorted = sorted(turn_map.items(), key=lambda kv: kv[1])
                        turn_rank = {ts: i for i, (ts, _) in enumerate(turn_sorted)}
                        scored = [(ts, amp_rank[ts] + turn_rank[ts]) for ts in inter if ts not in positions]
                        scored.sort(key=lambda x: x[1])
                        # need open price today
                        cands = []
                        for ts, _ in scored:
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
                            cands.append((ts, open_px))
                        slots = MAX_POSITIONS - len(positions)
                        for ts, open_px in cands[:slots]:
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
    last_day = cal[-1] if cal else end
    for ts, pos in list(positions.items()):
        cur_close = close_by_ts.get(ts, {}).get(last_day)
        if cur_close and pos["entry_price"]:
            gross = cur_close / pos["entry_price"] - 1
            net = gross - COSTS_ROUNDTRIP
            trades.append({"net": net})
            total_realized += net * POSITION_PCT
    total_pnl = total_realized * 100
    wins = sum(1 for t in trades if t["net"] > 0)
    win_rate = wins / len(trades) if trades else 0
    avg_net = (sum(t["net"] for t in trades) / len(trades) * 100) if trades else 0
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
    # trading time: days with at least one signal inter / total days
    # we can approximate by counting days where composite had inter
    return {"window": wname, "hold": hold, "trades": len(trades), "win_rate": win_rate, "avg_net": avg_net, "total_pnl": total_pnl, "max_dd": max_dd, "sharpe": sharpe}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--holds", default="5,10")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    holds = [int(x) for x in args.holds.split(",") if x.strip()]
    results = {}
    for w in WINDOWS:
        results[w] = {}
        for h in holds:
            print(f"[{w}] composite low-vol hold={h}...", flush=True)
            res = simulate_window(w, h)
            results[w][f"h{h}"] = res
            print(f"  -> total {res['total_pnl']:+.1f}% dd {res['max_dd']:.1f} sharpe {res['sharpe']:.2f} n{res['trades']} win{res['win_rate']*100:.1f}%")
    out = Path(args.json) if args.json else REPORT_DIR / "scout_composite_lowvol_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(), "holds": holds, "composite": "amplitude Q1 ∩ turnover_spike Q1", "universe": f"{UNIVERSE_MIN_MV}-{UNIVERSE_MAX_MV}亿", "results": results}
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out}")
    print("\n| window | hold | total% | dd% | sharpe | n | win% |")
    print("|--------|------|--------|-----|--------|---|------|")
    for w in WINDOWS:
        for h in holds:
            d = results[w][f"h{h}"]
            print(f"| {w:6s} | {h:4d} | {d['total_pnl']:+6.1f} | {d['max_dd']:4.1f} | {d['sharpe']:5.2f} | {d['trades']:3d} | {d['win_rate']*100:4.1f} |")


if __name__ == "__main__":
    raise SystemExit(main())
