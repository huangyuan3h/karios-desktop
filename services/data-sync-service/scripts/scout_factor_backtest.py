#!/usr/bin/env python3
"""Generic scout factor Q1/Q5 playback — 20-80亿 universe.

Usage:
  PYTHONPATH=src python3 scripts/scout_factor_backtest.py --factor gap --quintile Q5 --holds 5,10
  PYTHONPATH=src python3 scripts/scout_factor_backtest.py --factor dist_high5 --quintile Q5 --holds 5,10
  PYTHONPATH=src python3 scripts/scout_factor_backtest.py --factor turnover_spike --quintile Q1 --holds 5,10
  PYTHONPATH=src python3 scripts/scout_factor_backtest.py --factor amplitude --quintile Q5 --holds 5  # high-amp short proxy (long Q5 to see underperformance)
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


def _factor_for_day(per_ts, mv_map, day, factor: str):
    out = {}
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
        val = None
        if factor == "amplitude":
            if cur["high"] and cur["low"]:
                val = (cur["high"] - cur["low"]) / cur["close"] if cur["close"] else None
        elif factor == "turnover_spike":
            amts = [r["amount"] for r in series[idx - 20 : idx + 1] if r["amount"]]
            if len(amts) >= 15:
                avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1)
                val = (cur["amount"] / avg20) if avg20 and cur["amount"] else None
        elif factor == "gap":
            if idx > 0 and cur["open"] and series[idx - 1]["close"]:
                val = cur["open"] / series[idx - 1]["close"] - 1
        elif factor == "dist_high5":
            highs5 = [r["high"] for r in series[idx - 5 : idx + 1] if r["high"]]
            if highs5:
                mh = max(highs5)
                val = (cur["close"] - mh) / mh if mh else None
        elif factor == "ret5":
            if idx >= 5 and series[idx - 5]["close"]:
                val = cur["close"] / series[idx - 5]["close"] - 1
        elif factor == "neg_mv":
            val = -mv
        else:
            continue
        if val is None or not isinstance(val, (int, float)):
            continue
        # filter absurd
        if factor == "amplitude" and (val <= 0 or val > 0.5):
            continue
        if factor == "turnover_spike" and (val <= 0 or val > 10):
            continue
        out[ts] = float(val)
    return out


def simulate_window(wname: str, factor: str, quintile: str, hold: int):
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
        # open
        if day in date_to_idx:
            idx = date_to_idx[day]
            if idx > 0:
                sig_day = cal[idx - 1]
                fmap = _factor_for_day(per_ts, mv_map, sig_day, factor)
                if fmap and len(positions) < MAX_POSITIONS:
                    sorted_ts = sorted(fmap.items(), key=lambda kv: kv[1])
                    q = max(1, len(sorted_ts) // 5)
                    if quintile == "Q1":
                        sel = [ts for ts, _ in sorted_ts[:q]]
                    else:
                        sel = [ts for ts, _ in sorted_ts[-q:]]
                    cands = []
                    for ts in sel:
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
                        cands.append((ts, open_px, fmap[ts]))
                    # sort by factor order to keep selection stable
                    if quintile == "Q1":
                        cands.sort(key=lambda x: x[2])
                    else:
                        cands.sort(key=lambda x: -x[2])
                    slots = MAX_POSITIONS - len(positions)
                    for ts, open_px, fv in cands[:slots]:
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
    return {"window": wname, "factor": factor, "quintile": quintile, "hold": hold, "trades": len(trades), "win_rate": win_rate, "avg_net": avg_net, "total_pnl": total_pnl, "max_dd": max_dd, "sharpe": sharpe}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", required=True, help="amplitude, turnover_spike, gap, dist_high5, ret5, neg_mv")
    ap.add_argument("--quintile", default="Q1", choices=["Q1", "Q5"])
    ap.add_argument("--holds", default="5,10")
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    holds = [int(x) for x in args.holds.split(",") if x.strip()]
    results = {}
    for w in WINDOWS:
        results[w] = {}
        for h in holds:
            print(f"[{w}] {args.factor} {args.quintile} hold={h}...", flush=True)
            res = simulate_window(w, args.factor, args.quintile, h)
            results[w][f"h{h}"] = res
            print(f"  -> total {res['total_pnl']:+.1f}% dd {res['max_dd']:.1f} sharpe {res['sharpe']:.2f} n{res['trades']} win{res['win_rate']*100:.1f}%")
    out = Path(args.json) if args.json else REPORT_DIR / f"scout_{args.factor}_{args.quintile.lower()}_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(), "factor": args.factor, "quintile": args.quintile, "holds": holds, "results": results}
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
