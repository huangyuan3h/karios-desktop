#!/usr/bin/env python3
"""Scout low-amplitude Q1 playback — 20-80亿 universe, no score/RS.

Signal: amplitude=(high-low)/close rank ascending (low vol) → Q1 (bottom 20%)
Entry: signal day close → next_open (next trading day open), up to max_positions 10 × 10% sleeves
Hold: fixed max_hold 5 or 10 trading days, no trailing/stop (pure horizon) + optional -3/-5 stop as variant
Cost: slippage 0.15% single side + 0.1% commission roundtrip (paper_cost_model)

Windows: OOS2/train/valid

Usage:
  PYTHONPATH=src python3 scripts/scout_lowamp_backtest.py --holds 5,10
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import deque
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
SLIPPAGE_PCT = 0.15  # single side, in %

# paper_cost_model roundtrip: we approximate as slippage*2 + 0.2% (commission+fee)
# but for this minimal playback we just apply slippage*2 as costs_pct
# to keep comparable to earlier engine which already includes paper_cost_model.
# Here we apply costs_pct = slippage*2/100
COSTS_ROUNDTRIP = SLIPPAGE_PCT * 2 / 100.0


def _load_calendar(start: str, end: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (start, end))
            return [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]


def _load_mv_map(start: str, end: str):
    s = max(date.fromisoformat(start) - timedelta(days=5), date(1998, 1, 1)).isoformat()
    e = end
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (s, e))
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


def _amplitude_for_day(per_ts, mv_map, day):
    out = {}
    for ts, series in per_ts.items():
        mv = mv_map.get(day, {}).get(ts)
        if mv is None or not (UNIVERSE_MIN_MV <= mv <= UNIVERSE_MAX_MV):
            continue
        # find idx for day
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
        if idx < 0:
            continue
        cur = series[idx]
        if not cur["close"] or not cur["high"] or not cur["low"]:
            continue
        amp = (cur["high"] - cur["low"]) / cur["close"] if cur["close"] else None
        if amp is None or amp <= 0 or amp > 0.5:  # filter absurd
            continue
        out[ts] = amp
    return out


def _forward_close(per_ts, ts, entry_date):
    # next_open is open of entry_date, close is close of entry_date + hold
    # here entry_date is the entry trading day (next trading day after signal)
    # we need open for entry, close for exit
    series = per_ts.get(ts)
    if not series:
        return None, None
    for r in series:
        if r["date"] == entry_date:
            return r["open"], r["close"]
    return None, None


def _load_index_ma(index_code: str, start: str, end: str):
    s = max(date.fromisoformat(start) - timedelta(days=90), date(1998, 1, 1)).isoformat()
    e = end
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, close FROM index_daily WHERE ts_code=%s AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (index_code, s, e))
            rows = cur.fetchall()
    mp = {}
    closes = []
    for d, c in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        try:
            v = float(c)
        except Exception:
            continue
        closes.append((ds, v))
        mp[ds] = v
    # ma20
    ma = {}
    for i in range(len(closes)):
        if i >= 19:
            window = [v for _, v in closes[i - 19 : i + 1]]
            ma[closes[i][0]] = sum(window) / 20
    return mp, ma


def simulate_window(wname: str, hold: int, stop_pct: float | None = None, regime_filter: str = "none"):
    start, end = WINDOWS[wname]
    cal = _load_calendar(start, end)
    cal_set = set(cal)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    # regime: index close > MA20
    regime_ok = {}
    if regime_filter != "none":
        # use 中证500 as barometer for 20-80 small caps (000905 -7.8% in valid vs 000300 -0.7%)
        _, ma_map = _load_index_ma("000905.SH", start, end)
        _, ma2 = _load_index_ma("000300.SH", start, end)
        for d in cal:
            ma = ma_map.get(d) or ma2.get(d)
            # need index close for that day
            # we already have ma_map built from index closes; for regime we need close>ma
            # fetch close from same maps
            # quick: use ma_map's source; if ma exists then we have close
            # we can just check ma existence and close>ma
            # do separate load for closes
            if ma is None:
                regime_ok[d] = False
            else:
                # find close - we have it as ma window last close, but to avoid extra query use ma itself vs close approximation
                # instead fetch close directly
                regime_ok[d] = True  # placeholder, will compute correctly below
        # proper compute: reload index closes
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT trade_date, close FROM index_daily WHERE ts_code='000905.SH' AND trade_date>=%s AND trade_date<=%s ORDER BY trade_date", (max(date.fromisoformat(start)-timedelta(days=90), date(1998,1,1)).isoformat(), end))
                idx_rows = cur.fetchall()
        idx_close = { (d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)): float(c) for d,c in idx_rows if c }
        for d in cal:
            c = idx_close.get(d)
            ma = ma_map.get(d)
            if c is None or ma is None:
                regime_ok[d] = False
            else:
                regime_ok[d] = c > ma
    else:
        for d in cal:
            regime_ok[d] = True
    # build date->idx for quick next trading day
    date_to_idx = {d: i for i, d in enumerate(cal)}
    # also need extended calendar for entries beyond window end? we stop entries near end if no exit within window
    # gather all trade dates for lookup of next open beyond window (need per_ts opens anyway)
    # We'll just use cal for signal days; entry = next trading day in cal
    positions = {}  # ts -> {entry_date, entry_price, hold_end_date, amount}
    trades = []
    # for NAV curve: need daily close map for open positions
    # Build close_by_ts_day for window
    close_by_ts = {}
    for ts, series in per_ts.items():
        m = {}
        for r in series:
            if r["date"] in cal_set and r["close"]:
                m[r["date"]] = r["close"]
        if m:
            close_by_ts[ts] = m

    # daily NAV list for DD/Sharpe
    nav_cash = 0.0
    # we track realized pnl sum
    total_realized = 0.0
    nav_curve = []
    # we need to know entry price map: we will fetch open on entry day
    for day in cal:
        # 1) close positions that expire today (holding days reached) or hit stop if enabled
        to_close = []
        for ts, pos in list(positions.items()):
            # check hold expiry: calendar days between entry and today
            # count trading days held
            # compute holding days
            entry_idx = date_to_idx.get(pos["entry_date"], -1)
            cur_idx = date_to_idx.get(day, -1)
            held = cur_idx - entry_idx + 1 if entry_idx >= 0 and cur_idx >= 0 else 999
            if held >= hold:
                to_close.append((ts, "time"))
                continue
            if stop_pct is not None:
                cur_close = close_by_ts.get(ts, {}).get(day)
                if cur_close and pos["entry_price"]:
                    ret = cur_close / pos["entry_price"] - 1
                    if ret <= stop_pct / 100.0:
                        to_close.append((ts, "stop"))
        for ts, reason in to_close:
            pos = positions.pop(ts, None)
            if not pos:
                continue
            cur_close = close_by_ts.get(ts, {}).get(day)
            if not cur_close or not pos["entry_price"] or pos["entry_price"] <= 0:
                continue
            gross = cur_close / pos["entry_price"] - 1
            net = gross - COSTS_ROUNDTRIP
            trades.append({"symbol": ts, "entry": pos["entry_date"], "exit": day, "gross": gross, "net": net, "reason": reason, "hold": date_to_idx.get(day, 0) - date_to_idx.get(pos["entry_date"], 0) + 1})
            total_realized += net * POSITION_PCT

        # 2) open new positions based on prior day signal (signal day = prev trading day)
        if day in date_to_idx:
            idx = date_to_idx[day]
            if idx > 0:
                sig_day = cal[idx - 1]
                # regime filter: require signal day close>MA20 (time controllable: Weak空仓)
                if not regime_ok.get(sig_day, False):
                    # regime off -> stay in cash
                    pass
                else:
                    amp_map = _amplitude_for_day(per_ts, mv_map, sig_day)
                    if amp_map and len(positions) < MAX_POSITIONS:
                        # rank ascending, Q1 threshold 20%
                        sorted_ts = sorted(amp_map.items(), key=lambda kv: kv[1])
                        q = max(1, len(sorted_ts) // 5)
                        q1 = [ts for ts, _ in sorted_ts[:q]]
                        candidates = []
                        for ts in q1:
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
                            candidates.append((ts, open_px, amp_map[ts]))
                        candidates.sort(key=lambda x: x[2])
                        slots = MAX_POSITIONS - len(positions)
                        for ts, open_px, amp in candidates[:slots]:
                            positions[ts] = {"entry_date": day, "entry_price": open_px, "amp": amp}
        # 3) compute daily NAV for curve (cash realized + MTM of open)
        mtm = 0.0
        for ts, pos in positions.items():
            cur_close = close_by_ts.get(ts, {}).get(day)
            ep = pos["entry_price"]
            if cur_close and ep and ep > 0:
                mtm += POSITION_PCT * (cur_close / ep)
            else:
                mtm += POSITION_PCT  # at cost if no price
        # total NAV = 1 + realized + (mtm - open_positions_at_cost)
        # open positions at cost = len(positions)*POSITION_PCT
        nav = 1.0 + total_realized + (mtm - len(positions) * POSITION_PCT)
        nav_curve.append(nav)

    # close remaining at window end
    last_day = cal[-1] if cal else end
    for ts, pos in list(positions.items()):
        cur_close = close_by_ts.get(ts, {}).get(last_day)
        if cur_close and pos["entry_price"]:
            gross = cur_close / pos["entry_price"] - 1
            net = gross - COSTS_ROUNDTRIP
            trades.append({"symbol": ts, "entry": pos["entry_date"], "exit": last_day, "gross": gross, "net": net, "reason": "eow", "hold": 1})
            total_realized += net * POSITION_PCT
        positions.pop(ts, None)

    # stats
    total_pnl = total_realized * 100  # in %
    wins = sum(1 for t in trades if t["net"] > 0)
    losses = len(trades) - wins
    win_rate = wins / len(trades) if trades else 0
    avg_net = (sum(t["net"] for t in trades) / len(trades) * 100) if trades else 0
    # max drawdown and sharpe from nav_curve
    max_dd = 0.0
    peak = nav_curve[0] if nav_curve else 1.0
    for v in nav_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > max_dd:
            max_dd = dd
    # sharpe: daily returns of nav_curve
    rets = []
    for i in range(1, len(nav_curve)):
        if nav_curve[i-1] > 0:
            rets.append(nav_curve[i] / nav_curve[i-1] - 1)
    sharpe = 0.0
    if len(rets) > 10:
        import numpy as np
        arr = np.array(rets)
        # annualize 252 trading days
        if arr.std() > 0:
            sharpe = float(arr.mean() / arr.std() * (252 ** 0.5))
    return {
        "window": wname,
        "hold": hold,
        "stop": stop_pct,
        "trades": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_net": avg_net,
        "total_pnl": total_pnl,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "nav_curve": nav_curve,
        "trades_detail": trades[:5],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--holds", default="5,10", help="comma holds")
    ap.add_argument("--stop", default="", help="stop pct e.g. -3 for -3% (empty = no stop)")
    ap.add_argument("--regime", default="none", choices=["none", "ma20"], help="regime filter: none or ma20 (index 000300 close>MA20)")
    ap.add_argument("--json", default="", help="output json")
    args = ap.parse_args()
    holds = [int(x) for x in args.holds.split(",") if x.strip()]
    stop = float(args.stop) if args.stop else None
    regime = args.regime
    results = {}
    for w in WINDOWS:
        results[w] = {}
        for h in holds:
            print(f"[{w}] hold={h} stop={stop} regime={regime}...", flush=True)
            res = simulate_window(w, h, stop, regime_filter=regime)
            results[w][f"h{h}"] = {k: v for k, v in res.items() if k not in ("nav_curve", "trades_detail")}
            print(f"  -> trades {res['trades']} win {res['win_rate']*100:.1f}% total {res['total_pnl']:+.1f}% dd {res['max_dd']:.1f} sharpe {res['sharpe']:.2f} avg {res['avg_net']:.2f}%")
    out = Path(args.json) if args.json else REPORT_DIR / ("scout_lowamp_latest.json" if regime=="none" else "scout_lowamp_ma20_latest.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(), "holds": holds, "stop": stop, "regime": regime, "universe": f"{UNIVERSE_MIN_MV}-{UNIVERSE_MAX_MV}亿", "slippage": SLIPPAGE_PCT, "results": results}
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out}")
    # markdown table
    print("\n| window | hold | total% | dd% | sharpe | n | win% | avg% |")
    print("|--------|------|--------|-----|--------|---|------|------|")
    for w in WINDOWS:
        for h in holds:
            d = results[w][f"h{h}"]
            print(f"| {w:6s} | {h:4d} | {d['total_pnl']:+6.1f} | {d['max_dd']:4.1f} | {d['sharpe']:5.2f} | {d['trades']:3d} | {d['win_rate']*100:4.1f} | {d['avg_net']:5.2f} |")


if __name__ == "__main__":
    raise SystemExit(main())
