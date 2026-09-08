#!/usr/bin/env python3
"""CPA-CN v1 validation: Oliver Kell Cycle of Price Action as standalone long system in A-shares.

Pre-reg frozen (2026-09-08, read-only vs Postgres, never touches Live):
- Hypothesis: CPA early longs (Wedge Pop + EMA Crossback) with CPA-native exits
  (Wedge Drop / Exhaustion) have positive expectancy in CN A-shares.
- Universe: daily JOIN stock_basic, delist_date IS NULL, name NOT LIKE '%ST%',
  ts_code NOT LIKE '%.BJ' (same as state_bucket_track._load_rows).
- Indicators: EMA10 / EMA20 on qfq close, iterative EMA (seed close[0]), as-of safe.
- Entry signal day S (all known at S close):
  - WEDGE_POP: close[S] > max(EMA10,EMA20)*1.005 AND tightness
    |EMA10-EMA20|/close < 2% AND >=3 of prior 10 closes below max EMA.
  - CROSSBACK: EMA10[S] > EMA20[S] AND low[S] <= max(EMA[S])*1.01
    AND close[S] > max(EMA[S]) AND prior day close > max EMA (established uptrend
    pullback-reclaim). Standalone entry (not only after Pop) for coverage.
  - Liquidity: 60d avg amount (千元->亿元 /100000) >= 0.7, >=30 observations.
  - Only one signal per (ts,S); Pop takes precedence over Crossback.
- Execution: signal S -> entry next trading day T open. Skip if T open missing,
  or limit-up pinned (cannot buy, same _at_limit logic as backtest_engine).
  No volume filter, no RS/gate (pure CPA).
- Exits (evaluated at holding day H close, exit next open E):
  - EXHAUSTION: (close[H]-EMA10[H])/EMA10[H] > 10% -> exit next open.
  - WEDGE_DROP: close[H] < min(EMA10[H],EMA20[H]) -> exit next open.
  - MAX_HOLD 60 trading days -> exit next open.
  - Limit-down pinned on E -> delay one day at a time (max 5 sessions, then force).
  - Missing open -> delay; missing close for MTM -> carry last close.
- Portfolio: window-local empty book, start NAV 1.0, max 10 positions,
  each 10% of NAV at entry, cash<=100%, no leverage.
  Costs: COSTS_ROUNDTRIP=0.003 deducted at exit (same as twin satellite).
- Windows (fixed): OOS2 2024-08-01~2025-08-01 / train 2025-08-01~2026-02-01 /
  valid 2026-03-01~2026-08-07. Warmup 150 calendar days before each start
  for EMA60/liquidity (EMA needs ~60, we use 150 for safety).
- Metrics per window: NAV total, maxDD, Sharpe (daily, rf=0, *sqrt(252)),
  n closed trades, win rate, avg net/trade, worst trade, avg hold days,
  signals/day, entries/day. Leftover open at window end forced at last close
  (reason end_of_window, included in n).
- Verdict is descriptive (absolute performance in CN), not a Live gate.
  Comparison context: S-3 CN NAV baseline OOS2 +47.3 / train +34.1 / valid +38.7
  (strategy-params §3) printed side-by-side for scale only.

Usage:
  PYTHONPATH=src python3 scripts/replay_cpa_cn.py --windows OOS2,train,valid
  PYTHONPATH=src python3 scripts/replay_cpa_cn.py --windows OOS2 --save-report
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

COSTS_ROUNDTRIP = 0.003
POSITION_PCT = 0.10
MAX_POS = 10
MAX_HOLD_DAYS = 60
MIN_AVG_AMOUNT_YI = 0.7
TIGHT_PCT = 0.02
POP_TRIGGER = 1.005
EXHAUST_PCT = 0.10
WARMUP_DAYS = 150

WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
REPORT_FILE = REPORT_DIR / "cpa_cn_v1.json"


def _board_limit_pct(ts: str) -> float | None:
    code = str(ts).split(".")[0]
    if code.startswith(("300", "301", "688")):
        return 0.20
    if code.startswith(("8", "4")):
        return 0.30
    if code.startswith(("60", "00")):
        return 0.10
    return None


def _load_daily(start: str, end: str, a_only: bool = False):
    """Load OHLCV + amount for universe in [start, end]. Returns per_ts dict and calendar.

    a_only=True restricts to A-shares (sb.market IN 主板/创业板/科创板/中小板),
    excluding HK/ETF rows that leak into `daily` (universe-gate correction 2026-09-08;
    v1 default False preserves the frozen v1 reproduction).
    """
    from data_sync_service.db import get_connection

    per_ts: dict[str, list[dict]] = defaultdict(list)
    with get_connection() as conn:
        with conn.cursor() as cur:
            q = (
                """
                SELECT d.trade_date, d.ts_code, d.open, d.high, d.low, d.close,
                       d.pre_close, d.vol, d.amount
                FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
                WHERE d.trade_date >= %s::date AND d.trade_date <= %s::date
                  AND sb.delist_date IS NULL
                  AND sb.name NOT LIKE '%%ST%%'
                  AND d.ts_code NOT LIKE '%%.BJ'
                """
                + ("  AND sb.market IN ('主板','创业板','科创板','中小板')\n" if a_only else "")
                + "ORDER BY d.ts_code, d.trade_date"
            )
            cur.execute(q, (start, end))
            rows = cur.fetchall()
    cal_set: set[str] = set()
    for d, ts, o, h, low, c, pc, v, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
        cal_set.add(ds)
        per_ts[str(ts)].append({
            "date": ds,
            "open": float(o) if o is not None else None,
            "high": float(h) if h is not None else None,
            "low": float(low) if low is not None else None,
            "close": float(c) if c is not None else None,
            "pre_close": float(pc) if pc is not None else None,
            "vol": float(v) if v is not None else None,
            "amount": float(amt) if amt is not None else None,  # 千元
        })
    cal = sorted(cal_set)
    return per_ts, cal


def _ema(values: list[float | None], span: int) -> list[float | None]:
    k = 2.0 / (span + 1)
    out: list[float | None] = [None] * len(values)
    prev: float | None = None
    for i, v in enumerate(values):
        if v is None or v <= 0:
            out[i] = prev
            continue
        if prev is None:
            prev = v
        else:
            prev = v * k + prev * (1 - k)
        out[i] = prev
    return out


def _at_limit_up(ts: str, prev_close: float | None, px: float | None) -> bool:
    if prev_close is None or px is None or prev_close <= 0:
        return False
    lim = _board_limit_pct(ts)
    if lim is None:
        return False
    return px >= round(prev_close * (1.0 + lim), 2) - 0.01


def _at_limit_down(ts: str, prev_close: float | None, px: float | None) -> bool:
    if prev_close is None or px is None or prev_close <= 0:
        return False
    lim = _board_limit_pct(ts)
    if lim is None:
        return False
    return px <= round(prev_close * (1.0 - lim), 2) + 0.01


def _build_features(per_ts: dict[str, list[dict]]):
    """Per symbol: ema10/20 arrays, avg-amount(60d, 亿元), date->idx. Mutates rows with _i."""
    feats: dict[str, dict] = {}
    for ts, rows in per_ts.items():
        closes = [r["close"] for r in rows]
        ema10 = _ema(closes, 10)
        ema20 = _ema(closes, 20)
        # rolling avg amount in 亿元 (千元/100000), as-of inclusive
        avg_amt: list[float | None] = [None] * len(rows)
        window: list[float] = []
        acc = 0.0
        for i, r in enumerate(rows):
            a = r["amount"]
            if a is not None and a > 0:
                window.append(a)
                acc += a
                if len(window) > 60:
                    acc -= window.pop(0)
            if len(window) >= 30:
                avg_amt[i] = round(acc / len(window) / 100000.0, 4)
        d2i = {r["date"]: i for i, r in enumerate(rows)}
        feats[ts] = {"rows": rows, "ema10": ema10, "ema20": ema20, "avg_amt": avg_amt, "d2i": d2i}
    return feats


def _signal_at(feats_ts: dict, i: int) -> tuple[str, float] | None:
    """Return (kind, tightness) | None using data <= i (signal day i)."""
    rows = feats_ts["rows"]
    ema10 = feats_ts["ema10"]
    ema20 = feats_ts["ema20"]
    avg_amt = feats_ts["avg_amt"]
    if i < 20:
        return None
    e10 = ema10[i]
    e20 = ema20[i]
    r = rows[i]
    c, low = r["close"], r["low"]
    if e10 is None or e20 is None or c is None or low is None or c <= 0 or e10 <= 0 or e20 <= 0:
        return None
    aa = avg_amt[i]
    if aa is None or aa < MIN_AVG_AMOUNT_YI:
        return None
    mx = max(e10, e20)
    mn = min(e10, e20)
    tight = (mx - mn) / c
    # WEDGE_POP (faithful: tight basing + volatility contraction + reclaim)
    if c > mx * POP_TRIGGER and tight < TIGHT_PCT:
        below = 0
        for j in range(max(0, i - 10), i):
            cj = rows[j]["close"]
            e10j = ema10[j]
            e20j = ema20[j]
            if cj is None or e10j is None or e20j is None:
                continue
            if cj < max(e10j, e20j):
                below += 1
        if below >= 3:
            # volatility contraction: prior 10d range / close < 15%
            hh = None
            ll = None
            for j in range(max(0, i - 10), i + 1):
                hj, lj = rows[j]["high"], rows[j]["low"]
                if hj is None or lj is None or hj <= 0 or lj <= 0:
                    hh = None
                    break
                hh = hj if hh is None else max(hh, hj)
                ll = lj if ll is None else min(ll, lj)
            if hh is not None and ll is not None and (hh - ll) / c < 0.15:
                return ("pop", tight)
    # CROSSBACK (established uptrend pullback-reclaim)
    if e10 > e20 and low <= mx * 1.01 and c > mx:
        pj = rows[i - 1]
        e10p = ema10[i - 1]
        e20p = ema20[i - 1]
        if pj["close"] is not None and e10p is not None and e20p is not None:
            if pj["close"] > max(e10p, e20p):
                return ("crossback", tight)
    return None


def _exit_reason_at(feats_ts: dict, i: int) -> str | None:
    rows = feats_ts["rows"]
    ema10 = feats_ts["ema10"]
    ema20 = feats_ts["ema20"]
    e10 = ema10[i]
    e20 = ema20[i]
    c = rows[i]["close"]
    if e10 is None or e20 is None or c is None or c <= 0 or e10 <= 0:
        return None
    if (c - e10) / e10 > EXHAUST_PCT:
        return "exhaustion"
    if c < min(e10, e20):
        return "wedge_drop"
    return None


def replay_window(wname: str, wstart: str, wend: str, a_only: bool = False) -> dict:
    warm = (date.fromisoformat(wstart) - timedelta(days=WARMUP_DAYS)).isoformat()
    per_ts, cal_all = _load_daily(warm, wend, a_only=a_only)
    feats = _build_features(per_ts)
    cal = [d for d in cal_all if wstart <= d <= wend]
    cal_idx = {d: k for k, d in enumerate(cal_all)}
    # signals per date (signal day S within window, entry S+1)
    signals: dict[str, list[tuple[str, str, float]]] = defaultdict(list)  # date -> [(ts, kind, tight)]
    n_signals = 0
    for ts, f in feats.items():
        rows = f["rows"]
        for i, r in enumerate(rows):
            d = r["date"]
            if not (wstart <= d <= wend):
                continue
            # need next trading day to exist for entry; skip last window day
            sig = _signal_at(f, i)
            if sig:
                kind, tight = sig
                signals[d].append((ts, kind, tight))
                n_signals += 1
    # portfolio replay over window calendar
    cash = 1.0
    positions: list[dict] = []  # ts, shares, entry_px, entry_date, kind, hold_days
    nav_series: list[tuple[str, float]] = []
    trades: list[dict] = []
    pending_exits: list[dict] = []  # positions flagged at H close, exit next open
    n_entries = 0
    # helper: close lookup per (ts,date)
    for d in cal:
        # 1) execute pending exits at today open (with limit-down delay handling inside)
        still_pending = []
        for p in pending_exits:
            ts = p["ts"]
            f = feats.get(ts)
            exited = False
            if f is not None:
                j = f["d2i"].get(d)
                if j is not None:
                    row = f["rows"][j]
                    op = row["open"]
                    prev = row["pre_close"]
                    if prev is None and j > 0:
                        prev = f["rows"][j - 1]["close"]
                    if op is not None and op > 0:
                        if _at_limit_down(ts, prev, row["close"]) and (row["close"] is not None):
                            # limit-down close: assume cannot exit today, delay
                            p["delays"] = p.get("delays", 0) + 1
                            if p["delays"] > 5:
                                # force at close
                                ex_px = row["close"]
                                if ex_px and ex_px > 0:
                                    proceeds = p["shares"] * ex_px * (1 - COSTS_ROUNDTRIP)
                                    cash += proceeds
                                    net = ex_px * (1 - COSTS_ROUNDTRIP) / p["entry_px"] - 1
                                    trades.append({**p, "exit_date": d, "exit_px": ex_px,
                                                   "net": net, "forced": True})
                                    exited = True
                            else:
                                still_pending.append(p)
                                continue
                        else:
                            ex_px = op
                            proceeds = p["shares"] * ex_px * (1 - COSTS_ROUNDTRIP)
                            cash += proceeds
                            net = ex_px * (1 - COSTS_ROUNDTRIP) / p["entry_px"] - 1
                            trades.append({**p, "exit_date": d, "exit_px": ex_px,
                                           "net": net, "forced": False})
                            exited = True
                            continue
            if not exited:
                # symbol missing today: keep pending (suspended), count delay
                p["delays"] = p.get("delays", 0) + 1
                if p["delays"] > 5:
                    # give up: mark at last close
                    f2 = feats.get(p["ts"])
                    ex_px = None
                    if f2 is not None:
                        jj = f2["d2i"].get(d)
                        if jj is not None and f2["rows"][jj]["close"]:
                            ex_px = f2["rows"][jj]["close"]
                    if ex_px:
                        proceeds = p["shares"] * ex_px * (1 - COSTS_ROUNDTRIP)
                        cash += proceeds
                        trades.append({**p, "exit_date": d, "exit_px": ex_px,
                                       "net": ex_px * (1 - COSTS_ROUNDTRIP) / p["entry_px"] - 1,
                                       "forced": True})
                    else:
                        still_pending.append(p)
                else:
                    still_pending.append(p)
        pending_exits = still_pending
        # 2) evaluate exits for open positions at today close -> move to pending
        new_pending = []
        keep = []
        for p in positions:
            f = feats.get(p["ts"])
            move_out = False
            reason = None
            if f is not None:
                j = f["d2i"].get(d)
                if j is not None:
                    p["hold_days"] = p.get("hold_days", 0) + 1
                    if p["hold_days"] >= MAX_HOLD_DAYS:
                        reason = "max_hold"
                        move_out = True
                    else:
                        r = _exit_reason_at(f, j)
                        if r:
                            reason = r
                            move_out = True
                else:
                    p["hold_days"] = p.get("hold_days", 0)
            if move_out:
                p["exit_reason"] = reason
                p["flag_date"] = d
                p["delays"] = 0
                new_pending.append(p)
            else:
                keep.append(p)
        positions = keep
        pending_exits.extend(new_pending)
        # 3) entries: yesterday signals -> today open
        # find prev trading day in cal_all
        k = cal_idx.get(d)
        prev_d = cal_all[k - 1] if k is not None and k > 0 else None
        if prev_d is not None and prev_d in signals:
            cands = signals[prev_d]
            # faithful order: tightest basing first, pop before crossback, then ts
            cands = sorted(cands, key=lambda x: (0 if x[1] == "pop" else 1, x[2], x[0]))
            for ts, kind, _tight in cands:
                if len(positions) + len(pending_exits) >= MAX_POS:
                    break
                if any(p["ts"] == ts for p in positions) or any(p["ts"] == ts for p in pending_exits):
                    continue
                f = feats.get(ts)
                if f is None:
                    continue
                j = f["d2i"].get(d)
                if j is None:
                    continue
                row = f["rows"][j]
                op = row["open"]
                if op is None or op <= 0:
                    continue
                prev = row["pre_close"]
                if prev is None and j > 0:
                    prev = f["rows"][j - 1]["close"]
                if _at_limit_up(ts, prev, op):
                    continue
                # NAV at entry (MTM before trade)
                nav_now = cash + sum(
                    q["shares"] * (
                        feats[q["ts"]]["rows"][feats[q["ts"]]["d2i"][d]]["close"]
                        if feats.get(q["ts"]) is not None and feats[q["ts"]]["d2i"].get(d) is not None
                        and feats[q["ts"]]["rows"][feats[q["ts"]]["d2i"][d]]["close"] else q["entry_px"]
                    ) for q in positions
                )
                alloc = nav_now * POSITION_PCT
                if alloc > cash:
                    continue
                shares = alloc / op
                cash -= alloc
                positions.append({"ts": ts, "shares": shares, "entry_px": op,
                                  "entry_date": d, "kind": kind, "hold_days": 0})
                n_entries += 1
        # 4) MTM NAV
        mtm = cash
        for p in positions:
            f = feats.get(p["ts"])
            px = p["entry_px"]
            if f is not None:
                j = f["d2i"].get(d)
                if j is not None and f["rows"][j]["close"]:
                    px = f["rows"][j]["close"]
            mtm += p["shares"] * px
        # pending exits still economically held until exit; value at last close
        for p in pending_exits:
            f = feats.get(p["ts"])
            px = p["entry_px"]
            if f is not None:
                # use flag-date close or today close
                j = f["d2i"].get(d)
                if j is not None and f["rows"][j]["close"]:
                    px = f["rows"][j]["close"]
            mtm += p["shares"] * px
        nav_series.append((d, mtm))
    # force close leftovers at last close
    last_d = cal[-1] if cal else wend
    for p in positions + pending_exits:
        f = feats.get(p["ts"])
        ex_px = None
        if f is not None:
            j = f["d2i"].get(last_d)
            if j is not None:
                ex_px = f["rows"][j]["close"]
        if ex_px and ex_px > 0:
            net = ex_px * (1 - COSTS_ROUNDTRIP) / p["entry_px"] - 1
            trades.append({**p, "exit_date": last_d, "exit_px": ex_px, "net": net,
                           "exit_reason": p.get("exit_reason", "end_of_window"), "forced": True})
    # metrics
    import math
    navs = [v for _, v in nav_series]
    total = navs[-1] / navs[0] - 1 if navs and navs[0] else 0.0
    peak = -1e18
    maxdd = 0.0
    for v in navs:
        peak = max(peak, v)
        if peak > 0:
            maxdd = min(maxdd, v / peak - 1)
    rets = [(navs[i] / navs[i - 1] - 1) for i in range(1, len(navs))] if len(navs) > 1 else []
    if len(rets) > 1 and float(sum(x * x for x in rets) / len(rets) - (sum(rets) / len(rets)) ** 2) > 0:
        mu = sum(rets) / len(rets)
        var = sum((x - mu) ** 2 for x in rets) / len(rets)
        sharpe = mu / math.sqrt(var) * math.sqrt(252) if var > 0 else 0.0
    else:
        sharpe = 0.0
    closed = [t for t in trades if "net" in t]
    n = len(closed)
    wins = sum(1 for t in closed if t["net"] > 0)
    winrate = wins / n if n else 0.0
    avg = sum(t["net"] for t in closed) / n if n else 0.0
    worst = min((t["net"] for t in closed), default=0.0)
    avg_hold = sum(t.get("hold_days", 0) for t in closed) / n if n else 0.0
    nday = len(cal) or 1
    pops = sum(1 for t in closed if t.get("kind") == "pop")
    cbs = sum(1 for t in closed if t.get("kind") == "crossback")
    reasons: dict[str, int] = defaultdict(int)
    for t in closed:
        reasons[t.get("exit_reason", "?")] += 1
    return {
        "window": wname, "start": wstart, "end": wend,
        "trading_days": nday, "n_signals": n_signals,
        "signals_per_day": round(n_signals / nday, 3),
        "n_entries": n_entries, "n_trades": n,
        "total_nav": round(total * 100, 2), "sharpe": round(sharpe, 2),
        "maxdd": round(maxdd * 100, 2),
        "winrate": round(winrate * 100, 1), "avg_net_pct": round(avg * 100, 2),
        "worst_pct": round(worst * 100, 2), "avg_hold_days": round(avg_hold, 1),
        "n_pop": pops, "n_crossback": cbs, "exit_reasons": dict(reasons),
        "end_nav": round(navs[-1], 4) if navs else 1.0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--a-only", action="store_true",
                    help="A-share-only universe (ex HK/ETF). v1 default off (frozen).")
    args = ap.parse_args()
    names = [w.strip() for w in args.windows.split(",") if w.strip() in WINDOWS]
    out: dict = {"params": {"position_pct": POSITION_PCT, "max_pos": MAX_POS,
                            "costs": COSTS_ROUNDTRIP, "tight": TIGHT_PCT,
                            "pop_trigger": POP_TRIGGER, "exhaust": EXHAUST_PCT,
                            "max_hold": MAX_HOLD_DAYS, "min_avg_yi": MIN_AVG_AMOUNT_YI,
                            "a_only": bool(args.a_only)},
                 "windows": {}}
    for w in names:
        s, e = WINDOWS[w]
        print(f"[{w}] {s}..{e} loading... (a_only={args.a_only})", flush=True)
        r = replay_window(w, s, e, a_only=args.a_only)
        out["windows"][w] = r
        print(f"[{w}] NAV {r['total_nav']}% sr{r['sharpe']} dd{r['maxdd']}% "
              f"n{r['n_trades']} win{r['winrate']}% avg{r['avg_net_pct']}% "
              f"worst{r['worst_pct']}% hold{r['avg_hold_days']}d "
              f"sig/d{r['signals_per_day']} pop{r['n_pop']}/cb{r['n_crossback']} "
              f"exits{r['exit_reasons']}", flush=True)
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        dest = REPORT_DIR / ("cpa_cn_v1a_aonly.json" if args.a_only else "cpa_cn_v1.json")
        dest.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved {dest}")


if __name__ == "__main__":
    main()
