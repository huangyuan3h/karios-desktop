#!/usr/bin/env python3
"""R1 state IC: per-state-bucket factor IC. 6 states x 4 factors x h5/h10.

States computed daily (cross-sectional): limit-up, gap>3%, shrink low-vol low-turn, breakout new-high, fresh<250d, stress high turnover.
Select factor using OOS2+train only; valid is verification only.
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
FACTORS = ["amplitude", "turnover_spike", "gap", "ret1"]
STATES = ["S-limit", "S-gap", "S-shrink", "S-breakout", "S-fresh", "S-stress", "S-all"]


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


def _load_list_dates():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, list_date FROM stock_basic")
            return {str(r[0]): r[1] for r in cur.fetchall()}


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


def _forward_returns(per_ts, h):
    out = defaultdict(dict)
    for ts, series in per_ts.items():
        d2i = {r["date"]: i for i, r in enumerate(series)}
        for d, idx in d2i.items():
            j = idx + h
            if j >= len(series):
                continue
            c0 = series[idx]["close"]; c1 = series[j]["close"]
            if not c0 or not c1 or c0 <= 0:
                continue
            ret = c1 / c0 - 1
            if abs(ret) > 5:
                continue
            out[d][ts] = ret
    return out


def _spearman(x, y):
    if len(x) < 10:
        return np.nan
    rx = np.argsort(np.argsort(x)); ry = np.argsort(np.argsort(y))
    xm = rx - rx.mean(); ym = ry - ry.mean()
    den = np.sqrt((xm * xm).sum() * (ym * ym).sum())
    return float((xm * ym).sum() / den) if den else np.nan


def run_window(wname, list_dates):
    s, e = WINDOWS[wname]
    per_ts = _load_daily(s, e)
    mv_map = _load_mv_map(s, e)
    cal = _load_calendar(s, e)
    fwd_by_h = {h: _forward_returns(per_ts, h) for h in [5, 10]}
    # per-state per-factor per-h IC lists
    acc = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for day in cal:
        # state + factor computation for day
        state_map = defaultdict(dict)  # state -> ts -> {factor: val}
        # need cross-sectional thresholds: amplitude Q10, turnover Q30, amount>avg20x2
        day_amp = {}
        day_turn = {}
        day_all = {}
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
            if ts not in mv_map.get(day, {}):
                continue  # only A stocks with mv
            cur = series[idx]
            if not cur["close"] or not cur["high"] or not cur["low"] or cur["close"] <= 0:
                continue
            pc = series[idx - 1]["close"] if idx > 0 else None
            ret1 = (cur["close"] / pc - 1) if pc and pc > 0 else np.nan
            gap = (cur["open"] / pc - 1) if cur["open"] and pc and pc > 0 else np.nan
            amp = (cur["high"] - cur["low"]) / cur["close"]
            amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
            if len(amts) < 15:
                continue
            avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
            turn_spike = (cur["amount"] / avg20) if avg20 and avg20 > 0 else np.nan
            # 20d high
            highs20 = [r["high"] for r in series[idx - 19: idx + 1] if r["high"]]
            new_high = (cur["close"] >= max(highs20)) if highs20 else False
            # list age
            ld = list_dates.get(ts)
            fresh = False
            if ld:
                try:
                    ld_d = ld if hasattr(ld, "strftime") else date.fromisoformat(str(ld)[:10])
                    # count trading days via series positions
                    if idx < 250:
                        fresh = True
                except Exception:
                    fresh = False
            day_amp[ts] = amp
            day_turn[ts] = turn_spike
            fv = {"amplitude": amp, "turnover_spike": turn_spike, "gap": gap, "ret1": ret1}
            # assign state(s)
            states = []
            is_limit = False
            if pc and pc > 0 and cur["close"]:
                # limit-up approx: close >= pre_close*1.095 for main board (ignore 20% boards for now)
                lim = 1.095 if ts.endswith((".SH", ".SZ")) and (ts.startswith("60") or ts.startswith("00")) else 1.195
                is_limit = cur["close"] >= pc * lim - 1e-6
            if is_limit:
                states.append("S-limit")
            if gap and gap > 0.03:
                states.append("S-gap")
            if new_high and turn_spike and turn_spike > 2:
                states.append("S-breakout")
            if fresh:
                states.append("S-fresh")
            # shrink: low amp + low turn, needs cross-sectional Q10/Q30
            # defer shrink/stress to after collecting
            day_all[ts] = {"fv": fv, "states": states, "amp": amp, "turn": turn_spike}
        # cross-sectional shrink Q10 and turn Q30
        if len(day_amp) > 30:
            amp_thresh = np.percentile(sorted(day_amp.values()), 10)
            turn_thresh = np.percentile(sorted(day_turn.values()), 30)
            for ts, d in day_all.items():
                if d["amp"] <= amp_thresh and d["turn"] is not None and d["turn"] <= turn_thresh:
                    d["states"].append("S-shrink")
                if d["turn"] is not None and d["turn"] > 2 and d["amp"] and d["amp"] > np.percentile(sorted(day_amp.values()), 70):
                    d["states"].append("S-stress")
            for ts in day_all:
                day_all[ts]["states"].append("S-all")
            # compute per-state IC per factor
            for fname in FACTORS:
                for h in [5, 10]:
                    fwd = fwd_by_h[h].get(day)
                    if not fwd:
                        continue
                    by_state = defaultdict(dict)
                    for ts, d in day_all.items():
                        v = d["fv"].get(fname)
                        if v is None or not np.isfinite(v):
                            continue
                        for st in d["states"]:
                            by_state[st][ts] = v
                    for st, xmap in by_state.items():
                        common = set(xmap.keys()) & set(fwd.keys())
                        if len(common) < 15:
                            continue
                        xs = np.array([xmap[ts] for ts in common], dtype=float)
                        ys = np.array([fwd[ts] for ts in common], dtype=float)
                        ic = _spearman(xs, ys)
                        if np.isfinite(ic):
                            acc[st][fname][h].append(ic)
    results = {"window": wname}
    for st in acc:
        results[st] = {}
        for fname in FACTORS:
            results[st][fname] = {}
            for h in [5, 10]:
                ics = np.array(acc[st][fname][h])
                if len(ics) == 0:
                    continue
                ic_mean = float(np.nanmean(ics)); ic_std = float(np.nanstd(ics))
                ir = ic_mean / ic_std if ic_std and ic_std > 0 else float("nan")
                results[st][fname][f"h{h}"] = {"n_days": len(ics), "ic_mean": ic_mean, "ic_ir": ir, "hit_rate": float(np.mean(ics > 0))}
    return results


def main():
    list_dates = _load_list_dates()
    all_res = {}
    for w in WINDOWS:
        print(f"[{w}] ...", flush=True)
        all_res[w] = run_window(w, list_dates)
    out = REPORT_DIR / "state_ic_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"windows": list(WINDOWS.keys()), "factors": FACTORS, "states": STATES, "results": all_res}, ensure_ascii=False, indent=2, default=str))
    # print h10 summary per state per factor (OOS2+train select, valid verify)
    print("\n| state | factor | OOS2 IR | train IR | valid IR | select-same | valid-ok |")
    print("|-------|--------|---------|----------|----------|-------------|----------|")
    for st in STATES:
        for fname in FACTORS:
            def ir(w):
                d = all_res.get(w, {}).get(st, {}).get(fname, {}).get("h10", {})
                return d.get("ic_ir") if d else float("nan")
            irs = [ir("OOS2"), ir("train"), ir("valid")]
            sel = (irs[0] < 0 and irs[1] < 0) or (irs[0] > 0 and irs[1] > 0)
            vok = (irs[0] < 0 and irs[2] < 0) or (irs[0] > 0 and irs[2] > 0)
            print(f"| {st:10s} | {fname:15s} | {irs[0]:+.2f} | {irs[1]:+.2f} | {irs[2]:+.2f} | {sel} | {vok} |")
    print(f"report -> {out}")


if __name__ == "__main__":
    raise SystemExit(main())
