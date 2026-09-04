#!/usr/bin/env python3
"""Industry x style IC screening — per-industry amplitude/turnover_spike IC (I1).

Universe: ALL A shares (no mv pool), grouped by stock_basic.industry.
Factor: amplitude (10d), turnover_spike. Horizon 5/10. Three windows.
Output: data/backtest_reports/industry_ic_latest.json
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from data_sync_service.db import get_connection  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
HORIZONS = [5, 10]


def _load_industry():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code, industry FROM stock_basic WHERE delist_date IS NULL")
            return {str(r[0]): (r[1] or "UNKNOWN") for r in cur.fetchall()}


def _load_mv_map(start, end):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, total_mv FROM stock_dailybasic WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL", (start, end))
            rows = cur.fetchall()
    out = defaultdict(dict)
    for d, ts, mv in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        try:
            out[ds][str(ts)] = float(mv) / 10000.0
        except Exception:
            continue
    return out


def _load_daily(start, end):
    s = max(date.fromisoformat(start) - timedelta(days=90), date(1998, 1, 1)).isoformat()
    e = (date.fromisoformat(end) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT trade_date, ts_code, open, high, low, close, pre_close, amount FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY ts_code, trade_date", (s, e))
            rows = cur.fetchall()
    per_ts = defaultdict(list)
    for d, ts, o, h, l, c, pc, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts[str(ts)].append({"date": ds, "open": float(o) if o is not None else None, "high": float(h) if h is not None else None, "low": float(l) if l is not None else None, "close": float(c) if c is not None else None, "pre_close": float(pc) if pc is not None else None, "amount": float(amt) if amt is not None else None})
    return per_ts


def _factors_for_day(per_ts, day, mv_by_day):
    out = {}
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
        mv = mv_by_day.get(ts)
        if mv is None:
            continue
        cur = series[idx]
        if not cur["close"] or cur["close"] <= 0:
            continue
        amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        amt = cur["amount"] or 0
        turnover_spike = (amt / avg20) if avg20 and avg20 > 0 else np.nan
        high = cur["high"] or cur["close"]; low = cur["low"] or cur["close"]
        amplitude = (high - low) / cur["close"] if cur["close"] else np.nan
        out[ts] = {"amplitude": amplitude, "turnover_spike": turnover_spike}
    return out


def _forward_returns(per_ts, horizon):
    out = defaultdict(dict)
    for ts, series in per_ts.items():
        d2i = {r["date"]: i for i, r in enumerate(series)}
        for d, idx in d2i.items():
            j = idx + horizon
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


def run_window(wname, industry_map, horizons):
    start, end = WINDOWS[wname]
    print(f"[{wname}] loading...", flush=True)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (start, end))
            cal = [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]
    fwd_by_h = {h: _forward_returns(per_ts, h) for h in horizons}
    factor_names = ["amplitude", "turnover_spike"]
    # per-industry accumulators: {industry: {factor: {h: [ics]}}}
    acc = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    n_stocks = defaultdict(int)
    for day in cal:
        fmap = _factors_for_day(per_ts, day, mv_map.get(day, {}))
        if not fmap:
            continue
        for h in horizons:
            fwd = fwd_by_h[h].get(day)
            if not fwd:
                continue
            for fname in factor_names:
                # group by industry for this day
                day_by_ind = defaultdict(dict)
                for ts, fv in fmap.items():
                    ind = industry_map.get(ts, "UNKNOWN")
                    if fname in fv and np.isfinite(fv[fname]):
                        day_by_ind[ind][ts] = fv[fname]
                for ind, xmap in day_by_ind.items():
                    common = set(xmap.keys()) & set(fwd.keys())
                    if len(common) < 10:
                        continue
                    xs = np.array([xmap[ts] for ts in common], dtype=float)
                    ys = np.array([fwd[ts] for ts in common], dtype=float)
                    ic = _spearman(xs, ys)
                    if np.isfinite(ic):
                        acc[ind][fname][h].append(ic)
        # count stocks per industry
        for ts in fmap:
            n_stocks[industry_map.get(ts, "UNKNOWN")] += 1
    results = {"window": wname, "factors": {}}
    for ind, fdict in acc.items():
        results["factors"][ind] = {"n_stocks_total": n_stocks.get(ind, 0)}
        for fname in factor_names:
            results["factors"][ind][fname] = {}
            for h in horizons:
                ics = np.array(fdict[fname][h])
                if len(ics) == 0:
                    continue
                ic_mean = float(np.nanmean(ics)); ic_std = float(np.nanstd(ics))
                ir = ic_mean / ic_std if ic_std and np.isfinite(ic_std) and ic_std > 0 else float("nan")
                results["factors"][ind][fname][f"h{h}"] = {"n_days": len(ics), "ic_mean": ic_mean, "ic_std": ic_std, "ic_ir": ir, "hit_rate": float(np.mean(ics > 0))}
    return results


def main():
    wins = ["OOS2", "train", "valid"]
    industry_map = _load_industry()
    print(f"industry map {len(industry_map)}", flush=True)
    all_res = {}
    for w in wins:
        all_res[w] = run_window(w, industry_map, HORIZONS)
    out = REPORT_DIR / "industry_ic_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"windows": wins, "horizons": HORIZONS, "results": all_res}, ensure_ascii=False, indent=2, default=str))
    # print summary: per industry amplitude IR across windows
    print("\n| industry | n | OOS2 IR | train IR | valid IR | 三窗同号 |")
    print("|----------|---|---------|----------|----------|----------|")
    for ind, fdict in all_res["valid"]["factors"].items():
        if "amplitude" not in fdict:
            continue
        irs = []
        for w in wins:
            d = all_res[w]["factors"].get(ind, {}).get("amplitude")
            if d:
                irs.append(d.get("h10", {}).get("ic_ir"))
        irs = [x for x in irs if x is not None and np.isfinite(x)]
        if len(irs) < 3:
            continue
        same = all(x < 0 for x in irs) or all(x > 0 for x in irs)
        n = fdict.get("n_stocks_total", 0)
        print(f"| {ind:10s} | {n} | {irs[0]:+.2f} | {irs[1]:+.2f} | {irs[2]:+.2f} | {same} |")
    print(f"report -> {out}")


if __name__ == "__main__":
    raise SystemExit(main())
