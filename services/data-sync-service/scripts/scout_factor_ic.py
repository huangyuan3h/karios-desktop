#!/usr/bin/env python3
"""Scout factor IC screening — independent of S-3 score/RS.

4 families (institution-neglected) on 20-80亿 small-mid universe:
  liquidity microstructure, small-cap structure, short-wave behavior
  Metrics: IC mean/std/IR + quintile monotonicity per window.
  Windows: OOS2/train/valid + holdout read-only.
  Universe: total_mv 20-80亿 (stock_dailybasic) AND optional amount filter.

Usage:
  PYTHONPATH=src python3 scripts/scout_factor_ic.py --windows OOS2,train,valid
  PYTHONPATH=src python3 scripts/scout_factor_ic.py --windows OOS2 --horizons 5,10
"""
from __future__ import annotations

import argparse
import json
import sys
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
    "holdout": ("2026-08-08", "2027-02-08"),
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

# mv in 亿元 (total_mv /10000)
UNIVERSE_MIN_MV = 20.0
UNIVERSE_MAX_MV = 80.0

HORIZONS = [5, 10]


def _load_mv_map(start: str, end: str) -> dict[str, dict[str, float]]:
    """{trade_date: {ts_code: total_mv亿元}}"""
    # need lookback for universe? universe is per day as-of mv
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, ts_code, total_mv FROM stock_dailybasic
                WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL
                """,
                (start, end),
            )
            rows = cur.fetchall()
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for d, ts, mv in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        try:
            v = float(mv) / 10000.0
        except Exception:
            continue
        out[ds][str(ts)] = v
    return out


def _load_daily(start: str, end: str):
    """Load daily OHLCV for window with lookback/forward buffers."""
    # buffer: 60d lookback for avg20, 10d forward for 10d return
    s = max(date.fromisoformat(start) - timedelta(days=90), date(1998, 1, 1)).isoformat()
    e = (date.fromisoformat(end) + timedelta(days=20)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, ts_code, open, high, low, close, pre_close, amount
                FROM daily
                WHERE trade_date >= %s AND trade_date <= %s
                ORDER BY ts_code, trade_date
                """,
                (s, e),
            )
            rows = cur.fetchall()
    # organize per ts
    from collections import defaultdict

    per_ts: dict[str, list[dict]] = defaultdict(list)
    for d, ts, o, h, l, c, pc, amt in rows:
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts[str(ts)].append(
            {
                "date": ds,
                "open": float(o) if o is not None else None,
                "high": float(h) if h is not None else None,
                "low": float(l) if l is not None else None,
                "close": float(c) if c is not None else None,
                "pre_close": float(pc) if pc is not None else None,
                "amount": float(amt) if amt is not None else None,
            }
        )
    return per_ts


def _factors_for_day(series_by_ts: dict[str, list[dict]], day: str, mv_by_day: dict[str, float]):
    """Compute factor values for one day, universe filtered."""
    # need to find bars up to day
    out: dict[str, dict[str, float]] = {}
    for ts, series in series_by_ts.items():
        # binary search for day index
        # series sorted asc
        idx = -1
        for i, r in enumerate(series):
            if r["date"] == day:
                idx = i
                break
            if r["date"] > day:
                break
        if idx < 0:
            continue
        mv = mv_by_day.get(ts)
        if mv is None or not (UNIVERSE_MIN_MV <= mv <= UNIVERSE_MAX_MV):
            continue
        # need history for lookbacks
        if idx < 20:
            continue
        cur = series[idx]
        # basic sanity
        if not cur["close"] or cur["close"] <= 0:
            continue
        # amount avg20
        amts = [r["amount"] for r in series[idx - 20 : idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        amt = cur["amount"] or 0
        turnover_spike = (amt / avg20) if avg20 and avg20 > 0 else np.nan
        # amplitude
        high = cur["high"] or cur["close"]
        low = cur["low"] or cur["close"]
        amplitude = (high - low) / cur["close"] if cur["close"] else np.nan
        # gap
        prev_close = series[idx - 1]["close"] if idx > 0 else None
        gap = (cur["open"] / prev_close - 1) if cur["open"] and prev_close and prev_close > 0 else np.nan
        # ret1, ret5
        ret1 = (cur["close"] / prev_close - 1) if prev_close and prev_close > 0 else np.nan
        c5 = series[idx - 5]["close"] if idx >= 5 else None
        ret5 = (cur["close"] / c5 - 1) if c5 and c5 > 0 else np.nan
        # 5d max drawdown-like: (close - max_high_5)/max_high_5
        highs5 = [r["high"] for r in series[idx - 5 : idx + 1] if r["high"]]
        max_high5 = max(highs5) if highs5 else np.nan
        dist_high5 = (cur["close"] - max_high5) / max_high5 if max_high5 and max_high5 > 0 else np.nan
        # consecutive down days (0..5)
        down_cnt = 0
        for k in range(5):
            j = idx - k
            if j <= 0:
                break
            if series[j]["close"] and series[j - 1]["close"] and series[j]["close"] < series[j - 1]["close"]:
                down_cnt += 1
            else:
                break
        # circ mv rank proxy: use total_mv inversely? smaller = higher factor
        # we emit -mv so that small cap gets high rank
        neg_mv = -mv
        factors = {
            "turnover_spike": turnover_spike,
            "amplitude": amplitude,
            "gap": gap,
            "ret1": ret1,
            "ret5": ret5,
            "dist_high5": dist_high5,
            "down_cnt": float(down_cnt),
            "neg_mv": neg_mv,
            # also raw mv for diagnostics
            "mv": mv,
        }
        # filter nan
        clean = {k: float(v) for k, v in factors.items() if v is not None and not (isinstance(v, float) and np.isnan(v))}
        if len(clean) < 6:
            continue
        out[ts] = clean
    return out


def _forward_returns(series_by_ts: dict[str, list[dict]], horizon: int) -> dict[str, dict[str, float]]:
    """{date: {ts: fwd_ret_h}} for dates where fwd is knowable."""
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for ts, series in series_by_ts.items():
        # build date->idx
        date_to_idx = {r["date"]: i for i, r in enumerate(series)}
        for d, idx in date_to_idx.items():
            j = idx + horizon
            if j >= len(series):
                continue
            c0 = series[idx]["close"]
            c1 = series[j]["close"]
            if not c0 or not c1 or c0 <= 0:
                continue
            ret = c1 / c0 - 1
            if abs(ret) > 5:  # filter absurd jumps (e.g., 500%)
                continue
            out[d][ts] = ret
    return out


def _spearman(x: np.ndarray, y: np.ndarray) -> float:
    # rank correlation
    if len(x) < 10:
        return np.nan
    # rank via argsort
    rx = np.argsort(np.argsort(x))
    ry = np.argsort(np.argsort(y))
    # Pearson on ranks
    xm = rx - rx.mean()
    ym = ry - ry.mean()
    denom = np.sqrt((xm * xm).sum() * (ym * ym).sum())
    if denom == 0:
        return np.nan
    return float((xm * ym).sum() / denom)


def _quintile_returns(factor_vals: np.ndarray, fwd_vals: np.ndarray):
    # sort by factor ascending, split 5
    n = len(factor_vals)
    if n < 20:
        return None
    order = np.argsort(factor_vals)
    fv_sorted = fwd_vals[order]
    q = n // 5
    rets = []
    for i in range(5):
        lo = i * q
        hi = (i + 1) * q if i < 4 else n
        rets.append(float(np.nanmean(fv_sorted[lo:hi])))
    return rets


def run_window(wname: str, horizons: list[int]):
    start, end = WINDOWS[wname]
    print(f"[{wname}] {start}..{end} — loading...", flush=True)
    per_ts = _load_daily(start, end)
    mv_map = _load_mv_map(start, end)
    # calendar from daily distinct dates within window
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date", (start, end))
            cal = [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]
    print(f"[{wname}] cal {len(cal)}  per_ts {len(per_ts)}", flush=True)
    fwd_by_h = {h: _forward_returns(per_ts, h) for h in horizons}
    # factor names
    factor_names = ["turnover_spike", "amplitude", "gap", "ret1", "ret5", "dist_high5", "down_cnt", "neg_mv"]
    results: dict = {"window": wname, "start": start, "end": end, "factors": {}}
    for fname in factor_names:
        results["factors"][fname] = {}
        for h in horizons:
            ics = []
            q_sum = np.zeros(5)
            q_cnt = 0
            q_example = None
            for day in cal:
                fmap = _factors_for_day(per_ts, day, mv_map.get(day, {}))
                if not fmap:
                    continue
                fwd_map = fwd_by_h[h].get(day)
                if not fwd_map:
                    continue
                # intersect
                common = set(fmap.keys()) & set(fwd_map.keys())
                if len(common) < 20:
                    continue
                xs = np.array([fmap[ts][fname] for ts in common if fname in fmap[ts]], dtype=float)
                ys = np.array([fwd_map[ts] for ts in common if fname in fmap[ts]], dtype=float)
                # filter nan/inf
                mask = np.isfinite(xs) & np.isfinite(ys)
                xs = xs[mask]
                ys = ys[mask]
                if len(xs) < 20:
                    continue
                # winsorize xs at 1/99
                lo, hi = np.percentile(xs, [1, 99])
                xs = np.clip(xs, lo, hi)
                ic = _spearman(xs, ys)
                if np.isfinite(ic):
                    ics.append(ic)
                qr = _quintile_returns(xs, ys)
                if qr is not None:
                    q_sum += np.array(qr)
                    q_cnt += 1
                    if q_example is None:
                        q_example = qr
            ics_arr = np.array(ics) if ics else np.array([])
            ic_mean = float(np.nanmean(ics_arr)) if len(ics_arr) else float("nan")
            ic_std = float(np.nanstd(ics_arr)) if len(ics_arr) else float("nan")
            ic_ir = float(ic_mean / ic_std) if ic_std and ic_std != 0 and np.isfinite(ic_mean) else float("nan")
            hit = float(np.mean(np.array(ics_arr) > 0)) if len(ics_arr) else float("nan")
            q_avg = (q_sum / q_cnt).tolist() if q_cnt else None
            # monotonicity: Q5 - Q1
            spread = (q_avg[4] - q_avg[0]) if q_avg else float("nan")
            mono = None
            if q_avg:
                # check monotonic increasing (if spread positive, expect Q1<...<Q5)
                mono = all(q_avg[i] <= q_avg[i+1] + 1e-9 for i in range(4)) or all(q_avg[i] >= q_avg[i+1] -1e-9 for i in range(4))
            results["factors"][fname][f"h{h}"] = {
                "n_days": len(ics_arr),
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "ic_ir": ic_ir,
                "hit_rate": hit,
                "q_avg": q_avg,
                "spread_Q5_Q1": spread,
                "monotonic": mono,
            }
            print(f"  {fname:15s} h{h:2d} IC {ic_mean:+.3f} IR {ic_ir:+.2f} spread {spread:+.4f} mono {mono} n{len(ics_arr)} q {q_avg}", flush=True)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid", help="comma windows")
    ap.add_argument("--horizons", default="5,10", help="comma horizons")
    ap.add_argument("--json", default="", help="output json path")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]
    horizons = [int(x) for x in args.horizons.split(",") if x.strip()]
    all_res = {}
    for w in wins:
        if w not in WINDOWS:
            print(f"unknown window {w}", file=sys.stderr)
            sys.exit(2)
        all_res[w] = run_window(w, horizons)
    out_path = Path(args.json) if args.json else REPORT_DIR / "scout_factor_ic_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"generated_at": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(), "windows": wins, "horizons": horizons, "universe": f"{UNIVERSE_MIN_MV}-{UNIVERSE_MAX_MV}亿", "results": all_res}
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out_path}")
    # also print summary table
    print("\n| factor | window | h | IC | IR | spread | mono |")
    print("|--------|--------|---|----|----|--------|------|")
    for w in wins:
        for fname, hdict in all_res[w]["factors"].items():
            for h in horizons:
                d = hdict[f"h{h}"]
                print(f"| {fname:12s} | {w:6s} | {h} | {d['ic_mean']:+.3f} | {d['ic_ir']:+.2f} | {d['spread_Q5_Q1']:+.4f} | {d['monotonic']} |")


if __name__ == "__main__":
    raise SystemExit(main())
