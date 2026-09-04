#!/usr/bin/env python3
"""Dynamic core-satellite (v1): 每月再平衡 + 各自趋势门控 (play-to-strength / cut-weakness).

Pick-strong daily NAV from `pick_strong_grid.build_nav_from_cache(trail_pct=8.0)` (E1).
State-bucket daily NAV from its own simulate. Aligned on CN calendar.
Rule: at each month start, a leg is "active" if its trailing 60-trading-day return > 0;
allocate equally among active legs (REPO if none). Hold weights for the month.
Compared vs fixed 50/50 and each standalone. No look-ahead (trailing return only).
"""
import json, sys
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pick_strong_grid import warm_window, build_nav_from_cache, fetch_etf_closes
from scout_state_bucket_pickstrong import (_load_daily, _load_mv_map, _load_list_dates,
    simulate_state_bucket, stats, _load_calendar)

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}
LOOKBACK = 60


def dynamic_nav(cal, pk_nav, sb_nav):
    pk_r = [0.0] + [pk_nav[i] / pk_nav[i - 1] - 1 for i in range(1, len(pk_nav)) if pk_nav[i - 1] > 0]
    sb_r = [0.0] + [sb_nav[i] / sb_nav[i - 1] - 1 for i in range(1, len(sb_nav)) if sb_nav[i - 1] > 0]
    nav = [1.0]
    weights = (0.5, 0.5)
    for i in range(1, len(cal)):
        m = cal[i][:7]
        if cal[i - 1][:7] != m:
            lo = max(0, i - LOOKBACK)
            pk_ret = pk_nav[i - 1] / pk_nav[lo] - 1 if pk_nav[lo] > 0 else -1
            sb_ret = sb_nav[i - 1] / sb_nav[lo] - 1 if sb_nav[lo] > 0 else -1
            active = [pk_ret > 0, sb_ret > 0]
            n = sum(active)
            if n == 0:
                weights = (0.0, 0.0)
            elif n == 1:
                weights = (1.0, 0.0) if active[0] else (0.0, 1.0)
            else:
                weights = (0.5, 0.5)
        dr = weights[0] * pk_r[i] + weights[1] * sb_r[i]
        nav.append(nav[-1] * (1 + dr))
    return nav, weights


def main():
    etf_close = fetch_etf_closes()
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    out = {}
    for wname, (s, e) in WINDOWS.items():
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
                                 score="mom", top2=False, trail_pct=8.0)
        pk_nav_map = r["nav"]
        cal = _load_calendar(s, e)
        sb_nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx)
        pk_aligned = []
        last = 1.0
        for d in cal:
            v = pk_nav_map.get(d)
            if v is not None:
                last = v
            pk_aligned.append(last)
        dyn_nav_series, _ = dynamic_nav(cal, pk_aligned, sb_nav)
        out[wname] = {
            "pick_strong_trail8": stats(cal, pk_aligned),
            "state_bucket": stats(cal, sb_nav),
            "dynamic": stats(cal, dyn_nav_series),
        }
        print(f"=== {wname} ===")
        for k in ("pick_strong_trail8", "state_bucket", "dynamic"):
            st = out[wname][k]
            print(f"  {k:22s}: CAGR {st['cagr']:+.1f}%  dd {st['max_dd']:.1f}  sr {st['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/core_satellite_dynamic_latest.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())
