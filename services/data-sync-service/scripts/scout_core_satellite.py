#!/usr/bin/env python3
"""Core-satellite backtest (CORRECTED): 择强单轨 trail8 (core) + 状态分桶 (satellite).

Pick-strong daily NAV now from `pick_strong_grid.build_nav_from_cache` with trail_pct=8.0
(the canonical E1 / +190.7% config). State-bucket daily NAV from its own simulate. Aligned on
the CN daily calendar, blended at core/satellite weights, with daily-return correlation.
NOTE: pick-strong NAV here models 0 switch cost; state-bucket NAV has 0.3% round-trip.
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
        pk_nav = r["nav"]
        print(f"  [{wname}] 择强 trail8 fused={r['fusedPct']}% DD={r['maxDdFusedPct']}% trailExits={r['trailExits']}", flush=True)
        cal = _load_calendar(s, e)
        sb_nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx)
        pk_aligned = []
        last = 1.0
        for d in cal:
            v = pk_nav.get(d)
            if v is not None:
                last = v
            pk_aligned.append(last)
        pk_r = [pk_aligned[i] / pk_aligned[i - 1] - 1 for i in range(1, len(pk_aligned)) if pk_aligned[i - 1] > 0]
        sb_r = [sb_nav[i] / sb_nav[i - 1] - 1 for i in range(1, len(sb_nav)) if sb_nav[i - 1] > 0]
        corr = float(np.corrcoef(pk_r, sb_r)[0, 1]) if len(pk_r) > 2 else 0.0
        blends = {}
        for w in (0.8, 0.7, 0.5):
            nav = [1.0]
            for i in range(1, len(cal)):
                dr = w * (pk_aligned[i] / pk_aligned[i - 1] - 1) + (1 - w) * (sb_nav[i] / sb_nav[i - 1] - 1)
                nav.append(nav[-1] * (1 + dr))
            blends[str(w)] = stats(cal, nav)
        out[wname] = {"pick_strong_trail8": stats(cal, pk_aligned), "state_bucket": stats(cal, sb_nav), "corr": corr, "blends": blends, "pk_fusedPct": r["fusedPct"]}
        print(f"=== {wname} ({s}~{e}) ===")
        print(f"  择强核心(trail8): CAGR {out[wname]['pick_strong_trail8']['cagr']:+.1f}%  dd {out[wname]['pick_strong_trail8']['max_dd']:.1f}  sr {out[wname]['pick_strong_trail8']['sharpe']:.2f}")
        print(f"  状态分桶卫星    : CAGR {out[wname]['state_bucket']['cagr']:+.1f}%  dd {out[wname]['state_bucket']['max_dd']:.1f}  sr {out[wname]['state_bucket']['sharpe']:.2f}")
        print(f"  日收益相关 corr = {corr:.2f}")
        for w in (0.8, 0.7, 0.5):
            b = blends[str(w)]
            print(f"  组合 core{w}/sat{1-w}: CAGR {b['cagr']:+.1f}%  dd {b['max_dd']:.1f}  sr {b['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/core_satellite_trail8_latest.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())
