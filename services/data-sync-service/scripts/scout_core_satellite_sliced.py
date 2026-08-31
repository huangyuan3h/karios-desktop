#!/usr/bin/env python3
"""Core-satellite (R10): 择强 trail8 核心 + slice2 状态分桶卫星（按态切分版）。

卫星 NAV 复用 state_sliced_navs/{window}.json 的 slice2_LG；核心 NAV 现算
build_nav_from_cache(trail_pct=8.0)。权重 core/sat ∈ {80/20, 70/30, 50/50}。
"""
import json, sys
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pick_strong_grid import warm_window, build_nav_from_cache, fetch_etf_closes
from scout_state_bucket_pickstrong import stats, _load_calendar

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}
NAV_DIR = Path("data/backtest_reports/state_sliced_navs")


def main():
    etf_close = fetch_etf_closes()
    out = {}
    for wname, (s, e) in WINDOWS.items():
        cal = _load_calendar(s, e)
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
                                 score="mom", top2=False, trail_pct=8.0)
        pk_map = r["nav"]
        pk = []
        last = 1.0
        for d in cal:
            v = pk_map.get(d)
            if v is not None:
                last = v
            pk.append(last)
        sat = json.loads((NAV_DIR / f"{wname}.json").read_text())["slice2_LG"]
        n = min(len(pk), len(sat))
        pk, sat = pk[:n], sat[:n]
        pk_r = [pk[i] / pk[i - 1] - 1 for i in range(1, n)]
        sat_r = [sat[i] / sat[i - 1] - 1 for i in range(1, n)]
        corr = float(np.corrcoef(pk_r, sat_r)[0, 1]) if n > 2 else 0.0
        blends = {}
        for w in (0.8, 0.7, 0.5):
            nav = [1.0]
            for i in range(1, n):
                dr = w * pk_r[i - 1] + (1 - w) * sat_r[i - 1]
                nav.append(nav[-1] * (1 + dr))
            blends[str(w)] = stats(cal[:n], nav)
        out[wname] = {
            "pick_strong_trail8": stats(cal[:n], pk),
            "slice2_satellite": stats(cal[:n], sat),
            "corr": round(corr, 3),
            "blends": blends,
        }
        print(f"=== {wname} ===")
        print(f"  择强核心(trail8): CAGR {out[wname]['pick_strong_trail8']['cagr']:+.1f}%  dd {out[wname]['pick_strong_trail8']['max_dd']:.1f}  sr {out[wname]['pick_strong_trail8']['sharpe']:.2f}")
        print(f"  卫星 slice2     : CAGR {out[wname]['slice2_satellite']['cagr']:+.1f}%  dd {out[wname]['slice2_satellite']['max_dd']:.1f}  sr {out[wname]['slice2_satellite']['sharpe']:.2f}")
        print(f"  corr = {corr:.2f}")
        for w in (0.8, 0.7, 0.5):
            b = blends[str(w)]
            print(f"  组合 core{w}/sat{1-w}: CAGR {b['cagr']:+.1f}%  dd {b['max_dd']:.1f}  sr {b['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/core_satellite_sliced_latest.json")
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())