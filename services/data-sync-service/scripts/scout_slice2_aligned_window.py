#!/usr/bin/env python3
"""slice2 on exact pick-strong frozen window (2025-08-28~2026-08-28) for aligned comparison."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scout_state_bucket_pickstrong import (_load_daily, _load_mv_map, _load_list_dates,
    simulate_state_bucket, stats, _load_calendar)

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
WINDOW = ("2025-08-28", "2026-08-28")


def daily_ret(nav):
    return [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav)) if nav[i - 1] > 0]


def main():
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    cal = _load_calendar(*WINDOW)
    navs = {}
    for st in ("S-limit", "S-gap"):
        nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx, state_filter={st})
        navs[st] = nav
    rs = [daily_ret(n) for n in navs.values()]
    n = min(len(r) for r in rs)
    out = [1.0]
    for i in range(n):
        dr = sum(r[i] for r in rs) / len(rs)
        out.append(out[-1] * (1 + dr))
    for st, nav in navs.items():
        stx = stats(cal[:len(nav)], nav)
        print(f"{st:9s}: total {stx['total_pct']:+.1f}%  CAGR {stx['cagr']:+.1f}%  dd {stx['max_dd']:.1f}  sr {stx['sharpe']:.2f}")
    x = stats(cal[:n], out)
    print(f"slice2   : total {x['total_pct']:+.1f}%  CAGR {x['cagr']:+.1f}%  dd {x['max_dd']:.1f}  sr {x['sharpe']:.2f}  (n={n})")
    print("pick-strong trail8 (冻结 E1, 同窗): total +190.65%  dd 12.6")


if __name__ == "__main__":
    raise SystemExit(main())