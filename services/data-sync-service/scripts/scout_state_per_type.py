#!/usr/bin/env python3
"""Per-state-type decomposition (穿透分析): 四态各自独立回测 + 与择强(trail8)相关性 + union 槽位争抢审计。

每个状态类型单独作为策略（独占 10 槽）回测，得到各自 NAV；再与择强(trail8) NAV 求相关，
并审计 union（共享 10 槽）中每态候选被槽位挤掉的数量。最后做"按态切分"组合对比：
等权各态日收益 = 每态 2.5%×10 槽（总敞口 100%），对比共享槽 union。
"""
import itertools, json, sys
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pick_strong_grid import warm_window, build_nav_from_cache, fetch_etf_closes
from scout_state_bucket_pickstrong import (_load_daily, _load_mv_map, _load_list_dates,
    simulate_state_bucket, stats, _load_calendar)

LOAD_S = "2024-04-01"
LOAD_E = "2026-09-10"
STATES = ["S-limit", "S-gap", "S-fresh", "S-shrink"]
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}


def daily_ret(nav):
    return [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav)) if nav[i - 1] > 0]


def equal_weight_nav(cal, navs):
    rs = [daily_ret(n) for n in navs]
    n = min(len(r) for r in rs)
    out = [1.0]
    for i in range(n):
        dr = sum(r[i] for r in rs) / len(rs)
        out.append(out[-1] * (1 + dr))
    return out


def main():
    etf_close = fetch_etf_closes()
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    out = {}
    for wname, (s, e) in WINDOWS.items():
        cal = _load_calendar(s, e)
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
                                 score="mom", top2=False, trail_pct=8.0)
        pk_map = r["nav"]
        pk_aligned = []
        last = 1.0
        for d in cal:
            v = pk_map.get(d)
            if v is not None:
                last = v
            pk_aligned.append(last)
        counters = {"fills": {}, "blocked": {}, "cands": {}}
        union_nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx,
                                             counters=counters)
        per = {}
        per_navs = []
        for st in STATES:
            nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx,
                                           state_filter={st})
            per_navs.append(nav)
            pr = daily_ret(nav)
            cr = daily_ret(pk_aligned[:len(pr) + 1])
            corr = float(np.corrcoef(pr, cr)[0, 1]) if len(pr) > 2 else 0.0
            per[st] = {"stats": stats(cal[:len(nav)], nav), "corr_pk": round(corr, 3)}
        combos = {}
        for k in range(1, len(STATES) + 1):
            for sub in itertools.combinations(STATES, k):
                navs = [per_navs[STATES.index(x)] for x in sub]
                cn = equal_weight_nav(cal, navs)
                combos["+".join(sub)] = stats(cal[:len(cn)], cn)
        out[wname] = {
            "union": stats(cal, union_nav),
            "per_state": per,
            "counters": counters,
            "combos": combos,
        }
        print(f"=== {wname} ===")
        u = out[wname]["union"]
        print(f"  union(共享槽) : CAGR {u['cagr']:+.1f}%  dd {u['max_dd']:.1f}  sr {u['sharpe']:.2f}")
        for st in STATES:
            stx = per[st]["stats"]
            print(f"  {st:9s}单独: CAGR {stx['cagr']:+.1f}%  dd {stx['max_dd']:.1f}  sr {stx['sharpe']:.2f}  corr_pk {per[st]['corr_pk']:+.2f}")
        print(f"  争抢审计 cands={counters['cands']} blocked={counters['blocked']} fills={counters['fills']}")
        for name, c in sorted(combos.items(), key=lambda kv: -kv[1]["sharpe"])[:5]:
            print(f"  组合 {name:24s}: CAGR {c['cagr']:+.1f}%  dd {c['max_dd']:.1f}  sr {c['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/state_per_type_latest.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())