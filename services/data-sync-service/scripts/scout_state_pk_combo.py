#!/usr/bin/env python3
"""Per-state x pick-strong combo (R11): 优化后逐态与择强 trail8 的组合.

Satellite variants:
  slice2_old = S-limit(5,10,3)+S-gap(5,10,3) 50/50 (R10 冻结)
  slice2_opt = S-limit(2,10,3)+S-gap(3,15,3) 50/50 (R11 逐态优化)
  L_opt / G_opt = 各态单独(优化参)
  LGS_opt    = +S-shrink(5,10,15) 1/3 (对照可选腿)
Core = pick-strong trail8 (E1). Weights core/sat ∈ {80/20,70/30,50/50}.
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
    "aligned": ("2025-08-28", "2026-08-28"),
}
STATE_PARAMS = {
    "S-limit": {"bucket_q": 2, "max_pos": 10, "body": 3},
    "S-gap": {"bucket_q": 3, "max_pos": 15, "body": 3},
    "S-shrink": {"bucket_q": 5, "max_pos": 10, "body": 15},
}
SATELLITES = {
    "slice2_old": (["S-limit", "S-gap"], [0.5, 0.5], {"S-limit": (5, 10, 3), "S-gap": (5, 10, 3)}),
    "slice2_opt": (["S-limit", "S-gap"], [0.5, 0.5], {"S-limit": (2, 10, 3), "S-gap": (3, 15, 3)}),
    "L_opt": (["S-limit"], [1.0], {"S-limit": (2, 10, 3)}),
    "G_opt": (["S-gap"], [1.0], {"S-gap": (3, 15, 3)}),
    "LGS_opt": (["S-limit", "S-gap", "S-shrink"], [1 / 3] * 3,
                {"S-limit": (2, 10, 3), "S-gap": (3, 15, 3), "S-shrink": (5, 10, 15)}),
}


def daily_ret(nav):
    return [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav)) if nav[i - 1] > 0]


def combo_nav(cal, navs, weights):
    rs = [daily_ret(n) for n in navs]
    n = min(len(r) for r in rs)
    out = [1.0]
    for i in range(n):
        out.append(out[-1] * (1 + sum(w * r[i] for w, r in zip(weights, rs))))
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
        pk = []
        last = 1.0
        for d in cal:
            v = pk_map.get(d)
            if v is not None:
                last = v
            pk.append(last)
        to_compute = {}
        for sname, (sts, ws, params) in SATELLITES.items():
            for st in sts:
                to_compute[(st, *params[st])] = True
        state_navs = {}
        for (st, bq, mp, body) in to_compute:
            nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx,
                                           state_filter={st}, bucket_q=bq, max_pos=mp,
                                           hold_map={st: body})
            state_navs[(st, bq, mp, body)] = nav
        res = {}
        for sname, (sts, ws, params) in SATELLITES.items():
            sat = combo_nav(cal, [state_navs[(st, *params[st])] for st in sts], ws)
            n = min(len(pk), len(sat))
            pk2, sat2 = pk[:n], sat[:n]
            pr = daily_ret(pk2)
            sr_ = daily_ret(sat2)
            corr = float(np.corrcoef(pr, sr_)[0, 1]) if n > 2 else 0.0
            blends = {}
            for w in (0.8, 0.7, 0.5):
                nav = [1.0]
                for i in range(1, n):
                    nav.append(nav[-1] * (1 + w * pr[i - 1] + (1 - w) * sr_[i - 1]))
                blends[str(w)] = stats(cal[:n], nav)
            res[sname] = {"sat": stats(cal[:n], sat2), "corr": round(corr, 3), "blends": blends}
        out[wname] = {"pk": stats(cal[:n], pk2), "sats": res}
        print(f"=== {wname} ===")
        x = out[wname]["pk"]
        print(f"  择强核心: CAGR {x['cagr']:+.1f}%  dd {x['max_dd']:.1f}  sr {x['sharpe']:.2f}")
        for sname, v in res.items():
            x = v["sat"]
            print(f"  {sname:11s}卫星: CAGR {x['cagr']:+.1f}%  dd {x['max_dd']:.1f}  sr {x['sharpe']:.2f}  corr {v['corr']:.2f}")
            for w in (0.7, 0.5):
                b = v["blends"][str(w)]
                print(f"    混合 {w}/{1-w}: CAGR {b['cagr']:+.1f}%  dd {b['max_dd']:.1f}  sr {b['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/state_pk_combo_latest.json")
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())