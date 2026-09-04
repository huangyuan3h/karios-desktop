#!/usr/bin/env python3
"""Sliced state-bucket (R10): 按态切分验证 — 每态独立槽位 + 等权组合，对比冻结 union。

Variant slice2 = S-limit+S-gap (50/50); slice3 = +S-shrink (1/3); slice4 = 全四态(1/4, 对照 S-fresh 是否纯噪声)。
验证窗: OOS2/train/valid/past_year + holdout_partial (2026-08-08~数据末)。NAV 存盘供 core-satellite 复用。
"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
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
    "holdout_partial": ("2026-08-08", "2026-08-31"),
}


def daily_ret(nav):
    return [nav[i] / nav[i - 1] - 1 for i in range(1, len(nav)) if nav[i - 1] > 0]


def combo_nav(cal, navs, weights):
    rs = [daily_ret(n) for n in navs]
    n = min(len(r) for r in rs)
    out = [1.0]
    for i in range(n):
        dr = sum(w * r[i] for w, r in zip(weights, rs))
        out.append(out[-1] * (1 + dr))
    return out


def main():
    per_ts = _load_daily(LOAD_S, LOAD_E)
    mv_map = _load_mv_map(LOAD_S, LOAD_E)
    list_dates = _load_list_dates()
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    out = {}
    navs_dir = Path("data/backtest_reports/state_sliced_navs")
    navs_dir.mkdir(parents=True, exist_ok=True)
    for wname, (s, e) in WINDOWS.items():
        cal = _load_calendar(s, e)
        if len(cal) < 5:
            print(f"=== {wname} ({s}~{e}): 无数据 ===")
            continue
        navs = {}
        for st in STATES:
            nav, _ = simulate_state_bucket(cal, per_ts, mv_map, list_dates, date_idx,
                                           state_filter={st})
            navs[st] = nav
        variants = {
            "slice2_LG": (["S-limit", "S-gap"], [0.5, 0.5]),
            "slice3_LGS": (["S-limit", "S-gap", "S-shrink"], [1 / 3] * 3),
            "slice4_all": (STATES, [0.25] * 4),
        }
        res = {"cal": cal, "per_state": {st: stats(cal[:len(n)], n) for st, n in navs.items()}}
        for vname, (sts, ws) in variants.items():
            cn = combo_nav(cal, [navs[st] for st in sts], ws)
            res[vname] = stats(cal[:len(cn)], cn)
            navs[vname] = cn
        out[wname] = {k: v for k, v in res.items() if k != "cal"}
        (navs_dir / f"{wname}.json").write_text(json.dumps(navs))
        print(f"=== {wname} ({s}~{e}) ===")
        for k, v in res.items():
            if k == "cal":
                continue
            if k == "per_state":
                for st, stv in v.items():
                    print(f"  {st:9s}: CAGR {stv['cagr']:+.1f}%  dd {stv['max_dd']:.1f}  sr {stv['sharpe']:.2f}")
            else:
                print(f"  {k:12s}: CAGR {v['cagr']:+.1f}%  dd {v['max_dd']:.1f}  sr {v['sharpe']:.2f}")
        sys.stdout.flush()
    p = Path("data/backtest_reports/state_sliced_latest.json")
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {p}")


if __name__ == "__main__":
    raise SystemExit(main())