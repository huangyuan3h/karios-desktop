#!/usr/bin/env python3
"""H2 vs canonical parking across additional windows (read-only robustness check).

Reuses the frozen eval harness (eval_etf_parking_baseline) but runs extra
periods (adjacent ~1y chunks + cross-cycle spans) to see whether H2's edge is
window-stable, at two levels:
  LEG   = ETF parking sleeve NAV only (canonical parking_replay vs H2)
  HARBOR= S-3 engine + idle parking (P1 vs P1_H2)

Composite score uses the user's priority weights W = ret 0.5 / sharpe 0.3 /
mdd 0.2, scored per window by pairwise rank (+1 better / -1 worse / 0 tie).

Usage: PYTHONPATH=src:scripts python3 scripts/diag_h2_vs_canonical_windows.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_etf_parking_baseline import (  # noqa: E402
    COST,
    S3_CONFIG,
    _load_etf_closes,
    _load_stock_closes,
    _metrics,
    _mom_at,
)
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.parking_sleeve import hysteresis_parking_replay  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402

WEIGHTS = {"ret": 0.5, "sharpe": 0.3, "mdd": 0.2}
WINS = {
    "21H2-22H1": ("2021-08-01", "2022-08-01"),
    "22H2-23H1": ("2022-08-01", "2023-08-01"),
    "23H2-24H1": ("2023-08-01", "2024-08-01"),
    "24H2-25H1": ("2024-08-01", "2025-08-01"),
    "25H2-26H1": ("2025-08-01", "2026-02-01"),
    "26H1-26H2": ("2026-03-01", "2026-08-07"),
    "cycle21-23": ("2021-08-01", "2023-08-01"),
    "recent23-26": ("2023-08-01", "2026-08-07"),
    "full21-26": ("2021-08-01", "2026-08-07"),
}


def _cmp(a, b):
    return 1 if b > a else (-1 if b < a else 0)


def main() -> int:
    px = _load_etf_closes()
    etf_days = {d for mp in px.values() for d in mp}
    rows = []

    for name, (s, e) in WINS.items():
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        cal = [d for d in data.calendar if d in etf_days]
        eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
        snap_by = {str(x.get("date")): x for x in run.positions_by_day}

        stock_codes = {
            str(p.get("ts_code") or "").strip().upper()
            for snap in snap_by.values()
            for p in (snap.get("positions") or [])
            if str(p.get("ts_code") or "").strip()
        }
        spx = _load_stock_closes(stock_codes - set(px))

        idle_by_day = {}
        stock_mom_by_day = {}
        for idx in range(1, len(cal)):
            prev = cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))
            smoms = [
                m
                for p in (snap.get("positions") or [])
                if (m := _mom_at(spx.get(str(p.get("ts_code") or "").strip().upper()) or {}, prev))
                is not None
            ]
            stock_mom_by_day[prev] = sum(smoms) / len(smoms) if smoms else None

        # LEG
        leg = {}
        canon_recs = parking_replay(px, cal)
        h2_recs = hysteresis_parking_replay(px, cal)
        leg_canon = [1.0]
        leg_h2 = [1.0]
        for rc, rh in zip(canon_recs, h2_recs, strict=True):
            leg_canon.append(leg_canon[-1] * (1.0 + float(rc["parking_ret"]) - COST * int(rc["sides"])))
            leg_h2.append(leg_h2[-1] * (1.0 + float(rh["parking_ret"]) - COST * int(rh["sides"])))
        leg["canon"] = _metrics(leg_canon)
        leg["h2"] = _metrics(leg_h2)

        # HARBOR = engine + idle * parking
        park_recs = {"canon": parking_replay(px, cal, idle_by_day=idle_by_day),
                     "h2": hysteresis_parking_replay(px, cal)}
        har = {k: [1.0] for k in park_recs}
        for recs in zip(*[park_recs[k] for k in ("canon", "h2")], strict=True):
            rec0 = recs[0]
            day, prev = str(rec0["date"]), str(rec0["prev"])
            idle = idle_by_day.get(prev, 0.0)
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
            for k, rec in zip(("canon", "h2"), recs, strict=True):
                har[k].append(har[k][-1] * (1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * int(rec["sides"]))))
        har["canon"] = _metrics(har["canon"])
        har["h2"] = _metrics(har["h2"])

        row = {"window": name, "start": s, "end": e,
               "leg_canon": leg["canon"], "leg_h2": leg["h2"],
               "har_canon": har["canon"], "har_h2": har["h2"]}
        rows.append(row)
        print(
            f"{name:11s} LEG c {leg['canon']['cagr']:+7.1f}/{leg['canon']['mdd']:+6.1f}/{leg['canon']['sharpe']:4.2f}"
            f"  h2 {leg['h2']['cagr']:+7.1f}/{leg['h2']['mdd']:+6.1f}/{leg['h2']['sharpe']:4.2f}"
            f" | HAR c {har['canon']['cagr']:+7.1f}/{har['canon']['mdd']:+6.1f}/{har['canon']['sharpe']:4.2f}"
            f"  h2 {har['h2']['cagr']:+7.1f}/{har['h2']['mdd']:+6.1f}/{har['h2']['sharpe']:4.2f}",
            flush=True,
        )

    # ---- composite (pairwise rank, weighted) ----
    def score(level):
        tot = 0.0
        tally = {"ret": 0, "sharpe": 0, "mdd": 0}
        n = 0
        for r in rows:
            c, h = r[f"{level}_canon"], r[f"{level}_h2"]
            for dim, key in (("ret", "cagr"), ("sharpe", "sharpe"), ("mdd", "mdd")):
                # mdd is negative; less negative = better
                d = _cmp(c[key], h[key])
                tally[dim] += d
                tot += WEIGHTS[dim] * d
            n += 1
        return tot / n, tally

    print("\n## composite (weights ret 0.5 / sharpe 0.3 / mdd 0.2; +1 = H2 wins window)")
    for level, label in (("leg", "停车腿"), ("har", "港湾线(S-3+停车)")):
        s, t = score(level)
        print(f"  {label:18s} composite {s:+.3f}   wins(win/tie/loss vs canon): "
              f"ret {t['ret']:+d}  sharpe {t['sharpe']:+d}  mdd {t['mdd']:+d}")
    print("  (composite range -1..+1; >0 = H2 better under these weights)")

    # weighted edge using cagr only, to answer '收益最重要'
    print("\n## mean CAGR across windows (ret-first view)")
    for level in ("leg", "har"):
        cs = np.mean([r[f"{level}_canon"]["cagr"] for r in rows])
        hs = np.mean([r[f"{level}_h2"]["cagr"] for r in rows])
        print(f"  {level:5s} canon {cs:+7.2f}%  h2 {hs:+7.2f}%   edge {hs - cs:+.2f}pt")

    out = ROOT / "data" / "backtest_reports" / "h2_vs_canonical_windows.json"
    out.write_text(json.dumps({"weights": WEIGHTS, "is_holdout": False, "windows": rows},
                              indent=2, default=float))
    print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
