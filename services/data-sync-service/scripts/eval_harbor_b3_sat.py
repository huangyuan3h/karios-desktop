#!/usr/bin/env python3
"""H-B3-SAT: Homeport (Harbor x B3 50/50) x habit satellite three-leg (2026-09-14).

Only variable vs H-SAT-W: core leg = M50 monthly-rebalanced (B15 frozen caliber)
instead of Harbor. Satellite overlay = blend_nav_opportunity on active days.
Grid w in {0.10, 0.15, 0.20, 0.25, 1/3, 0.40, 0.50}; windows OOS2/train/valid/long
(+holdout descriptive). Frozen prereg: docs/designs/homeport-sat-prereg-2026-09-14.md
K1 worst dTotal >= -5pt; K2 long dSharpe >= +0.10 and dTotal >= +25pt; K3 long
dMDD >= 0 and valid dMDD >= -2pt; pick largest w passing all.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_harbor_b3_sat.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from eval_harbor_riskbudget import (  # noqa: E402
    COST,
    _blend_monthly,
    _harbor_nav,
    _load_panel,
    _rp_nav,
)
from eval_twin_star_parking import _fmt, _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
GRID = (0.10, 0.15, 0.20, 0.25, 1 / 3, 0.40, 0.50)
WINS = ("OOS2", "train", "valid", "long")


def _corr(a: list[float], b: list[float]) -> float | None:
    if len(a) < 3:
        return None
    x, y = np.array(a), np.array(b)
    if x.std() == 0 or y.std() == 0:
        return None
    return round(float(np.corrcoef(x, y)[0, 1]), 2)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_panel()
    results: dict[str, dict] = {}
    for w in (*WINS, "holdout"):
        s, e = WINDOWS[w]
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        cal, harbor = _harbor_nav(px, s, e)
        rp = _rp_nav(px, cal, COST)
        m50 = _blend_monthly(harbor, rp, 0.5, cal, COST)
        ctx = load_sgap_context(s, e)
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, body=3, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            rank_key="amp_1430", gate_1430=True,
        )
        rows = sat.get("rows") or []
        dates = [str(r["date"]) for r in rows]
        sat_nav = [float(r.get("satNav") or 1.0) for r in rows]
        active = [bool(r.get("satActive")) for r in rows]
        if sat_nav and sat_nav[0] > 0:
            base = sat_nav[0]
            sat_nav = [v / base for v in sat_nav]

        m50_by_day = dict(zip(cal, m50, strict=True))
        core_nav: list[float] = []
        last, j = 1.0, 0
        for d in dates:
            while j < len(cal) and cal[j] <= d:
                last = m50_by_day[cal[j]]
                j += 1
            core_nav.append(last)
        n = min(len(core_nav), len(sat_nav), len(active))
        core_nav, sat_nav, active = core_nav[:n], sat_nav[:n], active[:n]
        m50_m = _stats(core_nav)
        sat_m = _stats(sat_nav)
        harbor_m = _stats([v for d, v in zip(cal, harbor, strict=True) if s <= d <= e])
        core_rets = [core_nav[i] / core_nav[i - 1] - 1 for i in range(1, n) if core_nav[i - 1]]
        sat_rets = [sat_nav[i] / sat_nav[i - 1] - 1 for i in range(1, n) if sat_nav[i - 1]]
        act_idx = [i - 1 for i in range(1, n) if active[i]]
        grid: dict[str, dict] = {}
        for gw in GRID:
            nav = blend_nav_opportunity(core_nav, sat_nav, active, sat_weight=gw)
            m = _stats(nav)
            grid[str(round(gw, 4))] = {
                **m,
                "delta_total": round(m["total_pct"] - m50_m["total_pct"], 1),
                "delta_sharpe": round(m["sharpe"] - m50_m["sharpe"], 2),
                "delta_mdd": round(m["max_dd"] - m50_m["max_dd"], 1),
            }
        results[w] = {
            "harbor_ref": harbor_m, "m50": m50_m, "satellite": sat_m, "grid": grid,
            "pct_active": round(100 * sum(1 for a in active if a) / max(1, n), 1),
            "fills": sat["summary"].get("fillCount"),
            "corr_all": _corr(core_rets, sat_rets),
            "corr_active": _corr([core_rets[i] for i in act_idx], [sat_rets[i] for i in act_idx]),
            "n_days": n,
        }
        print(f"  M50 {_fmt(m50_m)} | sat {_fmt(sat_m)} | active {results[w]['pct_active']}% "
              f"corr all/act {results[w]['corr_all']}/{results[w]['corr_active']}", flush=True)
        for key, gm in grid.items():
            print(f"  w={float(key):<5} {gm['total_pct']:+7.1f}/{gm['cagr']:6.1f}/{gm['max_dd']:6.1f}/"
                  f"{gm['sharpe']:5.2f}  Δtot {gm['delta_total']:+6.1f} Δsr {gm['delta_sharpe']:+.2f} "
                  f"Δdd {gm['delta_mdd']:+.1f}", flush=True)
        del ctx

    verdict: dict[str, dict] = {}
    chosen = None
    for key in (str(round(x, 4)) for x in GRID):
        g = results["valid"]["grid"][key]
        worst_delta = min(results[w]["grid"][key]["delta_total"] for w in WINS)
        long_g = results["long"]["grid"][key]
        k1 = worst_delta >= -5.0
        k2 = long_g["delta_sharpe"] >= 0.10 and long_g["delta_total"] >= 25.0
        # max_dd is negative; worse = more negative = delta_mdd < 0.
        k3 = long_g["delta_mdd"] >= 0.0 and g["delta_mdd"] >= -2.0
        ok = k1 and k2 and k3
        verdict[key] = {
            "k1": k1, "k2": k2, "k3": k3, "pass": ok,
            "worst_delta": round(worst_delta, 1),
            "long_delta_total": long_g["delta_total"],
            "long_delta_sharpe": long_g["delta_sharpe"],
            "valid_delta_total": g["delta_total"], "valid_delta_mdd": g["delta_mdd"],
        }
        if ok:
            chosen = float(key)
    print("\n## verdict (frozen): K1 worst Δtotal ≥ −5pt · K2 long Δsr ≥ +0.10 & Δtot ≥ +25pt · "
          "K3 long ΔMDD ≥ 0 (max_dd negative; not worse) & valid ΔMDD ≥ −2pt")
    for key in (str(round(x, 4)) for x in GRID):
        v = verdict[key]
        print(f"  w={float(key):<5} K1 {'ok' if v['k1'] else 'X '} K2 {'ok' if v['k2'] else 'X '} "
              f"K3 {'ok' if v['k3'] else 'X '} | worstΔ {v['worst_delta']:+6.1f} "
              f"longΔ {v['long_delta_total']:+6.1f}/{v['long_delta_sharpe']:+.2f}sr "
              f"validΔ {v['valid_delta_total']:+6.1f}")
    print(f"  chosen: {chosen if chosen is not None else 'NONE -> REJECT (family extension closed)'}")
    hv = results.get("holdout")
    if hv:
        print(f"  [descriptive] holdout M50 {_fmt(hv['m50'])} | sat {_fmt(hv['satellite'])} | "
              f"w=0.25 {_fmt(hv['grid']['0.25'])}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "harbor_b3_sat_2026-09-14.json").write_text(
            json.dumps(
                {
                    "tag": "h-b3-sat-2026-09-14",
                    "prereg": "docs/designs/homeport-sat-prereg-2026-09-14.md",
                    "grid": list(GRID), "results": results, "verdict": verdict,
                    "chosen": chosen, "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False, indent=2, default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
