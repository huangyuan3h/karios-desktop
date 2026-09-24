#!/usr/bin/env python3
"""H-HARBOR-B3-PARK: replace Harbor's argmax parking sleeve with B3 risk-budget.

Prereg docs/designs/harbor-b3-parking-prereg-2026-09-21.md (frozen 2026-09-21).

Harbor = S-3 CN engine (frozen S3_CONFIG) + idle cash parking.
  V0     S-3 only (return base)
  P1     argmax sleeve parking (canonical, reference)
  P1_H2  product H2 sleeve parking (reference)
  H_B3   idle -> B3 risk-budget (homeport.risk_budget_nav), 5bps/side transfer [GATED]
nav *= 1 + r_eng + idle_t * park_ret_t - 5bps/1e4 * |idle_t - idle_{t-1}|

Verdict (frozen):
  K1 H_B3 3w dTot vs V0 all >= 0
  K2 H_B3 long dTot vs V0 >= +50pt
  K3 H_B3 valid MDD at least 5pt shallower than P1
  K4 H_B3 long Sharpe >= P1 long Sharpe
  PASS = K1 & K2 & K3 & K4

Read-only; Live untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_harbor_b3_parking.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_etf_parking_baseline import (  # noqa: E402
    COST,
    WINS,
    _load_etf_closes,
    _metrics,
)
from run_walk_forward import S3_CONFIG  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.harbor import parking_replay  # noqa: E402
from data_sync_service.service.homeport import load_risk_closes, risk_budget_nav  # noqa: E402
from data_sync_service.service.parking_sleeve import hysteresis_parking_replay  # noqa: E402
from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run  # noqa: E402

ARMS = ("V0", "P1", "P1_H2", "H_B3")
GATED = "H_B3"
REF = "P1"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_etf_closes()
    risk_closes = load_risk_closes()
    etf_days = {d for mp in px.values() for d in mp}

    res: dict[str, dict] = {}
    for w, (s, e) in WINS.items():
        cfg = BacktestConfig(start_date=s, end_date=e, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        cal = [d for d in data.calendar if d in etf_days]
        eng = engine_nav_by_day_from_run(list(data.calendar), run.nav_curve)
        snap_by = {str(x.get("date")): x for x in run.positions_by_day}

        idle_by_day: dict[str, float] = {}
        for idx in range(1, len(cal)):
            prev = cal[idx - 1]
            snap = snap_by.get(prev) or {}
            dep = sum(float(p.get("position_pct") or 0.0) for p in (snap.get("positions") or []))
            idle_by_day[prev] = max(0.0, 1.0 - min(1.0, dep))

        b3_nav = risk_budget_nav(risk_closes, cal)
        b3_by_day = dict(zip(cal, b3_nav, strict=True))

        p1 = parking_replay(px, cal, idle_by_day=idle_by_day)
        p1h2 = hysteresis_parking_replay(px, cal)

        navs: dict[str, list[float]] = {v: [1.0] for v in ARMS}
        prev_idle = 0.0
        for rec1, rec2 in zip(p1, p1h2, strict=True):
            day, prev = str(rec1["date"]), str(rec1["prev"])
            idle = idle_by_day.get(prev, 0.0)
            r_eng = eng[day] / eng[prev] - 1.0 if eng.get(prev) and eng.get(day) else 0.0
            navs["V0"].append(navs["V0"][-1] * (1.0 + r_eng))
            for v, rec in zip(("P1", "P1_H2"), (rec1, rec2), strict=True):
                sides = int(rec["sides"])
                navs[v].append(
                    navs[v][-1] * (1.0 + r_eng + idle * (float(rec["parking_ret"]) - COST * sides))
                )
            b3_ret = (
                b3_by_day[day] / b3_by_day[prev] - 1.0
                if b3_by_day.get(prev) and b3_by_day.get(day)
                else 0.0
            )
            transfer = COST * abs(idle - prev_idle)
            navs[GATED].append(navs[GATED][-1] * (1.0 + r_eng + idle * b3_ret - transfer))
            prev_idle = idle

        base = navs["V0"][-1] - 1.0
        res[w] = {
            v: {
                "total": round(100 * (navs[v][-1] - 1), 1),
                "delta_vs_V0": round(100 * ((navs[v][-1] - 1) - base), 1),
                **_metrics(navs[v]),
            }
            for v in ARMS
        }
        print(
            f"  {w:<10} V0 {res[w]['V0']['total']:+7.1f}% | "
            + "  ".join(
                f"{v}:{res[w][v]['total']:+.1f}({res[w][v]['delta_vs_V0']:+.1f})"
                for v in ARMS[1:]
            ),
            flush=True,
        )
        print(
            f"  {'':<10} V0 mdd {res[w]['V0']['mdd']} | P1 mdd {res[w]['P1']['mdd']} "
            f"sr {res[w]['P1']['sharpe']} | H_B3 mdd {res[w]['H_B3']['mdd']} sr {res[w]['H_B3']['sharpe']}",
            flush=True,
        )

    verdict: dict[str, object] = {}
    if all(w in res for w in ("OOS2", "train", "valid", "long")):
        d3 = [res[w][GATED]["delta_vs_V0"] for w in ("OOS2", "train", "valid")]
        d_long = res["long"][GATED]["delta_vs_V0"]
        mdd_gain = round(res["valid"][GATED]["mdd"] - res["valid"][REF]["mdd"], 1)
        sr_gain = round(res["long"][GATED]["sharpe"] - res["long"][REF]["sharpe"], 2)
        k1 = min(d3) >= 0
        k2 = d_long >= 50.0
        k3 = mdd_gain >= 5.0
        k4 = sr_gain >= 0
        verdict = {
            "arm": GATED,
            "deltas_3w_vs_V0": d3,
            "k1_no_window_dilution": k1,
            "long_delta_vs_V0": d_long,
            "k2_long_ge_50": k2,
            "valid_mdd_gain_vs_P1": mdd_gain,
            "k3_valid_mdd_5pt_shallower": k3,
            "long_sharpe_gain_vs_P1": sr_gain,
            "k4_sharpe_better": k4,
            "pass": bool(k1 and k2 and k3 and k4),
            "note": "Harbor parking asset swap; PASS = candidate only, Live unchanged",
        }
        print("\n## Verdict (frozen prereg)\n")
        print(f"  K1 (3w Δtot vs V0 all ≥ 0): {k1} {d3}")
        print(f"  K2 (long Δtot vs V0 ≥ +50): {k2} ({d_long})")
        print(f"  K3 (valid MDD ≥ 5pt shallower than P1): {k3} ({mdd_gain})")
        print(f"  K4 (long Sharpe ≥ P1): {k4} ({sr_gain})")
        print(f"  → {'PASS' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "harbor_b3_parking_2026-09-21.json"
        path.write_text(
            json.dumps(
                {
                    "tag": "harbor-b3-parking-2026-09-21",
                    "protocol": (
                        "Harbor = S-3 CN + idle parking; H_B3 = idle -> B3 risk-budget "
                        "(homeport.risk_budget_nav, 5 assets inverse-vol monthly); "
                        "P1/P1_H2 = argmax sleeve references; 5bps/side transfer; "
                        "prereg docs/designs/harbor-b3-parking-prereg-2026-09-21.md"
                    ),
                    "windows": res,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(),
                },
                ensure_ascii=False,
                indent=1,
            ),
            encoding="utf-8",
        )
        print(f"\nreport: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
