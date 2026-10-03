"""Regenerate the frozen fleet JSON (read-only display data).

Source (dev-only): ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/fleet/
  (legs.npz, nav_all/base npz, fleet_metrics.json; no DB queries).
Output (tracked frozen file served by GET /api/backtest/fleet):
- services/data-sync-service/data/backtest_reports/fleet.json

Usage:
  cd services/data-sync-service && PYTHONPATH=src python3 scripts/generate_fleet.py
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

AUDIT_FLEET = (
    Path.home() / "Projects" / "wealth-ideas" / "karios-audit-2026-10" / "scratch" / "fleet"
)
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "fleet.json"


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate fleet.json")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--date", default=date.today().isoformat())
    args = ap.parse_args()
    try:
        import numpy as np
    except ImportError as exc:
        print(f"numpy required: {exc}")
        return 1
    legs = np.load(AUDIT_FLEET / "legs.npz", allow_pickle=True)
    cal_long = list(legs["cal_long"])
    cal_hold = list(legs["cal_hold"])
    nav_all = np.load(AUDIT_FLEET / "nav_all.npz")
    nav_base = np.load(AUDIT_FLEET / "nav_base.npz")
    fleet_long = np.array(nav_all["long_nav"], float)
    fleet_hold = np.array(nav_all["hold_nav"], float)
    base_long = np.array(nav_base["long_nav"], float)
    base_hold = np.array(nav_base["hold_nav"], float)
    star_long = np.array(legs["star_orig_long"], float)
    star_hold = np.array(legs["star_orig_hold"], float)
    hs_long = np.array(legs["hs300_long"], float)
    hs_hold = np.array(legs["hs300_hold"], float)
    cal_full = cal_long + cal_hold
    # Stitch holdout rebased to the long end (weekend flat, no gap return).
    fleet_full = list(fleet_long / fleet_long[0]) + list(
        fleet_hold / fleet_hold[0] * fleet_long[-1] / fleet_long[0]
    )
    base_full = list(base_long / base_long[0]) + list(
        base_hold / base_hold[0] * base_long[-1] / base_long[0]
    )
    star_full = list(star_long / star_long[0]) + list(
        star_hold / star_hold[0] * star_long[-1] / star_long[0]
    )
    hs_full = list(hs_long / hs_long[0]) + list(hs_hold / hs_hold[0] * hs_long[-1] / hs_long[0])
    metrics = json.loads((AUDIT_FLEET / "fleet_metrics.json").read_text())
    payload = {
        "meta": {
            "source": "karios-audit-2026-10/FLEET_report.md + scratch/fleet (120w, 50M PIT, monthly)",
            "generated_at": args.date,
            "windows": {
                "OOS2": "2024-08-01..2025-08-01",
                "valid": "2026-03-02..2026-08-07",
                "holdout": "2026-08-10..2026-09-30",
                "long": "2021-08-02..2026-08-07",
            },
            "gate": {"rule": "H2k-K2", "N": 40, "X": 75.0, "off_below": 50.0, "steps": [0, 10, 20]},
            "cost": "satellite 32.28bp/trade, monthly 5bp/side + 15bp/switch, 50M filter + sqrt k150bp",
            "note": (
                "Starship B stays the live baseline; fleet is research/display only. "
                "L1/L4 never fired in sample (free insurance); L2/L3 carry small "
                "insurance taxes (flagged, kept as approved)."
            ),
        },
        "windows": {
            "fleet": {
                w: {
                    "total": metrics["all"][w]["total"] * 100.0,
                    "cagr": metrics["all"][w]["cagr"] * 100.0,
                    "mdd": metrics["all"][w]["mdd"] * 100.0,
                    "sharpe": metrics["all"][w]["sharpe"],
                    "calmar": metrics["all"][w]["calmar"],
                    "worst_month": metrics["all"][w]["worst_month"],
                    "worst_month_ret": metrics["all"][w]["worst_month_ret"] * 100.0,
                    "recover_days": metrics["all"][w]["recover_days"],
                }
                for w in ("OOS2", "valid", "holdout", "long")
            },
            "base": {
                w: {
                    "total": metrics["base"][w]["total"] * 100.0,
                    "mdd": metrics["base"][w]["mdd"] * 100.0,
                    "sharpe": metrics["base"][w]["sharpe"],
                }
                for w in ("OOS2", "valid", "holdout", "long")
            },
        },
        "defense": {
            "days": 1252,
            "L1_days": 0,
            "L2_days": 69,
            "L3_days": 20,
            "L4_days": 0,
            "recommendation": "keep-all",
        },
        "equity": {
            "dates": cal_full,
            "fleet": [round(float(v), 6) for v in fleet_full],
            "base": [round(float(v), 6) for v in base_full],
            "starship_b": [round(float(v), 6) for v in star_full],
            "hs300": [round(float(v), 6) for v in hs_full],
        },
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out} ({len(cal_full)} days)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
