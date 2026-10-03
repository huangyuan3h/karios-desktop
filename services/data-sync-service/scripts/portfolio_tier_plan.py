#!/usr/bin/env python3
"""Portfolio tier-plan CLI (Karios RESTRUCTURE PR1, read-only).

Prints the combo-tier plan as JSON. No DB, no network, no broker calls.

Examples:
  PYTHONPATH=src python scripts/portfolio_tier_plan.py --total-assets 1200000 --current-tier A
  PYTHONPATH=src python scripts/portfolio_tier_plan.py --total-assets 1600000 --current-tier A
  PYTHONPATH=src python scripts/portfolio_tier_plan.py --total-assets 2200000 --current-tier B
  PYTHONPATH=src python scripts/portfolio_tier_plan.py --total-assets 1200000 --starship-ready
  PYTHONPATH=src python scripts/portfolio_tier_plan.py --total-assets 1200000 \\
      --paper20-pass --holdout-recovered --filtered-valid-positive
  KARIOS_TOTAL_ASSETS=1200000 PYTHONPATH=src python scripts/portfolio_tier_plan.py

NAV input (optional, oldest -> newest, for fuse check):
  --nav "1.0,0.99,0.98"  |  --nav-file nav.csv  (comma/line separated numbers)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _parse_nav_text(text: str) -> list[float]:
    parts = [p.strip() for p in text.replace(";", ",").replace("\n", ",").split(",") if p.strip()]
    return [float(p) for p in parts]


def main() -> int:
    ap = argparse.ArgumentParser(description="Plan combo tier (read-only)")
    ap.add_argument("--total-assets", type=float, default=None)
    ap.add_argument("--current-tier", type=str, default="A")
    ap.add_argument("--nav", type=str, default=None)
    ap.add_argument("--nav-file", type=str, default=None)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--starship-ready", dest="starship_ready", action="store_true", default=None)
    g.add_argument("--no-starship-ready", dest="starship_ready", action="store_false", default=None)
    ap.add_argument("--paper20-pass", action="store_true", default=False)
    ap.add_argument("--holdout-recovered", action="store_true", default=False)
    ap.add_argument("--filtered-valid-positive", action="store_true", default=False)
    ap.add_argument("--pretty", action="store_true", default=False)
    args = ap.parse_args()

    from data_sync_service.service import combo_tiers as ct
    from data_sync_service.service import combo_tiers_config as cfg

    ta = args.total_assets
    source = "flag"
    if ta is None:
        env_raw = (os.getenv("KARIOS_TOTAL_ASSETS") or "").strip()
        if env_raw:
            ta = float(env_raw)
            source = "env"
        else:
            ta = float(cfg.DEFAULT_TOTAL_ASSETS)
            source = "default"

    nav: list[float] | None = None
    if args.nav_file:
        nav = _parse_nav_text(Path(args.nav_file).read_text())
    elif args.nav:
        nav = _parse_nav_text(args.nav)

    if args.starship_ready is not None:
        status: bool | None = bool(args.starship_ready)
    elif args.paper20_pass or args.holdout_recovered or args.filtered_valid_positive:
        status = None
    else:
        status = False  # fail-closed 0% until H2k lands

    plan = ct.plan_tier(
        float(ta),
        args.current_tier,
        nav,
        status,
        paper20_pass=args.paper20_pass or None,
        holdout_recovered=args.holdout_recovered or None,
        filtered_valid_positive=args.filtered_valid_positive or None,
    )
    plan["total_assets_source"] = source
    print(json.dumps(plan, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
