#!/usr/bin/env python3
"""Run the backtest engine (OPT-063 / L3-P2) from the CLI.

Usage:
  PYTHONPATH=src python3 scripts/run_backtest.py --start 2026-06-18 --end 2026-08-07
  PYTHONPATH=src python3 scripts/run_backtest.py --start ... --end ... --grid
  PYTHONPATH=src python3 scripts/run_backtest.py --start ... --end ... \
      --score-threshold 85 --max-hold 10 --stop -5 --out /tmp/bt.json

--grid runs the v0 sensitivity grid (score x hold x stop) and prints a
markdown table. Reports are written under data/backtest_reports/ and the
latest one is mirrored to data/backtest_reports/latest.json (the API's
GET /api/backtest/latest-report reads it).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataclasses import asdict

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    default_sensitivity_grid,
    simulate,
)

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


def _md_grid(results) -> str:
    lines = [
        "| score | max_hold | stop | trades | win_rate | avg_net% | max_dd% |",
        "|-------|----------|------|--------|----------|----------|---------|",
    ]
    for r in sorted(results, key=lambda s: -(s.win_rate or 0)):
        c = r.config
        lines.append(
            f"| {c['score_threshold']:.0f} | {c['max_hold_days']} | {c['stop_loss_pct']:.0f} "
            f"| {r.closed} | {r.win_rate or 0:.3f} | {r.avg_net_pnl_pct or 0:.2f} "
            f"| {r.max_drawdown_pct:.1f} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", required=True, help="Window start YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="Window end YYYY-MM-DD")
    ap.add_argument("--grid", action="store_true", help="Run the v0 sensitivity grid")
    ap.add_argument("--score-threshold", type=float, default=85.0)
    ap.add_argument("--max-hold", type=int, default=5)
    ap.add_argument("--stop", type=float, default=-5.0)
    ap.add_argument("--target", type=float, default=10.0)
    ap.add_argument("--score-floor", type=float, default=30.0)
    ap.add_argument("--market", default="CN", choices=["CN", "HK"])
    ap.add_argument("--out", help="Write the JSON report to this file (else REPORT_DIR)")
    args = ap.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "window": {"start": args.start, "end": args.end},
        "mode": "grid" if args.grid else "single",
    }

    if args.grid:
        results = [simulate(c).summary for c in default_sensitivity_grid(args.start, args.end)]
        payload["results"] = [r.to_dict() for r in results]
        print(_md_grid(results))
    else:
        config = BacktestConfig(
            start_date=args.start,
            end_date=args.end,
            score_threshold=args.score_threshold,
            max_hold_days=args.max_hold,
            stop_loss_pct=args.stop,
            target_pnl_pct=args.target,
            score_floor=args.score_floor,
            market=args.market,
        )
        run = simulate(config)
        payload["config"] = asdict(config)
        payload["summary"] = run.summary.to_dict()
        payload["trades"] = [t.to_dict() for t in run.trades]
        s = run.summary
        print(
            f"window {args.start}..{args.end} | trades={s.closed} win_rate={s.win_rate} "
            f"avg_net={s.avg_net_pnl_pct}% max_dd={s.max_drawdown_pct}% "
            f"score_buckets={len(s.by_score_bucket)}"
        )

    out_file = Path(args.out) if args.out else REPORT_DIR / "latest.json"
    out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
