"""Repeatable factor-system walk-forward validation (2026-11-11).

Reads only frozen, already-used inputs:
- ``h2f_trades.json`` (1130 S-gap trades, contrib already net of 32.28bp)
- optional ``h2d_nav.json`` M30 windows (70% harbor + 30% B3)

It assembles the three preregistered arms (K2 gated / always-on starship /
wait-in-M30 baseline), writes a JSON artifact and a short Chinese report.
Thresholds are frozen; this is the system's validation loop, not a new alpha
search. It never changes live settings, TRAIL_PCT, cooldown k=0, weights,
broker orders, or the user's mode.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src .venv/bin/python scripts/validate_factor_system.py

  # explicit frozen inputs / output paths
  PYTHONPATH=src .venv/bin/python scripts/validate_factor_system.py \
    --trades ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2f_trades.json \
    --m30-nav ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2d_nav.json \
    --output ../../docs/backtests/factor-system-validation-2026-11-11.json \
    --report ../../docs/backtests/factor-system-validation-2026-11-11.md
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.factor_system_validation import (  # noqa: E402
    render_validation_markdown,
    run_walk_forward,
)

AUDIT_SCRATCH = Path.home() / "Projects" / "wealth-ideas" / "karios-audit-2026-10" / "scratch"
DEFAULT_TRADES = AUDIT_SCRATCH / "h2f_trades.json"
DEFAULT_M30_NAV = AUDIT_SCRATCH / "h2d_nav.json"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = REPO_ROOT / "docs" / "backtests" / "factor-system-validation-2026-11-11.json"
DEFAULT_REPORT = REPO_ROOT / "docs" / "backtests" / "factor-system-validation-2026-11-11.md"


def main() -> int:
    parser = argparse.ArgumentParser(description="Factor-system walk-forward validation")
    parser.add_argument("--trades", default=str(DEFAULT_TRADES))
    parser.add_argument("--m30-nav", default=str(DEFAULT_M30_NAV))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    parser.add_argument("--as-of", default=date.today().isoformat())
    parser.add_argument(
        "--no-m30",
        action="store_true",
        help="Do not use an M30 NAV file; baseline arm is then reported unavailable.",
    )
    args = parser.parse_args()

    trades_path = Path(args.trades).expanduser()
    if not trades_path.exists():
        print(f"missing trades input: {trades_path}", file=sys.stderr)
        return 2
    try:
        trades = json.loads(trades_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"unreadable trades input: {exc}", file=sys.stderr)
        return 2

    m30_path = None if args.no_m30 else Path(args.m30_nav).expanduser()
    result = run_walk_forward(trades, m30_nav_path=m30_path, as_of=args.as_of)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_validation_markdown(result), encoding="utf-8")

    summary = result.get("summary") or {}
    print(f"wrote {out_path}")
    print(f"wrote {report_path}")
    print(
        f"trades={summary.get('nTrades')} "
        f"gated={result.get('arms', {}).get('gatedK2', {}).get('total')} "
        f"always={result.get('arms', {}).get('alwaysOnStarship', {}).get('total')} "
        f"baselineAvailable={result.get('arms', {}).get('waitInM30Baseline', {}).get('available')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
