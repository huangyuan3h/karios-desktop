"""Regenerate the frozen S-gap decay JSON (read-only display data).

Source (default): ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2f_trades.json
  (1130 per-trade S-gap rows; reuse existing data, no DB queries).
Output (default): services/data-sync-service/data/backtest_reports/sgap_decay.json
  (tracked frozen file served by GET /api/backtest/sgap-decay).

Usage:
  PYTHONPATH=src python3 scripts/generate_sgap_decay.py
  PYTHONPATH=src python3 scripts/generate_sgap_decay.py --input /tmp/x.json --output /tmp/sgap_decay.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.sgap_decay import compute_sgap_decay  # noqa: E402

DEFAULT_INPUT = (
    Path.home()
    / "Projects"
    / "wealth-ideas"
    / "karios-audit-2026-10"
    / "scratch"
    / "h2f_trades.json"
)
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "sgap_decay.json"
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate sgap_decay.json")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()
    src = Path(args.input)
    if not src.exists():
        print(f"missing input: {src}", file=sys.stderr)
        return 1
    trades = json.loads(src.read_text(encoding="utf-8"))
    if isinstance(trades, dict):
        # Accept {"trades": [...]} wrappers defensively.
        trades = trades.get("trades", [])
    payload = compute_sgap_decay(trades)
    payload["meta"]["generated_at"] = date.today().isoformat()
    payload["meta"]["source_path"] = str(src)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out} ({payload['summary']['n_trades']} trades)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
