"""Regenerate the S-gap decay JSON (read-only display data).

Source: the frozen ~/Projects/wealth-ideas/karios-audit-2026-10/scratch/
  h2f_trades.json prefix (1130 per-trade S-gap rows, read-only) **plus** the
  live overlay's appended S-gap trades (factor_live_trades.json) so the decay
  tab and the factor vault share one last-40 series. When the live file is
  absent the frozen prefix alone is used (identical to the pre-overlay run).

Output: services/data-sync-service/data/backtest_reports/sgap_decay.json
  (tracked frozen file served by GET /api/backtest/sgap-decay).

Usage:
  PYTHONPATH=src python3 scripts/generate_sgap_decay.py
  PYTHONPATH=src python3 scripts/generate_sgap_decay.py --input /tmp/x.json --output /tmp/sgap_decay.json
  PYTHONPATH=src python3 scripts/generate_sgap_decay.py --no-live   # frozen prefix only
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service import factor_live as fl  # noqa: E402
from data_sync_service.service.sgap_decay import compute_sgap_decay  # noqa: E402

DEFAULT_INPUT = fl.audit_scratch_dir() / fl.SGAP_FROZEN_FILENAME
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parents[1] / "data" / "backtest_reports" / "sgap_decay.json"
)


def _load_frozen(path: Path) -> list[dict]:
    trades = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(trades, dict):
        # Accept {"trades": [...]} wrappers defensively.
        trades = trades.get("trades", [])
    return trades if isinstance(trades, list) else []


def main() -> int:
    ap = argparse.ArgumentParser(description="Regenerate sgap_decay.json")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument(
        "--no-live",
        action="store_true",
        help="ignore factor_live_trades.json (frozen prefix only)",
    )
    args = ap.parse_args()
    src = Path(args.input)
    if not src.exists():
        print(f"missing input: {src}", file=sys.stderr)
        return 1
    out = Path(args.output)
    frozen = _load_frozen(src)
    live = None if args.no_live else fl.load_live_payload(out.parent / fl.LIVE_FILENAME)
    appended = list((live or {}).get("appended", {}).get("sgap") or [])
    # Same merged series the vault uses for its last-40 override.
    trades = fl.merge_trades(fl.load_frozen_sgap(src), appended, family="sgap") or frozen
    payload = compute_sgap_decay(trades, as_of=date.today())
    payload["meta"]["generated_at"] = date.today().isoformat()
    payload["meta"]["source_path"] = str(src)
    if live is not None:
        payload["meta"]["live_overlay"] = {
            "path": str(out.parent / fl.LIVE_FILENAME),
            "status": (live.get("meta") or {}).get("status"),
            "asOf": (live.get("meta") or {}).get("asOf"),
            "appendedTrades": len(appended),
        }
        payload["meta"]["source"] = (
            f"{fl.SGAP_FROZEN_FILENAME} frozen prefix + live overlay "
            f"({len(appended)} appended S-gap trades)"
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out} ({payload['summary']['n_trades']} trades)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
