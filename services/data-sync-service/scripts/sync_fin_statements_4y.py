"""Backfill CN raw financial statements (4-year window) to local Postgres.

Usage:
    PYTHONPATH=src python3 scripts/sync_fin_statements_4y.py --limit 5
    PYTHONPATH=src python3 scripts/sync_fin_statements_4y.py
    PYTHONPATH=src python3 scripts/sync_fin_statements_4y.py --offset 1000 --limit 1000

Full run is ~16k tushare calls (3/stock); reruns are idempotent and skip
fresh stocks by default. Progress logs every 50 stocks.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _load_root_env() -> None:
    """Best-effort load of repo-root .env for manual runs (no secret output)."""
    root = Path(__file__).resolve().parents[3] / ".env"
    if not root.exists():
        return
    with open(root) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k in ("TU_SHARE_API_KEY", "TUSHARE_TOKEN", "DATABASE_URL"):
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def main() -> int:
    _load_root_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--cutoff", type=str, default=None,
                    help="end_date >= YYYY-MM-DD (default: 4 years ago today)")
    ap.add_argument("--no-skip-fresh", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cutoff = date.fromisoformat(args.cutoff) if args.cutoff else date.today() - timedelta(days=4 * 365)

    from data_sync_service.service import cn_fin_statements

    out = cn_fin_statements.sync_4y_window(
        cutoff=cutoff,
        limit_codes=args.limit,
        offset=args.offset,
        sleep=args.sleep,
        skip_fresh=not args.no_skip_fresh,
    )
    print(f"cutoff={cutoff} universe={out['universe']} skipped_fresh={out['skipped_fresh']} "
          f"stocks={out['stocks']} updated={out['updated']} failed={len(out['failed'])}")
    if out["failed"]:
        print("failed sample:", out["failed"][:10])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
