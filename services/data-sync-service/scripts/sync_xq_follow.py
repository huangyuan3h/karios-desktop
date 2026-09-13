"""One-shot Snowball follow-count snapshot (P0-13 attention panel).

Fetches the current 雪球 full A-share follow cross-section and upserts one
snapshot row set into cn_xq_follow (also runs daily via the scheduler at
15:40 Asia/Shanghai). Use --date to back-tag a manual run.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_xq_follow.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _load_root_env() -> None:
    root = Path(__file__).resolve().parents[3] / ".env"
    if not root.exists():
        return
    with open(root) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() in ("DATABASE_URL",):
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main() -> int:
    _load_root_env()
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=None, help="override snapshot trade_date (YYYY-MM-DD)")
    args = ap.parse_args()

    from data_sync_service.service.xq_follow import sync_xq_follow_snapshot

    result = sync_xq_follow_snapshot(as_of=args.date)
    print(result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
