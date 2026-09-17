#!/usr/bin/env python3
"""CLI: snapshot East Money limit-up pools into services/data/zt_pool/.

AkShare keeps only ~2 weeks, so run this daily (weekdays) to accumulate the
封板资金/首封时间/炸板次数/连板数 panel (P0-13 B18 follow-up).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/snapshot_zt_pool.py                # today (skip if exists)
  PYTHONPATH=src python3 scripts/snapshot_zt_pool.py --backfill 14  # last 14 calendar days
  PYTHONPATH=src python3 scripts/snapshot_zt_pool.py --force        # overwrite
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.zt_pool_snapshot import (  # noqa: E402
    out_dir,
    snapshot_recent,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--backfill", type=int, default=1, help="calendar days back (weekdays only)")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    logger.info("zt_pool dir: %s", out_dir())
    res = snapshot_recent(days=args.backfill, force=args.force)
    logger.info("saved=%s skipped=%s", res["saved"], res["skipped"])
    for f in res["failed"]:
        logger.warning("failed: %s", f)
    return 0 if (res["saved"] or res["skipped"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
