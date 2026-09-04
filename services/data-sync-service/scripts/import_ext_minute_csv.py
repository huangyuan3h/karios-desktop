#!/usr/bin/env python3
"""Import vendor 5/15-minute yearly CSVs into bar_5min (last hour only).

Need from 网盘 (按年汇总, not 按月, not 1/30/60min):
  5分钟_按年汇总  2024 + 2025 + 2026
Drop under repo data/ as data/2024_5min, data/2025_5min, data/2026_5min
(or unzip in place). 5min overwrites 15min on the same 1430/1500 slot.

Until 5min lands, 2025 15min is imported as 1430+1500 closes (fill proxy).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/import_ext_minute_csv.py
  PYTHONPATH=src python3 scripts/import_ext_minute_csv.py --path ../../data/2025_15min
  PYTHONPATH=src python3 scripts/import_ext_minute_csv.py --path ../../data/2025_5min --freq 5
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.service.ext_minute_csv import (  # noqa: E402
    detect_freq,
    import_vendor_tree,
    parse_keep_times,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_DATA = Path(__file__).resolve().parents[3] / "data"


def _discover(data_root: Path) -> list[Path]:
    """Prefer 5min trees, then 15min. Skip zips (unzip first)."""
    if not data_root.is_dir():
        return []
    fives: list[Path] = []
    fifteens: list[Path] = []
    for p in sorted(data_root.iterdir()):
        if not p.is_dir():
            continue
        freq = detect_freq(p)
        name = p.name.lower()
        if "5min" not in name and "5分钟" not in name and "15min" not in name and "15分钟" not in name:
            continue
        if freq == 5:
            fives.append(p)
        else:
            fifteens.append(p)
    return fives + fifteens


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--path", type=Path, default=None, help="One folder or CSV")
    ap.add_argument("--freq", type=int, choices=(5, 15), default=None)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument(
        "--times",
        default=None,
        help="Comma HHMM keep-list (default: last hour for 5min, 1430+1500 for 15min). "
        "Example: 1330,1400",
    )
    args = ap.parse_args()
    keep = parse_keep_times(args.times)

    roots = [args.path] if args.path else _discover(REPO_DATA)
    if not roots:
        logger.error("no *5min* / *15min* folders under %s", REPO_DATA)
        return 1

    roots = sorted(roots, key=lambda p: 0 if detect_freq(p) == 5 else 1)
    total = {"imported": 0, "stored": 0, "skipped": 0, "files": 0}
    for root in roots:
        stats = import_vendor_tree(root, freq=args.freq, workers=args.workers, keep_times=keep)
        logger.info("done %s %s", root, stats)
        for k in total:
            total[k] += int(stats.get(k) or 0)
    logger.info("all %s", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
