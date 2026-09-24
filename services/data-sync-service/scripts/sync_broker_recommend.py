#!/usr/bin/env python3
"""Pull monthly broker top-picks (券商月度金股) 2021+ via tushare broker_recommend.

This is the closest obtainable "consensus / attention" text-ish dataset on the
current ¥200 Tushare Pro token (news/anns/ths_hot/dc_hot have no permission).
Registered in docs/designs/data-gap-backfill-2026-08.md §5 as a free pull.

Output: data/altdata/broker_recommend.csv (resume-safe: months already present are skipped).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_broker_recommend.py --smoke
  PYTHONPATH=src python3 scripts/sync_broker_recommend.py --start 2021-01
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.clients.tushare_pool import get_pool  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUT = Path(__file__).resolve().parents[1] / "data" / "altdata" / "broker_recommend.csv"
COLS = ["month", "broker", "ts_code", "name"]
DEFAULT_START = "2021-01"


def _months(start: str, end: str) -> list[str]:
    y0, m0 = (int(x) for x in start.split("-"))
    y1, m1 = (int(x) for x in end.split("-"))
    out: list[str] = []
    y, m = y0, m0
    while (y, m) <= (y1, m1):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default="")
    ap.add_argument("--smoke", action="store_true", help="pull only the last month")
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    from datetime import date

    end = args.end or f"{date.today().year:04d}-{date.today().month:02d}"
    months = _months(args.start, end)
    if args.smoke:
        months = months[-1:]

    done: set[str] = set()
    if OUT.exists():
        with OUT.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                done.add(row["month"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    new_file = not OUT.exists()
    ok = 0
    failed: list[str] = []
    with OUT.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        if new_file:
            w.writeheader()
        pro = get_pool().pro()
        for mth in months:
            if mth in done:
                continue
            try:
                df = pro.broker_recommend(month=mth)
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{mth}: {str(exc)[:100]}")
                logger.warning("failed %s: %s", mth, str(exc)[:100])
                time.sleep(args.sleep)
                continue
            n = 0
            if df is not None and len(df):
                for _, r in df.iterrows():
                    w.writerow(
                        {
                            "month": mth,
                            "broker": r.get("broker", ""),
                            "ts_code": r.get("ts_code", ""),
                            "name": r.get("name", ""),
                        }
                    )
                    n += 1
                fh.flush()
            ok += 1
            logger.info("%s: %d picks", mth, n)
            time.sleep(args.sleep)

    logger.info("done: months_ok=%d failed=%d -> %s", ok, len(failed), OUT)
    for f in failed[:10]:
        logger.warning("  FAIL %s", f)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
