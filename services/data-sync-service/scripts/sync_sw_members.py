#!/usr/bin/env python3
"""Pull Shenwan industry membership history (申万成分史) via tushare index_member_all.

`index_member_all` returns L1/L2/L3 membership with `in_date` / `out_date` /
`is_new` — i.e. a **point-in-time** industry map, which the repo has been
missing (`docs/todo.md`: "指数成分史缺"). Free on the current token.

Output: data/altdata/sw_members.csv (one row per L1/L2/L3/stock).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_sw_members.py
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

OUT = Path(__file__).resolve().parents[1] / "data" / "altdata" / "sw_members.csv"
COLS = [
    "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
    "ts_code", "name", "in_date", "out_date", "is_new",
]

# Fallback SW2021 L1 codes (used if index_classify is not accessible).
SW_L1_FALLBACK = [
    "801010.SI", "801030.SI", "801040.SI", "801050.SI", "801080.SI",
    "801110.SI", "801120.SI", "801130.SI", "801140.SI", "801150.SI",
    "801160.SI", "801170.SI", "801180.SI", "801200.SI", "801210.SI",
    "801230.SI", "801710.SI", "801720.SI", "801730.SI", "801740.SI",
    "801750.SI", "801760.SI", "801770.SI", "801780.SI", "801790.SI",
    "801880.SI", "801890.SI", "801950.SI", "801960.SI", "801970.SI",
    "801980.SI",
]


def _l1_codes(pro) -> list[str]:
    for src in ("SW2021", "SW"):
        try:
            df = pro.index_classify(level="L1", src=src)
            if df is not None and len(df):
                codes = [str(c) for c in df["index_code"].tolist()]
                logger.info("index_classify(%s) -> %d L1 codes", src, len(codes))
                return codes
        except Exception as exc:  # noqa: BLE001
            logger.warning("index_classify(%s) failed: %s", src, str(exc)[:80])
    logger.info("using hardcoded SW L1 fallback (%d codes)", len(SW_L1_FALLBACK))
    return SW_L1_FALLBACK


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sleep", type=float, default=0.6)
    args = ap.parse_args()

    pro = get_pool().pro()
    codes = _l1_codes(pro)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = 0
    failed: list[str] = []
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS)
        w.writeheader()
        for i, l1 in enumerate(codes):
            try:
                df = pro.index_member_all(l1_code=l1)
            except Exception as exc:  # noqa: BLE001
                failed.append(f"{l1}: {str(exc)[:80]}")
                logger.warning("failed %s: %s", l1, str(exc)[:80])
                time.sleep(args.sleep)
                continue
            n = 0
            if df is not None and len(df):
                for _, r in df.iterrows():
                    w.writerow({k: r.get(k, "") for k in COLS})
                    n += 1
            rows += n
            logger.info("L1 %d/%d %s: %d rows (total %d)", i + 1, len(codes), l1, n, rows)
            time.sleep(args.sleep)

    logger.info("done: rows=%d failed=%d -> %s", rows, len(failed), OUT)
    for f in failed[:10]:
        logger.warning("  FAIL %s", f)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
