"""Backfill CN raw financial statements (balancesheet/income/cashflow) 2007-2017.

Market-wide ``period`` queries are not permitted for this token, so fetch
per stock with pagination (default page = 100), restricted to the ann_date
window 2007-01-01..2018-06-30 and kept to end_date <= 2017-12-31 (2018+
already stored). Idempotent via upsert_rows; resumable via --offset/--limit.

Usage:
    PYTHONPATH=src python3 scripts/backfill_cn_financials_history.py --offset 0 --limit 800
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

ANN_START = "20070101"
ANN_END = "20180630"
END_MIN = "20060101"
END_MAX = "20171231"


def _codes() -> list[str]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code FROM stock_basic "
                "WHERE (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ') "
                "AND (list_date IS NULL OR list_date <= '2018-01-01') "
                "ORDER BY ts_code"
            )
            return [str(r[0]) for r in cur.fetchall() if r[0]]


def _fetch(pro, endpoint: str, ts_code: str) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    while True:
        df = getattr(pro, endpoint)(
            ts_code=ts_code, start_date=ANN_START, end_date=ANN_END,
            offset=offset, limit=100,
        )
        if df is None or df.empty:
            break
        clean = df.where(pd.notna(df), None)
        for rec in clean.to_dict("records"):
            e = str(rec.get("end_date") or "")
            if END_MIN <= e <= END_MAX:
                rows.append(rec)
        if len(df) < 100:
            break
        offset += 100
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=800)
    ap.add_argument("--sleep", type=float, default=0.3)
    args = ap.parse_args()

    from data_sync_service.clients.tushare_pool import get_pool
    from data_sync_service.db import cn_balance, cn_cashflow, cn_income

    pro = get_pool().pro()
    for mod in (cn_balance, cn_income, cn_cashflow):
        mod.ensure_table()

    all_codes = _codes()
    codes = all_codes[args.offset:args.offset + args.limit]
    print(f"universe {len(all_codes)}; batch offset={args.offset} n={len(codes)}")
    totals = {"bal": 0, "inc": 0, "cf": 0}
    t0 = time.time()
    for i, ts in enumerate(codes):
        try:
            totals["bal"] += cn_balance.upsert_rows(_fetch(pro, "balancesheet", ts))
            totals["inc"] += cn_income.upsert_rows(_fetch(pro, "income", ts))
            totals["cf"] += cn_cashflow.upsert_rows(_fetch(pro, "cashflow", ts))
        except Exception as e:  # noqa: BLE001
            print(f"  {ts} FAILED: {e}")
            time.sleep(1.0)
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(codes)} bal={totals['bal']} inc={totals['inc']} "
                  f"cf={totals['cf']} ({time.time() - t0:.0f}s)", flush=True)
        time.sleep(args.sleep)
    print(f"done batch: {totals} in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
