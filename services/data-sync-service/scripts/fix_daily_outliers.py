"""Targeted fix: rebuild specific stocks' daily history as clean qfq.

Some legacy rows (delisted / long-suspended / BJ) kept stale adj bases or
raw prices and were not covered by the market-wide rebuild. For each code,
re-fetch its full raw daily + adj_factor, recompute qfq with
base = that stock's latest adj_factor, delete its rows, and re-insert.

Usage:
    PYTHONPATH=src python3 scripts/fix_daily_outliers.py 000004.SZ 600381.SH ...
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

DAILY_FIELDS = ["ts_code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "vol", "amount"]


def main(codes: list[str]) -> int:
    from data_sync_service.clients.tushare_pool import get_pool
    from data_sync_service.db import get_connection
    from data_sync_service.db.daily import (
        update_adj_factor_from_dataframe,
        upsert_from_dataframe,
    )

    pro = get_pool().pro()
    for ts in codes:
        raw = pro.daily(ts_code=ts, start_date="20070101")
        adj = pro.adj_factor(ts_code=ts, start_date="20070101")
        if raw is None or raw.empty or adj is None or adj.empty:
            print(f"  {ts}: no data, skip")
            continue
        m = raw.merge(adj, on=["ts_code", "trade_date"], how="inner")
        base = float(m["adj_factor"].max())
        if base <= 0:
            print(f"  {ts}: bad base, skip")
            continue
        f = m["adj_factor"] / base
        for c in ("open", "high", "low", "close", "pre_close", "change"):
            m[c] = pd.to_numeric(m[c], errors="coerce") * f
        for c in ("vol", "amount", "pct_chg"):
            m[c] = pd.to_numeric(m[c], errors="coerce")
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily WHERE ts_code = %s", (ts,))
            conn.commit()
        upsert_from_dataframe(m[DAILY_FIELDS])
        update_adj_factor_from_dataframe(m[["ts_code", "trade_date", "adj_factor"]])
        print(f"  {ts}: rebuilt {len(m)} rows (base={base:.4f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
