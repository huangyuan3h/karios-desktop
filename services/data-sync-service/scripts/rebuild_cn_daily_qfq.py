"""Rebuild A-share daily 2021-2026 as a single consistent qfq series.

LOOK-AHEAD DECLARATION (ledger B7, 2026-09-24): qfq is a function of *future*
dividends/ex-rights, so a whole-series rewrite restates past prices. This is a
deliberate repo convention (a backup is mandatory), but it means frozen backtest
windows can drift across reseeds — always record the data version with a frozen
number (validation-gates-v2 L5).

Root cause fixed here: the stored 2021+ ``daily.close`` was a mix of
standard qfq, stale qfq (old adjustment bases never rescaled), and raw
(unadjusted, mostly BJ 920xxx). This re-fetches raw daily + adj_factor
market-wide and rewrites OHLC as qfq with base = each stock's latest
adj_factor -- the SAME base already used by the 2007-2020 backfill, so
the whole 2007-2026 series is continuous.

qfq_price(t) = raw_price(t) * adj_factor(t) / adj_base
  adj_base = max adj_factor within this fetch (fallback: DB max).

Backup required first: daily_backup_20260911 (created 2026-09-11).

Usage:
    PYTHONPATH=src python3 scripts/rebuild_cn_daily_qfq.py --years 2021-2026
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pandas as pd

DAILY_FIELDS = ["ts_code", "trade_date", "open", "high", "low", "close",
                "pre_close", "change", "pct_chg", "vol", "amount"]
ADJ_FIELDS = ["ts_code", "trade_date", "adj_factor"]


def _fetch_paged(pro, method: str, td: str, fields: list[str]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    offset = 0
    while True:
        df = getattr(pro, method)(
            trade_date=td, offset=offset, limit=5000, fields=",".join(fields)
        )
        if df is None or df.empty:
            break
        parts.append(df)
        if len(df) < 5000:
            break
        offset += len(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=fields)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2021-2026")
    args = ap.parse_args()
    y0, y1 = (int(x) for x in args.years.split("-"))

    from data_sync_service.clients.tushare_pool import get_pool
    from data_sync_service.db import get_connection
    from data_sync_service.db.daily import (
        update_adj_factor_from_dataframe,
        upsert_from_dataframe,
    )

    pro = get_pool().pro()
    with get_connection() as conn:
        db_max = pd.read_sql(
            "SELECT ts_code, max(adj_factor) AS base FROM daily "
            "WHERE adj_factor IS NOT NULL GROUP BY ts_code",
            conn,
        )
    db_base = dict(zip(db_max["ts_code"], db_max["base"].astype(float)))

    cal = pro.trade_cal(exchange="SSE", start_date=f"{y0}0101",
                        end_date=f"{y1}1231", is_open="1")
    dates = sorted(cal["cal_date"].tolist())
    print(f"trade dates: {len(dates)} ({dates[0]}..{dates[-1]})")

    daily_parts, adj_parts = [], []
    t0 = time.time()
    for i, td in enumerate(dates):
        d = _fetch_paged(pro, "daily", td, DAILY_FIELDS)
        a = _fetch_paged(pro, "adj_factor", td, ADJ_FIELDS)
        if not d.empty:
            daily_parts.append(d)
        if not a.empty:
            adj_parts.append(a)
        if (i + 1) % 200 == 0:
            print(f"  fetched {i + 1}/{len(dates)} dates ({time.time() - t0:.0f}s)",
                  flush=True)

    daily = pd.concat(daily_parts, ignore_index=True)
    adj = pd.concat(adj_parts, ignore_index=True)
    del daily_parts, adj_parts
    print(f"fetched daily={len(daily)} adj={len(adj)} in {time.time() - t0:.0f}s")

    fetch_base = adj.groupby("ts_code")["adj_factor"].max().to_dict()
    adj["_base"] = adj["ts_code"].map(lambda c: fetch_base.get(c) or db_base.get(c))
    m = daily.merge(adj[["ts_code", "trade_date", "adj_factor", "_base"]],
                    on=["ts_code", "trade_date"], how="left")
    m = m[m["adj_factor"].notna() & (m["_base"] > 0)].copy()
    f = m["adj_factor"] / m["_base"]
    for c in ("open", "high", "low", "close", "pre_close", "change"):
        m[c] = pd.to_numeric(m[c], errors="coerce") * f
    for c in ("vol", "amount", "pct_chg"):
        m[c] = pd.to_numeric(m[c], errors="coerce")
    print(f"rows to upsert: {len(m)}")

    out = m[DAILY_FIELDS]
    step = 200_000
    n = 0
    for j in range(0, len(out), step):
        n += upsert_from_dataframe(out.iloc[j:j + step])
        print(f"  upsert {min(j + step, len(out))}/{len(out)}", flush=True)
    af = adj[ADJ_FIELDS]
    for j in range(0, len(af), step):
        update_adj_factor_from_dataframe(af.iloc[j:j + step])
    print(f"done: upserted {n} rows, adj_factor rows {len(af)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
