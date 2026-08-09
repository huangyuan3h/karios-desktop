#!/usr/bin/env python3
"""Backfill market_cn_industry_fund_flow_daily history from Eastmoney.

The post-close sync only backfills 10 days per industry per run, so the
fund-flow table starts at the date the cron began (2025-12-15). Backtest
windows before that (train 2025-08..2025-12, OOS2 2024-08..2025-08) ran with
the mainline/flow gates failing open — this script pulls the FULL daykline
history per SW L1 industry (Eastmoney `push2his` board fund-flow daykline,
same source the cron uses) and upserts it (idempotent).

Usage:
  PYTHONPATH=src python3 scripts/backfill_industry_flow_history.py [--since 2024-07-01]
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.db.industry_fund_flow import upsert_daily_rows  # noqa: E402
from data_sync_service.service.industry_fund_flow import (  # noqa: E402
    _eastmoney_board_fund_flow_daykline,
    _with_retry,
)


def _distinct_sw_l1_rows() -> list[tuple[str, str]]:
    """(industry_code, industry_name) pairs — SW L1 granularity, BK codes first."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT industry_code, industry_name
                FROM market_cn_industry_fund_flow_daily
                WHERE industry_name IS NOT NULL AND industry_name != ''
                ORDER BY industry_name
                """
            )
            rows = cur.fetchall()
    out: dict[str, tuple[str, str]] = {}
    for code, name in rows:
        if not name:
            continue
        key = str(name)
        bk = str(code or "").startswith("BK")
        if key not in out or bk:
            out[key] = (str(code or ""), str(name))
    return list(out.values())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2024-07-01", help="Keep rows with date >= this (YYYY-MM-DD)")
    ap.add_argument("--workers", type=int, default=1, help="Serial is safer: eastmoney push2his throttles concurrent bursts")
    ap.add_argument("--rounds", type=int, default=4, help="Re-run failing industries up to this many rounds")
    args = ap.parse_args()

    since = date.fromisoformat(args.since)
    pairs = _distinct_sw_l1_rows()
    print(f"industries to backfill: {len(pairs)}")

    total_rows = 0
    per_industry: list[tuple[str, int, str, str]] = []

    def fetch_one(item: tuple[str, str]) -> tuple[str, int, str, str] | str:
        code, name = item
        secid = f"90.{code}" if code.startswith("BK") else ""
        try:
            items = _with_retry(
                lambda: _eastmoney_board_fund_flow_daykline(secid=secid),
                tries=5,
                base_sleep_s=1.0,
                max_sleep_s=6.0,
            )
        except Exception as exc:  # noqa: BLE001
            return f"{name}: {exc}"
        kept = [h for h in items if h.get("date") and date.fromisoformat(h["date"]) >= since]
        if not kept:
            return name
        rows = []
        for h in kept:
            rows.append(
                {
                    "date": h["date"],
                    "industry_code": code,
                    "industry_name": name,
                    "net_inflow": h["net_inflow"],
                    "updated_at": "",
                    "source": "backfill",
                }
            )
        upsert_daily_rows(rows)
        return name, len(rows), kept[0]["date"], kept[-1]["date"]

    pending = list(pairs)
    for round_no in range(1, args.rounds + 1):
        if not pending:
            break
        failures: list[str] = []
        print(f"\n--- round {round_no}/{args.rounds} ({len(pending)} industries) ---")
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(fetch_one, p): p for p in pending}
            for fut in as_completed(futures):
                res = fut.result()
                if isinstance(res, str):
                    failures.append(res)
                    print(f"  FAIL {res}")
                else:
                    name, n, d0, d1 = res
                    per_industry.append((name, n, d0, d1))
                    total_rows += n
                    print(f"  {name:24s} +{n:5d} rows  {d0} .. {d1}")
        pending = failures

    print(f"\ntotal backfilled rows: {total_rows}  (industries done: {len(per_industry)}/{len(pairs)})")
    if pending:
        print("still failing:")
        for f in pending:
            print("  ", f)
    return 0 if not pending else 1


if __name__ == "__main__":
    raise SystemExit(main())
