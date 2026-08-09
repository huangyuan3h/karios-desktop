#!/usr/bin/env python3
"""Retry the industry fund-flow history backfill until it completes.

eastmoney push2his fflow has been flaky since 2026-08-07 (RemoteDisconnected,
sometimes OK for hours, sometimes dead). The backfill upsert is idempotent,
so the safest path is a slow retry loop: run one serial backfill round, check
how much pre-2025-12-15 history is now in the table, wait, and repeat until
the target is met or --max-hangs consecutive no-progress rounds pass.

Usage:
  nohup PYTHONPATH=src python3 scripts/retry_backfill_until_done.py \
      --since 2024-07-01 --interval-s 900 --max-hangs 20 \
      --log /tmp/backfill_retry.log &
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

SCRIPT = Path(__file__).resolve().parents[0] / "backfill_industry_flow_history.py"
PROBE_URL = (
    "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    "?lmt=0&klt=101&fields1=f1,f2,f3,f7&fields2=f51,f52,f53"
    "&ut=b2884a393a59ad64002292a3e90d46a5&secid=90.BK0475"
)


def _rows_before(since: str) -> int:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM market_cn_industry_fund_flow_daily WHERE date < %s",
                (since,),
            )
            return int(cur.fetchone()[0])


def _probe(timeout_s: float = 6.0) -> bool:
    req = urllib.request.Request(
        PROBE_URL,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Referer": "https://data.eastmoney.com/",
            "Connection": "close",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return resp.status == 200
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2025-12-15", help="Rows with date < this count as 'history' (cron start date)")
    ap.add_argument("--fetch-since", default="2024-07-01", help="Backfill start date passed to the backfill script")
    ap.add_argument("--interval-s", type=int, default=900, help="Wait between rounds")
    ap.add_argument("--max-hangs", type=int, default=24, help="Give up after N rounds with zero progress")
    args = ap.parse_args()

    target = date.fromisoformat(args.fetch_since)
    hangs = 0
    while True:
        rows = _rows_before(args.since)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] history rows now: {rows}", flush=True)
        if not _probe():
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] probe dead — skipping round", flush=True)
            hangs += 1
            if hangs >= args.max_hangs:
                print(f"no progress for {args.max_hangs} rounds — giving up", flush=True)
                return 1
            time.sleep(args.interval_s)
            continue
        proc = subprocess.run(
            [sys.executable, str(SCRIPT), "--since", str(target), "--rounds", "2", "--workers", "1"],
            cwd=str(SCRIPT.parents[1]),
            env={**__import__("os").environ, "PYTHONPATH": str(SCRIPT.parents[1] / "src")},
        )
        rows2 = _rows_before(args.since)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] after round: {rows2} "
              f"(+{rows2 - rows}), backfill rc={proc.returncode}", flush=True)
        if rows2 > rows:
            hangs = 0
        else:
            hangs += 1
            if hangs >= args.max_hangs:
                print(f"no progress for {args.max_hangs} rounds — giving up", flush=True)
                return 1
        time.sleep(args.interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
