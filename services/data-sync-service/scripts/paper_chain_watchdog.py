#!/usr/bin/env python3
"""Paper-chain watchdog: verify today's close-chain crons ran, self-heal if not.

Runs weekdays at 18:05 Asia/Shanghai (launchd com.karios.paper-chain), i.e.
after the daily chain:
  17:10 close_sync (daily bars) → 17:30 watchlist_automation (scores) →
  17:40 paper_trading_intake → 17:42 paper_s3_intake (CN+HK) → 17:45 update.

For each missing step it:
  - logs a warning row into sync_job_record (job_type paper_chain_watchdog)
  - re-runs the missing step ONCE (best-effort, guard-railed):
      * watchlist_automation missing → recompute CN+HK universe scores
      * paper_s3_intake missing   → run_intake_s3 CN + HK
      * paper_trading_update missing → run_update
    (only if today's close_sync succeeded, so we never score on stale bars)

This is the "让 paper 贴近回测" execution floor: a missing cron must not
silently create the 3-day gap that produced the 8-05~8-07 drift (2026-08-11
smoke-test incident showed exactly how fragile the chain is).

Usage:
  PYTHONPATH=src python3 scripts/paper_chain_watchdog.py [--dry-run]
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db.sync_job_record import get_today_run, insert_record  # noqa: E402

WATCHDOG_JOB = "paper_chain_watchdog"
CLOSE_JOB = "close_sync"


def _today() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _run_ok(job: str) -> bool:
    """True when today's run for ``job`` succeeded.

    paper_s3_intake records per-market (paper_s3_intake_CN / _HK) so both
    must be checked. ``get_today_run`` compares in UTC — the close chain runs
    at 09:30-09:45 UTC (17:30-17:45 Beijing), same UTC day as this watchdog.
    """
    if job == "paper_s3_intake":
        for m in ("CN", "HK"):
            rec = get_today_run(f"{job}_{m}")
            if not (rec or {}).get("success"):
                return False
        return True
    rec = get_today_run(job)
    return bool((rec or {}).get("success"))


def _main(dry_run: bool) -> int:
    day = _today()
    close_ok = _run_ok(CLOSE_JOB)

    checks = [
        ("watchlist_automation", "score"),
        ("paper_s3_intake", "s3_intake"),
        ("paper_trading_update", "update"),
    ]
    missing = [job for job, _tag in checks if not _run_ok(job)]

    if not missing:
        print(f"[{day}] paper chain OK (close_sync={close_ok})")
        return 0

    if not close_ok:
        print(
            f"[{day}] WARNING: today's close_sync missing → chain broken upstream, "
            f"skipping self-heal for {missing}"
        )
        insert_record(WATCHDOG_JOB, success=False, error_message=f"close_sync missing; missing={missing}")
        return 1

    for job in missing:
        if dry_run:
            print(f"[{day}] WOULD self-heal {job}")
            continue
        try:
            if job == "watchlist_automation":
                from data_sync_service.service.watchlist_automation import run_watchlist_automation

                run_watchlist_automation()
                print(f"[{day}] self-healed watchlist_automation")
            elif job == "paper_s3_intake":
                from data_sync_service.service.paper_s3 import run_intake_s3

                for market in ("CN", "HK"):
                    summary = run_intake_s3(trade_date=day, market=market)
                    print(f"[{day}] self-healed paper_s3_intake[{market}]: {summary.get('inserted', 0)} inserted")
            elif job == "paper_trading_update":
                from data_sync_service.service.paper_trading import run_update

                summary = run_update(today_iso=day)
                print(f"[{day}] self-healed paper_trading_update: {summary}")
        except Exception as exc:  # noqa: BLE001
            print(f"[{day}] self-heal {job} FAILED: {exc}")
            insert_record(WATCHDOG_JOB, success=False, error_message=f"self-heal {job} failed: {exc}")
            continue
        insert_record(WATCHDOG_JOB, success=True, error_message=f"self-healed {job}")

    # after healing, re-check and record the final state
    still_missing = [job for job, _tag in checks if not _run_ok(job)]
    print(f"[{day}] after self-heal, still missing: {still_missing}")
    if still_missing:
        insert_record(WATCHDOG_JOB, success=False, error_message=f"still missing: {still_missing}")
        return 1
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report only, no self-heal")
    args = ap.parse_args()
    raise SystemExit(_main(args.dry_run))
