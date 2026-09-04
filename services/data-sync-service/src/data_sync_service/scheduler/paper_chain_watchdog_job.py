"""Paper-chain watchdog (2026-08-11, migrated from launchd → apscheduler).

Weekdays 18:05 Asia/Shanghai — verify today's close-chain crons ran
(watchlist_automation 17:30 / paper_s3_intake CN+HK 17:42 /
paper_trading_update 17:45); self-heal any missing step IF today's
close_sync succeeded (never score on stale bars).

MIGRATION NOTE (2026-08-11): this used to run via launchd
(~/Library/LaunchAgents/com.karios.paper-chain.plist) whose
StartCalendarInterval did NOT fire reliably (runs=1 on load day, no
18:05 trigger). The same uvicorn scheduler that runs the three chain
crons (and proved reliable today) is the more observable home: cron
in Asia/Shanghai, results in sync_job_record like every other job.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import get_today_run, insert_record

logger = logging.getLogger(__name__)

JOB_ID = "paper_chain_watchdog"
CRON_EXPRESSION = "5 18 * * 1-5"  # weekdays 18:05 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"
CLOSE_JOB = "stock_close_sync"
CLOSE_JOB_ALIASES = ("stock_close_sync", "close_sync")


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _run_ok(job: str) -> bool:
    """True when today's run for ``job`` succeeded.

    paper_s3_intake records per-market (paper_s3_intake_CN / _HK) so both
    must be checked. ``get_today_run`` compares in UTC — the close chain runs
    at 09:30-09:45 UTC (17:30-17:45 Beijing), same UTC day as this watchdog.
    close_sync is stored as stock_close_sync (plus legacy alias close_sync).
    """
    if job == "paper_s3_intake":
        for m in ("CN", "HK"):
            rec = get_today_run(f"{job}_{m}")
            if not (rec or {}).get("success"):
                return False
        return True
    if job in CLOSE_JOB_ALIASES or job == CLOSE_JOB:
        for alias in CLOSE_JOB_ALIASES:
            rec = get_today_run(alias)
            if bool((rec or {}).get("success")):
                return True
        return False
    rec = get_today_run(job)
    return bool((rec or {}).get("success"))


def _self_heal(job: str, day: str) -> bool:
    """Run the missing step. Returns True when it succeeded."""
    try:
        if job == "watchlist_automation":
            from data_sync_service.service.watchlist_automation import run_watchlist_automation

            run_watchlist_automation()
        elif job == "paper_s3_intake":
            from data_sync_service.service.paper_s3 import run_intake_s3

            for market in ("CN", "HK"):
                run_intake_s3(trade_date=day, market=market)
        elif job == "paper_trading_update":
            from data_sync_service.service.paper_trading import run_update

            run_update(today_iso=day)
    except Exception as exc:  # noqa: BLE001
        logger.warning("paper_chain self-heal %s failed: %s", job, exc)
        insert_record(JOB_ID, success=False, error_message=f"self-heal {job} failed: {exc}")
        return False
    insert_record(JOB_ID, success=True, error_message=f"self-healed {job}")
    return True


def run() -> None:
    day = _today()
    close_ok = _run_ok(CLOSE_JOB)
    checks = [
        ("watchlist_automation", "score"),
        ("paper_s3_intake", "s3_intake"),
        ("paper_trading_update", "update"),
    ]
    missing = [job for job, _tag in checks if not _run_ok(job)]

    from data_sync_service.db.webhook import emit_event

    if not missing:
        insert_record(JOB_ID, success=True, last_ts_code=f"{day}|ok")
        logger.info("paper chain OK (close_sync=%s)", close_ok)
        return

    emit_event(
        "paper_chain_issue",
        {"day": day, "missing": missing, "close_sync_ok": close_ok},
        dedupe_key=f"paper_chain:{day}",
    )
    if not close_ok:
        # self-heal close first (idempotent, catches yesterday→today gap)
        try:
            from data_sync_service.scheduler.close_sync_job import run as run_close

            run_close()
            close_ok = _run_ok(CLOSE_JOB)
        except Exception as exc:  # noqa: BLE001
            logger.warning("paper_chain self-heal close_sync failed: %s", exc)
        if not close_ok:
            insert_record(
                JOB_ID, success=False,
                error_message=f"close_sync missing; missing={missing}",
            )
            logger.warning("close_sync missing → skipping self-heal for %s", missing)
            return

    for job in list(missing):
        _self_heal(job, day)

    still_missing = [job for job, _tag in checks if not _run_ok(job)]
    logger.info("after self-heal, still missing: %s", still_missing)
    if still_missing:
        insert_record(JOB_ID, success=False, error_message=f"still missing: {still_missing}")
