"""Late-evening watchlist retry (weekdays 20:30 Asia/Shanghai).

Root cause of the 17:30 skip rate (2026-09): close_sync (17:10 cron)
regularly finishes AFTER 17:30 on heavy days (2026-09-30: 20:41 Beijing),
so the 17:30 watchlist_automation precheck bails with
``close_sync_not_ready``. The 18:05 paper_chain_watchdog heals most days,
but on slow-close days close still is not ready at 18:05 either — the pool
then waits for the 18:40 bar_5min post_5min refresh.

This job is the last net: at 20:30 close is virtually always landed. It
runs ONLY when today is open, close succeeded, and no applied (non-skipped)
pool exists for today — idempotent, never scores on stale bars.
"""

from __future__ import annotations

import logging
from datetime import date as _date
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import get_today_run, insert_record

logger = logging.getLogger(__name__)

JOB_ID = "watchlist_automation_retry"
CRON_EXPRESSION = "30 20 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _cn_today() -> str:
    return _date.today().isoformat()


def _cstonight() -> str:
    from datetime import datetime

    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def run() -> None:
    day = _cstonight()
    try:
        from data_sync_service.db.trade_calendar import is_trading_day

        if is_trading_day("SSE", _date.fromisoformat(day)) is not True:
            insert_record(JOB_ID, success=True, error_message="not a trading day — skip")
            return
    except Exception:  # noqa: BLE001
        pass
    try:
        from data_sync_service.db.watchlist_automation import automation_applied_on

        if automation_applied_on(day):
            insert_record(JOB_ID, success=True, error_message="already applied — skip")
            return
    except Exception:  # noqa: BLE001
        pass
    close_ok = bool((get_today_run("stock_close_sync") or {}).get("success")) or bool(
        (get_today_run("close_sync") or {}).get("success")
    )
    if not close_ok:
        insert_record(JOB_ID, success=False, error_message="close_sync not ready — skip")
        logger.info("watchlist retry skipped: close_sync not ready for %s", day)
        return
    try:
        from data_sync_service.service.watchlist_automation import run_watchlist_automation

        result = run_watchlist_automation(trigger="retry_2030", force=False)
        if result.get("skipped"):
            insert_record(JOB_ID, success=True, error_message=str(result.get("skipReason")))
        else:
            insert_record(JOB_ID, success=True, last_ts_code=str(result.get("runId") or ""))
        logger.info("watchlist retry done for %s: skipped=%s", day, result.get("skipped"))
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        logger.warning("watchlist retry failed: %s", exc)
