"""Watchlist post-close automation job (17:30 Asia/Shanghai, weekdays)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.watchlist_automation import run_watchlist_automation

logger = logging.getLogger(__name__)

JOB_ID = "watchlist_automation"
CRON_EXPRESSION = "30 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_watchlist_automation(trigger="scheduled", force=False)
    if result.get("skipped"):
        logger.info(
            "watchlist_automation skipped: %s (runId=%s)",
            result.get("skipReason"),
            result.get("runId"),
        )
    else:
        logger.info(
            "watchlist_automation ok: remove=%s alpha=%s runId=%s",
            len(result.get("remove") or []),
            len(result.get("alphaAdd") or []),
            result.get("runId"),
        )
