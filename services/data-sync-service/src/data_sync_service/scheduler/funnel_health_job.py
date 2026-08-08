"""Watchlist funnel health job (18:10 Asia/Shanghai, weekdays)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.watchlist_funnel_health import JOB_TYPE, check_funnel_health

logger = logging.getLogger(__name__)

JOB_ID = JOB_TYPE
CRON_EXPRESSION = "10 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        result = check_funnel_health()
        logger.info(
            "watchlist_funnel_health ok=%s metrics=%s streak=%s",
            result.get("ok"),
            result.get("metrics"),
            result.get("streak"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("watchlist_funnel_health failed: %s", exc)
