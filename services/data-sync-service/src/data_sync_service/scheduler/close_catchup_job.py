"""Catch-up close sync job to heal missed runs (e.g. sleep)."""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import get_today_run
from data_sync_service.service.close_sync import JOB_TYPE as CLOSE_JOB_TYPE
from data_sync_service.service.close_sync import sync_close

logger = logging.getLogger(__name__)

JOB_ID = "close_sync_catchup"
# Every 10 minutes after close time on weekdays (Asia/Shanghai).
CRON_EXPRESSION = "*/10 17-23 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    # Avoid running when close sync already succeeded today.
    today_run = get_today_run(CLOSE_JOB_TYPE)
    if today_run and bool(today_run.get("success")):
        return

    now_cn = datetime.now(ZoneInfo("Asia/Shanghai"))
    logger.info("close_sync_catchup tick at %s", now_cn.isoformat())
    result = sync_close(exchange="SSE", force=True)
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("close_sync_catchup skipped: %s", result.get("message", ""))
        else:
            logger.info(
                "close_sync_catchup ok: daily=%s adj_factor=%s dates=%s",
                result.get("updated_daily_rows", 0),
                result.get("updated_adj_factor_rows", 0),
                result.get("trade_dates", []),
            )
    else:
        logger.warning("close_sync_catchup failed: %s", result.get("error", "unknown"))

