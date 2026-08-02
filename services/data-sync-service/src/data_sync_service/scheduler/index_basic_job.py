"""Full sync of index_dailybasic (market breadth) on weekdays at 17:15 Asia/Shanghai.

Runs independently of the close_sync chain so that macro_snapshot.market_breadth
(turnover_rate / float_mv from index_dailybasic) is warm before the dashboard
17:35 reads without relying on the user clicking "Sync all".
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.index_basic import sync_index_basic_full

logger = logging.getLogger(__name__)

JOB_ID = "index_basic_sync"
# Weekdays 17:15 Asia/Shanghai (after close_sync at 17:10, before 17:35 post-close chain).
CRON_EXPRESSION = "15 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_index_basic_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("index_basic_sync skipped: %s", result.get("message", ""))
        else:
            logger.info(
                "index_basic_sync ok: updated=%s",
                result.get("updated", 0),
            )
    else:
        logger.warning("index_basic_sync failed: %s", result.get("error", "unknown"))