"""Sync ETF daily bars after market close.

- ``sleeve_etf_daily_sync``: the 5 Twin-Star core-leg ETFs (GOLD/OIL/NASDAQ×2/
  BOND10) every weekday 17:25 Asia/Shanghai — the core-leg decision depends on
  fresh mom60/MA200 (GOLD/BOND10 went stale for 7+ days under the monthly cron).
- ``etf_daily_full_sync``: full-market fund_daily stays on its monthly cron
  (rate-limit prone; sleeve sync covers the decision path).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.service.etf_daily import sync_etf_daily_full, sync_sleeve_etfs

logger = logging.getLogger(__name__)

JOB_ID = "sleeve_etf_daily_sync"
# Weekdays 17:25 Asia/Shanghai (after close_sync 17:10 + daily_basic 17:20).
CRON_EXPRESSION = "25 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = sync_sleeve_etfs()
    if result.get("ok"):
        logger.info("sleeve_etf_daily_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("sleeve_etf_daily_sync failed: %s", result.get("error", "unknown"))


def run_full() -> None:
    result = sync_etf_daily_full()
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("etf_daily_full_sync skipped: already synced today")
        else:
            logger.info("etf_daily_full_sync ok: updated=%s", result.get("updated", 0))
    else:
        logger.warning("etf_daily_full_sync failed: %s", result.get("error", "unknown"))


FULL_JOB_ID = "etf_daily_full_sync"
FULL_CRON_EXPRESSION = "0 19 1 * *"