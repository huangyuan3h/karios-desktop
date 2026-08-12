"""Webhook delivery job (todo §14 #3 · P1 · 2026-08-12).

Drains pending webhook deliveries every minute (HMAC-signed POSTs, retry
5/15/60 min x3, 30/min per subscription burst guard). Cheapest and most
useful cadence: minute-tick keeps intraday alarms (E3) within a minute of
their emit point.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.webhook_delivery import deliver_pending

logger = logging.getLogger(__name__)

JOB_ID = "webhook_delivery"
CRON_EXPRESSION = "* * * * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        result = deliver_pending()
        if result.get("failed") or result.get("blocked"):
            logger.warning(
                "webhook delivery: %s ok, %s failed, %s blocked",
                result.get("delivered"),
                result.get("failed"),
                result.get("blocked"),
            )
    except Exception:  # noqa: BLE001
        logger.exception("webhook delivery job failed")
