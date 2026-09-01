"""机会双子星 (Opportunity Twin-Star) 14:30 前操作提醒 — weekdays 14:20 Asia/Shanghai.

Emits a webhook event `twin_star_reminder` (dedupe per day) with the core
pick-strong target + S-gap satellite gate/candidates, so the user can act
before 14:30 (A-share close-30min). Also visible on-demand via
GET /api/twin-star/action and the notifications hub.
"""
from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.webhook import emit_event
from data_sync_service.service.twin_star_daily import (
    build_twin_star_reminder_payload,
    now_cn,
)

logger = logging.getLogger(__name__)

JOB_ID = "twin_star_reminder"
CRON_EXPRESSION = "20 14 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    payload = build_twin_star_reminder_payload()
    if not payload.get("detail"):
        return
    today = now_cn().date().isoformat()
    try:
        emit_event("twin_star_reminder", payload, dedupe_key=f"twin_star_reminder:{today}")
        logger.info("twin_star_reminder emitted: %s", payload["detail"])
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_reminder emit failed")


if __name__ == "__main__":
    run()