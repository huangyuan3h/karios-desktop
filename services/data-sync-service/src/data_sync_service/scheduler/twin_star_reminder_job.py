"""机会双子星 14:20 刷新当日行情并提醒 — weekdays 14:20 Asia/Shanghai.

Pulls a fresh full-market snapshot (afternoon tape ≈ 14:30 execution price),
re-runs the S-gap screen, then emits webhook + hub with buys AND sells.
"""
from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.webhook import emit_event
from data_sync_service.service.twin_star_daily import (
    build_twin_star_reminder_payload,
    now_cn,
)
from data_sync_service.service.twin_star_intraday import (
    build_intraday_sat,
    cache_intraday_sat,
)

logger = logging.getLogger(__name__)

JOB_ID = "twin_star_reminder"
CRON_EXPRESSION = "20 14 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    today = now_cn().date()
    try:
        sat = build_intraday_sat(today)
        if sat is not None:
            cache_intraday_sat(sat, today)
            logger.info(
                "twin_star_reminder snapshot refreshed: gateOpen=%s candidates=%s",
                sat.get("gateOpen"),
                [c["ts"] for c in (sat.get("candidates") or [])],
            )
        else:
            logger.warning("twin_star_reminder: afternoon snapshot unavailable, using cache/t-1")
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_reminder snapshot refresh failed")
    payload = build_twin_star_reminder_payload(today)
    if not payload.get("detail"):
        return
    try:
        emit_event(
            "twin_star_reminder",
            payload,
            dedupe_key=f"twin_star_reminder:{today.isoformat()}",
        )
        logger.info("twin_star_reminder emitted: %s", payload["detail"])
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_reminder emit failed")


if __name__ == "__main__":
    run()
