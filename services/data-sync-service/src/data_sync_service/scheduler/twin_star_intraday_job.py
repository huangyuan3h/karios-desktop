"""机会双子星盘中近似信号 — weekdays 12:30 Asia/Shanghai.

Pulls a full-market quote snapshot at the lunch break (12:30, price static),
treats the snapshot price as today's simulated close, re-runs the S-gap
satellite screen (R-wide breadth / gap / low-vol 33% / limit-locked filter),
and caches the result to data/twin_star_intraday/{date}.json so
GET /api/twin-star/action can serve the "buy at 14:30 at (approx) close"
candidates after 14:30. Falls back to the t-1 signal when the snapshot fails.
"""
from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.twin_star_intraday import (
    build_intraday_sat,
    cache_intraday_sat,
)

logger = logging.getLogger(__name__)

JOB_ID = "twin_star_intraday"
CRON_EXPRESSION = "30 12 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        sat = build_intraday_sat()
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_intraday build failed")
        return
    if sat is None:
        logger.warning("twin_star_intraday: no signal built (snapshot/data unavailable)")
        return
    cache_intraday_sat(sat)
    logger.info(
        "twin_star_intraday cached: gateOpen=%s breadth=%s gapCount=%s candidates=%s",
        sat.get("gateOpen"),
        sat.get("breadth"),
        sat.get("gapCount"),
        [c["ts"] for c in (sat.get("candidates") or [])],
    )
    _emit_reminder(sat)


def _emit_reminder(sat: dict) -> None:
    """Push the intraday (simulated-close) signal to webhook (Bark) + hub.

    Uses a distinct dedupe key so the 14:20 t-1 reminder does not override it;
    the 14:20 job itself skips when today's intraday cache exists.
    """
    from data_sync_service.db.webhook import emit_event
    from data_sync_service.service.twin_star_daily import (
        build_twin_star_reminder_payload,
        now_cn,
    )

    try:
        today = now_cn().date()
        payload = build_twin_star_reminder_payload(today)
        if not payload.get("detail"):
            return
        emit_event(
            "twin_star_reminder",
            payload,
            dedupe_key=f"twin_star_intraday:{today.isoformat()}",
        )
        logger.info("twin_star_intraday reminder emitted: %s", payload.get("detail"))
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_intraday reminder emit failed")


if __name__ == "__main__":
    run()