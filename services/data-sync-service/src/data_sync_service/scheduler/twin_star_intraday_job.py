"""机会双子星盘中近似信号 — weekdays 12:30 Asia/Shanghai.

Lunch-break snapshot only (preview cache). The 14:20 reminder job refreshes
the snapshot with afternoon prices before the user trades. No webhook here —
a 12:30 ping would fire two hours early and block the 14:20 sell/buy alert.
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


if __name__ == "__main__":
    run()
