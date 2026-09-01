"""机会双子星盘中全市场快照 — weekdays 09:30–15:00 every minute Asia/Shanghai.

Each tick pulls East Money clist and rebuilds the S-gap screen using that
tape as today's last bar. 15:00 freezes the file until 09:00 the next day.
No webhook here — 14:20 reminder job still emits the trade ping.
"""
from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.twin_star_intraday import (
    in_live_tape_window,
    maybe_refresh_intraday_sat,
    now_cn,
)

logger = logging.getLogger(__name__)

JOB_ID = "twin_star_intraday"
CRON_EXPRESSION = "* 9-15 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    now = now_cn()
    if not in_live_tape_window(now):
        return
    try:
        sat = maybe_refresh_intraday_sat(now=now)
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_intraday refresh failed")
        return
    if sat is None:
        logger.warning("twin_star_intraday: no signal built (snapshot/data unavailable)")
        return
    logger.info(
        "twin_star_intraday cached: gateOpen=%s breadth=%s gapCount=%s frozen=%s candidates=%s",
        sat.get("gateOpen"),
        sat.get("breadth"),
        sat.get("gapCount"),
        sat.get("frozen"),
        [c["ts"] for c in (sat.get("candidates") or [])],
    )


if __name__ == "__main__":
    run()
