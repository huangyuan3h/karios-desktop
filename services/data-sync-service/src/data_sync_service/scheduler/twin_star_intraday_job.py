"""机会双子星盘中全市场快照 — weekdays 09:30–15:00 every minute Asia/Shanghai.

Each tick pulls East Money clist and rebuilds the S-gap screen using that
tape as today's last bar. 15:00 freezes the file until 09:00 the next day.
No webhook here — 14:20 reminder job still emits the trade ping.

After 12:30 a missing/stale session file is recorded as a trading-job
failure (once per day) so Watchlist + notifications can see it. Lookback
from yesterday is not a success.
"""
from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.service.twin_star_intraday import (
    SNAPSHOT_EXPECT_MIN,
    in_live_tape_window,
    intraday_snapshot_status,
    maybe_refresh_intraday_sat,
    now_cn,
)

logger = logging.getLogger(__name__)

JOB_ID = "twin_star_intraday"
CRON_EXPRESSION = "* 9-15 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _record_once(*, success: bool, error: str | None = None) -> None:
    """Write at most one success and one failure per UTC day (job ticks every minute)."""
    try:
        from data_sync_service.db.sync_job_record import get_today_run, insert_record

        prev = get_today_run(JOB_ID)
        if success and prev and prev.get("success"):
            return
        if not success and prev and not prev.get("success"):
            return
        insert_record(JOB_ID, success=success, error_message=(error or "")[:500] or None)
    except Exception:  # noqa: BLE001
        logger.exception("twin_star_intraday: job record failed")


def run() -> None:
    now = now_cn()
    if not in_live_tape_window(now):
        return
    try:
        sat = maybe_refresh_intraday_sat(now=now)
    except Exception as exc:  # noqa: BLE001
        logger.exception("twin_star_intraday refresh failed")
        mins = now.hour * 60 + now.minute
        if mins >= SNAPSHOT_EXPECT_MIN:
            _record_once(success=False, error=str(exc))
        return
    status = intraday_snapshot_status(now=now)
    if status.get("required") and not status.get("ok"):
        logger.warning(
            "twin_star_intraday: session snapshot failed reason=%s sat=%s",
            status.get("reason"),
            sat is not None,
        )
        _record_once(
            success=False,
            error=str(status.get("reason") or "snapshot/data unavailable"),
        )
        return
    if sat is None:
        logger.warning("twin_star_intraday: no signal built (snapshot/data unavailable)")
        return
    _record_once(success=True)
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
