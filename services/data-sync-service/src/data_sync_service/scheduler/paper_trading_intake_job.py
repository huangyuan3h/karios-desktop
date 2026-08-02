"""Paper-trading intake cron (OPT-049).

Runs at 17:40 Asia/Shanghai on weekdays — after close_sync (17:10),
watchlist_automation (17:30), and cn_industry_post_close (17:35). At this
point the decision journal + daily close are both fresh for the day, so we
can record what the BUY/ADD signals WOULD HAVE done for symbols whose live
position is still 0%.

The intake is idempotent on (symbol, entry_date, side) — re-running on the
same day is a no-op.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.paper_trading import run_intake

logger = logging.getLogger(__name__)

JOB_ID = "paper_trading_intake"
# Weekdays 17:40 Asia/Shanghai.
CRON_EXPRESSION = "40 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso_utc() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def run() -> None:
    today = _today_iso_utc()
    try:
        summary = run_intake(trade_date=today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("paper_trading_intake failed: %s", exc)
        return

    if "error" in summary:
        insert_record(JOB_ID, success=False, error_message=str(summary.get("error")))
        logger.warning("paper_trading_intake partial: %s", summary)
        return

    insert_record(JOB_ID, success=True)
    logger.info(
        "paper_trading_intake ok: %s -> %d inserted, %d skipped (%s)",
        today,
        summary.get("inserted", 0),
        summary.get("skipped", 0),
        summary.get("skippedReasons", {}),
    )
