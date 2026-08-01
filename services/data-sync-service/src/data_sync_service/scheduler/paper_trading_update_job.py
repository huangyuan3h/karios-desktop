"""Paper-trading update cron (OPT-049).

Runs at 17:45 Asia/Shanghai on weekdays — 5 minutes after the intake cron
gives the daily table time to settle. For every open trade, looks up the
latest close, updates pnl_pct / holding_days, and applies v0 close
conditions: ``stop_hit`` (pnl_pct <= -5%) or ``max_hold`` (holding_days
>= 5).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.paper_trading import run_update

logger = logging.getLogger(__name__)

JOB_ID = "paper_trading_update"
# Weekdays 17:45 Asia/Shanghai (after paper_trading_intake 17:40).
CRON_EXPRESSION = "45 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso_utc() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def run() -> None:
    today = _today_iso_utc()
    try:
        summary = run_update(today_iso=today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("paper_trading_update failed: %s", exc)
        return

    if "error" in summary:
        insert_record(JOB_ID, success=False, error_message=str(summary.get("error")))
        logger.warning("paper_trading_update partial: %s", summary)
        return

    insert_record(JOB_ID, success=True)
    logger.info(
        "paper_trading_update ok: %s -> %d scanned, %d updated, %d closed (%s)",
        today,
        summary.get("scanned", 0),
        summary.get("updated", 0),
        summary.get("closed", 0),
        summary.get("closeReasons", {}),
    )
