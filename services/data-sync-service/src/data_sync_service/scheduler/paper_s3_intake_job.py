"""Paper-trading S-3 intake cron (G4).

Runs at 17:42 Asia/Shanghai on weekdays — after watchlist_automation
(17:30, scores fresh) and the regular paper intake (17:40), 3 minutes before
paper_trading_update (17:45). Paper-trades today's S-3 backtest candidates
(score>=65 · RS>=50% · regime!=Weak · mainline · panic cooldown) so the
paper record probes the S-3 strategy out of sample.

Idempotent on (symbol, entry_date, side) — re-running the same day is a no-op.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.paper_s3 import run_intake_s3

logger = logging.getLogger(__name__)

JOB_ID = "paper_s3_intake"
# Weekdays 17:42 Asia/Shanghai (after paper_trading_intake 17:40).
CRON_EXPRESSION = "42 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso_utc() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def run() -> None:
    today = _today_iso_utc()
    try:
        summary = run_intake_s3(trade_date=today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("paper_s3_intake failed: %s", exc)
        return

    if "error" in summary:
        insert_record(JOB_ID, success=False, error_message=str(summary.get("error")))
        logger.warning("paper_s3_intake partial: %s", summary)
        return

    insert_record(JOB_ID, success=True)
    logger.info(
        "paper_s3_intake ok: %s -> %d candidates, %d inserted, %d skipped (%s)",
        today,
        summary.get("candidates", 0),
        summary.get("inserted", 0),
        summary.get("skipped", 0),
        summary.get("skippedReasons", {}),
    )
