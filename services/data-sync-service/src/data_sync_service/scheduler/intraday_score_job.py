"""Intraday S-3 score refresh job (10:30 / 14:00 Asia/Shanghai, weekdays).

Writes ``watchlist_score_daily`` for the CN + HK universes with realtime
quotes merged into the last bar (``run_intraday_scores``), so the S-3 health
card shows TODAY's candidates during trading hours — before this job the
intraday decision surface was always empty because scores were only written
by the EOD pass at 17:30.

The EOD chain (17:10 close_sync → 17:30 watchlist_automation → 17:42
paper_s3_intake) still overwrites the same rows with close prices, so the
paper record stays faithful to EOD data.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.watchlist_automation import run_intraday_scores

logger = logging.getLogger(__name__)

JOB_ID = "intraday_score"
# 10:30 (CN/HK morning session established) + 14:00 (afternoon) Asia/Shanghai.
CRON_EXPRESSION = "30 10,14 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        result = run_intraday_scores(trigger="scheduled", force=False)
        success = bool(result.get("ok")) and not bool(result.get("skipped"))
        err_msg = result.get("skipReason") or result.get("cnError") or result.get("hkError")
        insert_record(
            JOB_ID,
            success=success,
            last_ts_code=str(result.get("tradeDate") or None),
            error_message=err_msg,
        )
        if success:
            logger.info(
                "intraday_score ok: cn=%s hk=%s tradeDate=%s",
                result.get("cnScoreSnapshots"),
                result.get("hkScoreSnapshots"),
                result.get("tradeDate"),
            )
        else:
            logger.info("intraday_score skipped/failed: %s", result)
    except Exception as e:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(e))
        logger.warning("intraday_score failed: %s", e)
