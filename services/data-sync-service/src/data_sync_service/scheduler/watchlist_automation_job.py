"""Watchlist post-close automation job (17:30 Asia/Shanghai, weekdays)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.watchlist_automation import run_watchlist_automation

logger = logging.getLogger(__name__)

JOB_ID = "watchlist_automation"
CRON_EXPRESSION = "30 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        result = run_watchlist_automation(trigger="scheduled", force=False)
        skipped = bool(result.get("skipped"))
        skip_reason = str(result.get("skipReason") or "")
        # 闸门跳过（非交易日 / close未就绪）不算失败，不推 Bark，watchdog 静默
        is_gate_skip = skipped and skip_reason in ("not_trading_day", "close_sync_not_ready")
        success = (result.get("ok", True) and not skipped) or is_gate_skip
        err_msg = None if is_gate_skip else (result.get("skipReason") if skipped else result.get("error"))
        run_id = result.get("runId") or None
        insert_record(
            JOB_ID,
            success=success,
            last_ts_code=run_id,
            error_message=err_msg,
        )
        if skipped:
            logger.info(
                "watchlist_automation skipped: %s (runId=%s)",
                result.get("skipReason"),
                result.get("runId"),
            )
        else:
            logger.info(
                "watchlist_automation ok: remove=%s alpha=%s runId=%s",
                len(result.get("remove") or []),
                len(result.get("alphaAdd") or []),
                result.get("runId"),
            )
    except Exception as e:
        insert_record(JOB_ID, success=False, error_message=str(e))
        logger.warning("watchlist_automation failed: %s", e)
