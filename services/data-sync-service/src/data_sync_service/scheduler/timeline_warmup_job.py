"""Timeline warmup (2026-08-24) — precompute past-year single-track timeline daily.

08:20 Asia/Shanghai, after close_sync (17:10) next day pre-warms 365d window so
BacktestPage Timeline loads <100ms from file cache instead of ~50s full S-3 replay.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)
JOB_ID = "timeline_warmup"
CRON_EXPRESSION = "20 8 * * 1-5"  # weekdays 08:20
TIMEZONE = "Asia/Shanghai"

def _trigger():
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=ZoneInfo(TIMEZONE))

def run() -> dict:
    from data_sync_service.api.backtest_routes import backtest_timeline as _fn
    today = date.today().isoformat()
    start = (date.today() - timedelta(days=365)).isoformat()
    try:
        result = _fn(start=start, end=today)
        insert_record(job_type="timeline_warmup", success=True, last_ts_code=f"{start}_{today}", error_message=f"warmed {len(result.get('rows',[]))} rows")
        return result
    except Exception as exc:
        insert_record(job_type="timeline_warmup", success=False, last_ts_code="", error_message=str(exc)[:500])
        logger.warning("timeline warmup failed: %s", exc)
        raise

def build_trigger():
    from zoneinfo import ZoneInfo

    from apscheduler.triggers.cron import CronTrigger
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=ZoneInfo(TIMEZONE))
