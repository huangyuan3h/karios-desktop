"""Morning Brief scheduled job — News Substrate 2.0 · Track 3.

Two daily runs:
- 08:30 Asia/Shanghai (morning brief — overnight + pre-market)
- 12:30 Asia/Shanghai (midday brief — morning session)
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID_AM = "morning_brief_am"
JOB_ID_PM = "morning_brief_pm"


def build_am_trigger():
    # 08:30 Asia/Shanghai on weekdays
    return CronTrigger(day_of_week="mon-fri", hour=8, minute=30, timezone="Asia/Shanghai")


def build_pm_trigger():
    # 12:30 Asia/Shanghai on weekdays
    return CronTrigger(day_of_week="mon-fri", hour=12, minute=30, timezone="Asia/Shanghai")


def run(brief_type: str = "morning"):
    print(f"[morning-brief] Generating {brief_type} brief...")
    try:
        from data_sync_service.service.morning_brief import generate_brief

        brief = generate_brief(brief_type=brief_type)
        item_count = len(brief.get("items") or [])
        success = item_count > 0
        err_msg = None if success else "No enriched items available for brief"
        insert_record(
            JOB_ID_AM if brief_type == "morning" else JOB_ID_PM,
            success=success,
            last_ts_code=str(item_count),
            error_message=err_msg,
        )
        print(f"[morning-brief] Done: {brief_type} brief with {item_count} items")
    except Exception as e:
        job_id = JOB_ID_AM if brief_type == "morning" else JOB_ID_PM
        insert_record(job_id, success=False, error_message=str(e))
        logger.warning("morning_brief_%s failed: %s", brief_type, e)
