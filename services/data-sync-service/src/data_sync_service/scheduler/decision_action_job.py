"""Decision action tracking job (18:30 Asia/Shanghai, weekdays, TIP-015).

Extract structured actions from the day's decision briefs, match them
against execution journal changes (did the watchlist actually follow?),
and fill price outcomes.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.decision import (
    extract_pending_actions,
    match_executions,
    track_action_outcomes,
)

logger = logging.getLogger(__name__)

JOB_ID = "decision_action_tracking"
CRON_EXPRESSION = "30 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    try:
        extracted = extract_pending_actions()
        matched = match_executions()
        tracked = track_action_outcomes()
        insert_record(JOB_ID, success=True)
        logger.info(
            "decision actions: extracted=%s matched=%s tracked=%s",
            extracted.get("extracted"),
            matched.get("matched"),
            tracked.get("tracked"),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("decision action tracking failed")
        insert_record(JOB_ID, success=False, error_message=str(exc))
