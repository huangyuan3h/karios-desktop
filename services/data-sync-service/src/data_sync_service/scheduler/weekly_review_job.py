"""Weekly review job (2026-08-12, H2).

Monday 07:40 Asia/Shanghai — aggregate last ISO week (Mon..Fri) into the
weekly decision-quality report (weekly_review.build_weekly_review), store
it in morning_briefs (brief_type='weekly-review') so the decision agent /
frontend / ai-service all read the same artifact, and record the run in
sync_job_record like every other job.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID = "weekly_review"
CRON_EXPRESSION = "40 7 * * 1"  # Monday 07:40 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"
BRIEF_TYPE = "weekly-review"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _prev_friday() -> str:
    """The Friday ending the previous ISO week (Monday run → last Friday)."""
    today = datetime.now(tz=UTC).date()
    return (today - timedelta(days=3)).isoformat()


def run() -> dict[str, Any] | None:
    """Generate + store the weekly review for the week ending last Friday."""
    from data_sync_service.db.morning_brief import upsert_brief
    from data_sync_service.service.weekly_review import build_weekly_review

    end_date = _prev_friday()
    try:
        result = build_weekly_review(end_date=end_date)
        md = result.get("markdown") or ""
        brief = upsert_brief(
            brief_date=end_date,
            brief_type=BRIEF_TYPE,
            items=result.get("stats", []),
            macro_overview=None,
            model_version="weekly_review_v1",
            source_item_ids=None,
            markdown=md,
        )
        insert_record(JOB_ID, True, None)
        logger.info("[weekly_review] generated %s (%s) markdown=%d chars", end_date, brief.get("id"), len(md))
        return {"endDate": end_date, "briefId": brief.get("id"), "markdownChars": len(md)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("[weekly_review] failed: %s", exc)
        insert_record(JOB_ID, False, None, error_message=str(exc)[:500])
        return None
