"""CN industry / mainline / sentiment post-close sync at 17:35 Asia/Shanghai (weekdays).

Drives the Dashboard 顶部 industry-fund-flow + mainline + sentiment blocks
(per docs/modules/industry-flow.md and market-sentiment.md "盘后每日更新").
Previously only triggered by the user clicking "Sync all (force)" on Dashboard.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.industry_fund_flow import sync_cn_industry_fund_flow
from data_sync_service.service.mainline import sync_cn_industry_mainline
from data_sync_service.service.market_sentiment import sync_cn_sentiment

logger = logging.getLogger(__name__)

JOB_ID = "cn_industry_post_close_sync"
# Weekdays 17:35 Asia/Shanghai (after close_sync 17:10 + watchlist_automation 17:30).
CRON_EXPRESSION = "35 17 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso_utc() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def run() -> None:
    today = _today_iso_utc()
    industry = sync_cn_industry_fund_flow(days=10, top_n=10)
    mainline = sync_cn_industry_mainline(force=False)
    sentiment = sync_cn_sentiment(date_str=today, force=False)

    # Contract: the three sync services only set an "error" key on failure.
    # Success dicts carry asOfDate/rows/items (no "ok" key); skips carry a
    # "skipped" flag. Judging success by "ok" always fails (regression 2026-08-06).
    results = {"industry": industry, "mainline": mainline, "sentiment": sentiment}
    failed = {
        name: str(r.get("error"))
        for name, r in results.items()
        if not isinstance(r, dict) or r.get("error")
    }
    if failed:
        first_name, first_err = next(iter(failed.items()))
        insert_record(JOB_ID, success=False, error_message=f"{first_name}: {first_err}")
        logger.warning("cn_industry_post_close_sync failed: %s", failed)
        return

    skipped = any(isinstance(r, dict) and bool(r.get("skipped")) for r in results.values())
    insert_record(JOB_ID, success=True)
    if skipped:
        logger.info(
            "cn_industry_post_close_sync skipped: industry reason=%s",
            industry.get("reason") or industry.get("message") or "",
        )
    else:
        logger.info(
            "cn_industry_post_close_sync ok: industry asOfDate=%s mainline asOfDate=%s sentiment asOfDate=%s",
            industry.get("asOfDate"),
            mainline.get("asOfDate"),
            sentiment.get("asOfDate"),
        )