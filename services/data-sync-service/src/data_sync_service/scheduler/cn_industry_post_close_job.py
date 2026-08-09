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
# Weekdays 18:15 Asia/Shanghai. Was 17:35 — eastmoney publishes the daily
# industry fund-flow/mainline data late afternoon (17:30-18:30); 17:35 runs
# failed every weekday with no data yet (2026-08-09 audit). 18:15 sits after
# close_sync 17:10 + watchlist_automation 17:30 + paper_s3_intake 17:42.
CRON_EXPRESSION = "15 18 * * 1-5"
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

    ok = bool(industry.get("ok")) and bool(mainline.get("ok")) and bool(sentiment.get("ok"))
    skipped = bool(industry.get("skipped")) and bool(mainline.get("ok")) and bool(sentiment.get("ok"))

    if not ok:
        err = (
            industry.get("error")
            or mainline.get("error")
            or sentiment.get("error")
            or "unknown"
        )
        # 2026-08-09: include per-part status so a silent part (ok=False with
        # no error field) is diagnosable instead of a bare "unknown".
        detail = {
            "industry": (industry.get("ok"), industry.get("skipped"), industry.get("error")),
            "mainline": (mainline.get("ok"), mainline.get("error")),
            "sentiment": (sentiment.get("ok"), sentiment.get("error")),
        }
        insert_record(JOB_ID, success=False, error_message=f"{err} {detail}")
        logger.warning("cn_industry_post_close_sync failed: industry=%s mainline=%s sentiment=%s", industry, mainline, sentiment)
        return

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