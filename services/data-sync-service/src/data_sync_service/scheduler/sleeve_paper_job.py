"""Sleeve auto-configuration for the paper book (T6 · 2026-08-21 落地).

Weekdays 18:20 Asia/Shanghai — after close_sync (17:10), watchlist_automation
(17:30) and cn_industry_post_close (17:35), mirror the third-asset sleeve
decision into paper_trades: open ETF:513100 on BUY_513100, close the sleeve
leg on SELL_TO_REPO / SELL_TO_A_SHARE. Idempotent by design.

Three-window validation of the underlying rule: scripts/sleeve_nav_sim.py
(OPT-119 — all windows positive delta).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.sleeve_paper_auto import apply_sleeve_to_paper

logger = logging.getLogger(__name__)

JOB_ID = "sleeve_paper_auto"
CRON_EXPRESSION = "20 18 * * 1-5"  # weekdays 18:20 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    day = datetime.now(tz=UTC).date().isoformat()
    try:
        out = apply_sleeve_to_paper(day=day)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sleeve_paper_auto: %s", exc)
        insert_record(
            job_id=JOB_ID,
            status="failed",
            message=str(exc),
            started_at=datetime.now(tz=UTC),
        )
        return
    insert_record(
        job_id=JOB_ID,
        status="success",
        message=f"{out.get('action')} · {out.get('reason')}",
        started_at=datetime.now(tz=UTC),
    )
    logger.info("sleeve_paper_auto %s: %s (%s)", day, out.get("action"), out.get("reason"))