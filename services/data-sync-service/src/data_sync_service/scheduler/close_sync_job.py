"""Close-time sync job (runs every day, skips if not trading day)."""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import (
    record_failure,
    record_success,
    run_guarded,
)
from data_sync_service.service.close_sync import sync_close
from data_sync_service.service.post_close_sync import run_post_close_sync

logger = logging.getLogger(__name__)

JOB_ID = "close_sync"
# Daily 17:10 Asia/Shanghai (after market close)
CRON_EXPRESSION = "10 17 * * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, lambda: sync_close(exchange="SSE", force=False), log=logger)
    if result is None:
        return  # exception path already recorded + logged
    if result.get("ok"):
        if result.get("skipped"):
            logger.info("close_sync skipped: %s", result.get("message", ""))
        else:
            logger.info(
                "close_sync ok: daily=%s adj_factor=%s dates=%s",
                result.get("updated_daily_rows", 0),
                result.get("updated_adj_factor_rows", 0),
                result.get("trade_dates", []),
            )
        post = run_post_close_sync()
        logger.info(
            "post_close_sync: index=%s macro=%s",
            post.get("indexDaily"),
            post.get("macroDaily"),
        )
        record_success(JOB_ID)
        # 2026-11-10 speed pass + Step 3 (2026-11-11): the evening agent read
        # follows the close data. Drop the pre-close brief cache, refresh the
        # trailing-year timeline caches (memory + file) with force=True, then
        # re-warm the brief so backtest.recent comes from the post-close files.
        try:
            from data_sync_service.api.backtest_routes import (
                warm_default_timelines_async,
            )
            from data_sync_service.service.agent_brief import (
                invalidate_judgment_brief_cache,
            )

            invalidate_judgment_brief_cache("close_sync")
            warm_default_timelines_async(force=True, reason="close_sync")
        except Exception:  # noqa: BLE001 — warmup must never fail the close job
            logger.warning("agent brief warmup after close_sync failed", exc_info=True)
    else:
        logger.warning("close_sync failed: %s", result.get("error", "unknown"))
        record_failure(JOB_ID, result.get("error", "unknown"))
