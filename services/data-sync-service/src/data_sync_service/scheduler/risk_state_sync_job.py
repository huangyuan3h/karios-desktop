"""TIP-017 risk-state sync — weekdays 18:50 Asia/Shanghai.

Daily catch-up for the risk-state sensor family (broad-ETF shares, market
margin total, north-bound flow, HSI/HSTECH global bars) + margin_detail /
moneyflow per-date catch-up (margin detail publishes T+1, so a ~3-week
catch-up window covers publication lag and any missed runs; all upserts are
idempotent). Runs after the post-close chain (17:35).
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded
from data_sync_service.service.cn_risk_state_sync import catch_up

logger = logging.getLogger(__name__)

JOB_ID = "risk_state_sync"
CRON_EXPRESSION = "50 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    result = run_guarded(JOB_ID, lambda: catch_up(days=15), log=logger)
    if result is None:
        return

    def _ok(r) -> None:
        parts = {
            k: (v.get("updated") if isinstance(v, dict) else None)
            for k, v in r.items()
            if isinstance(v, dict)
        }
        logger.info("risk_state_sync ok: %s", parts)

    def _fail(r) -> None:
        bad = {k: v.get("error") for k, v in r.items() if isinstance(v, dict) and not v.get("ok")}
        logger.warning("risk_state_sync failed: %s", bad or r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
