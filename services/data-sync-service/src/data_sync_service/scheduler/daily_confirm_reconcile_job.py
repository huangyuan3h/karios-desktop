"""Weekday 18:55 14:30-confirmation reconcile (record-keeping only).

After the A-share close chain has the day's bars, compare each pending 14:30
confirmation with the next session's actual fills / book changes:

- advice comes from the confirmation row (loaded at 14:30),
- actual evidence is the watchlist registry + ``user_trades`` (read-only),
- the result is written back to ``outcome.reconcile`` on the same row.

Never places orders, never edits settings, never changes the watchlist. A
missing input leaves ``pending`` / ``degraded`` and keeps the row. Fails open:
an unexpected exception records a job failure, not a data wipe.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

JOB_ID = "daily_confirm_reconcile"
# 18:55 sits after behavior_audit (18:45) / risk_state_sync (18:50) and before
# factor_live_append (19:00), which is intentionally left untouched.
CRON_EXPRESSION = "55 18 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    from data_sync_service.scheduler._job_guard import record_failure, record_success
    from data_sync_service.service import agent_daily_confirm

    try:
        result = agent_daily_confirm.reconcile_pending()
    except Exception as exc:  # noqa: BLE001 — record and keep the confirm rows
        record_failure(JOB_ID, exc)
        logger.warning("daily_confirm_reconcile error: %s", exc)
        return

    counts = result.get("counts") or {}
    logger.info(
        "daily_confirm_reconcile checked=%s updated=%s done=%s pending=%s degraded=%s",
        result.get("checked"),
        result.get("updated"),
        counts.get("done", 0),
        counts.get("pending", 0),
        counts.get("degraded", 0),
    )
    record_success(JOB_ID)
