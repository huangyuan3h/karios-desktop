"""After-close strategy alert snapshot (weekdays 19:20 Asia/Shanghai).

Runs after ``factor_live_append`` (19:00), ``factor_vault_refresh`` (19:10)
and ``sgap_decay_refresh`` (19:15), so it sees today's lights and S-gap
``revival_pos``. Writes one gitignored snapshot per ``asOf`` under
``data/backtest_reports/strategy_alerts/`` for 乐乐 to poll in chat:

- new decay / revival events (dated today only),
- excess classification changes vs the 母港 M30 baseline (same unit only).

Idempotent per day (re-run overwrites the same file). Never places orders,
never edits settings / the watchlist, never changes a target weight. Fails
open: a missing vault keeps yesterday's snapshot and records a job failure,
but the judgment brief / GET path never crash.
"""

from __future__ import annotations

import logging

from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

JOB_ID = "strategy_alerts_refresh"
# 19:20 sits after the S-gap refresh (19:15) and does not retune any upstream
# job. Keep this order: the alert pack must read today's vault, not yesterday's.
CRON_EXPRESSION = "20 19 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    from data_sync_service.scheduler._job_guard import record_failure, record_success
    from data_sync_service.service import agent_strategy_alerts

    try:
        result = agent_strategy_alerts.run_strategy_alerts()
    except Exception as exc:  # noqa: BLE001 — advice pack must never crash the cron
        record_failure(JOB_ID, exc)
        logger.warning("strategy_alerts_refresh error: %s", exc)
        return

    if not result.get("persisted"):
        record_failure(
            JOB_ID, result.get("writeError") or "strategy alerts snapshot not persisted"
        )
        return
    if not result.get("available"):
        record_failure(JOB_ID, result.get("error") or "factor vault unavailable")
        return

    logger.info(
        "strategy_alerts_refresh ok: asOf=%s events=%s quiet=%s stale=%s",
        result.get("asOf"),
        len(result.get("events") or []),
        result.get("quiet"),
        (result.get("freshness") or {}).get("stale"),
    )
    record_success(JOB_ID)
    # The evening brief may already be warm; make the next 乐乐 read include
    # the new alert pack without waiting for the 75s TTL.
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("strategy_alerts")
    except Exception:  # noqa: BLE001 — cache eviction is best-effort
        pass
