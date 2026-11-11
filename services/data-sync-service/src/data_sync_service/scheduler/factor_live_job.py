"""Factor-vault live overlay refresh (read-only replay, no DB writes).

Weekdays 19:00 Asia/Shanghai — after the EOD chain (close_sync 17:10,
sleeve_etf_daily 17:25, bar_5min 18:40) and before ``factor_vault_refresh``
19:10 / ``sgap_decay_refresh`` 19:15:

1. replay the frozen S-gap habit recipe for sessions after the frozen
   h2f_trades prefix (only closed trades on a complete daily bar),
2. replay the frozen S-3 stock leg + the Harbor parking path after the frozen
   H2d blotter exit,
3. append the new closed trades to ``factor_live_trades.json`` (the frozen
   audit prefix stays frozen and read-only).

Pure display data: never touches live strategy params, TRAIL_PCT, the parking
cooldown, settings, or any broker/paper order. Fail-open: an input failure
keeps yesterday's appended rows and the job records a failure (which pages via
HIGH_JOB_TYPES); the live file is never deleted or truncated.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

JOB_ID = "factor_live_append"
CRON_EXPRESSION = "0 19 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _reports_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports"


def run() -> None:
    from data_sync_service.scheduler._job_guard import record_failure, record_success
    from data_sync_service.service import factor_live

    reports_dir = _reports_dir()
    now = datetime.now(tz=ZoneInfo(TIMEZONE))
    try:
        payload = factor_live.refresh_live_blotter(reports_dir, now=now)
    except Exception as exc:  # noqa: BLE001 - display-only daily file
        record_failure(JOB_ID, exc)
        logger.warning("factor_live_append error: %s", exc)
        return

    meta = payload.get("meta") or {}
    inputs = meta.get("inputs") or {}
    if meta.get("status") != "ok":
        reason = str(meta.get("reason") or "live overlay inputs incomplete")
        record_failure(JOB_ID, reason)
        logger.warning("factor_live_append degraded: %s", reason)
        return

    logger.info(
        "factor_live_append ok: as_of=%s sgap+%s harbor+%s parking=%s",
        meta.get("asOf"),
        (inputs.get("sgap") or {}).get("newTrades"),
        (inputs.get("harbor") or {}).get("newTrades"),
        (inputs.get("harbor") or {}).get("parkingTrades"),
    )
    record_success(JOB_ID)
