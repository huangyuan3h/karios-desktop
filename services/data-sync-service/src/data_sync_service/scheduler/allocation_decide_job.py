"""Weekly cross-market allocation decision (T4, 2026-08-11).

Monday 17:45 Asia/Shanghai — after close_sync (17:10) and watchlist
automation (17:30), before paper_trading_update (17:45). Persists the week's
R5c weights (CN tradable -> 100% CN, only HK tradable -> 100% HK, both weak
-> 0/0) into allocation_weights; the paper S-3 intake scales sleeves by the
same record (first decision of the week wins; intake self-heals with a
same-day fallback decision when the job missed).

The backtest replays the exact same decision function
(service/allocation.weights_from_regimes) so the real book and the
backtest share one allocation rule.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.allocation import decide_week, week_start_for

logger = logging.getLogger(__name__)

JOB_ID = "allocation_decide"
CRON_EXPRESSION = "45 17 * * 1"  # Monday 17:45 Asia/Shanghai
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_iso() -> str:
    return datetime.now(tz=ZoneInfo(TIMEZONE)).date().isoformat()


def run() -> None:
    today = _today_iso()
    wk = week_start_for(today)
    try:
        out = decide_week(week_start=wk, as_of_date=today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc))
        logger.warning("allocation_decide failed: %s", exc)
        return
    insert_record(JOB_ID, success=True, last_ts_code=f"{out['weekStart']}")
    logger.info("allocation_decide ok: week=%s weights=%s", out["weekStart"], out["decision"])
