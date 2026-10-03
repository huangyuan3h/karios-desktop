"""Monthly ETF research-panel snapshot (weekdays-safe monthly cron).

Refreshes data/etf/etf_daily.csv for the trailing 45 calendar days
(covers the monthly gap plus any missed month) via
service.etf_snapshot.refresh_window — same UNIVERSE/math as the manual
scripts/sync_etf_daily.py, throttled under the tushare rate limit.

Runs monthly on the 2nd at 19:30 Asia/Shanghai (a day after the fragile
etf_daily_full day-1 run, so the two never compete for quota).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler._job_guard import record_dict_result, run_guarded

logger = logging.getLogger(__name__)

JOB_ID = "etf_snapshot_sync"
CRON_EXPRESSION = "30 19 2 * *"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _window() -> tuple[str, str]:
    end = datetime.now(UTC).date()
    start = end - timedelta(days=45)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def run() -> None:
    from data_sync_service.service.etf_snapshot import refresh_window

    start, end = _window()

    def _body() -> dict:
        return refresh_window(start, end)

    result = run_guarded(JOB_ID, _body, log=logger)
    if result is None:
        return

    def _ok(r) -> None:
        logger.info(
            "etf_snapshot_sync ok: updated=%s csv_max=%s", r.get("updated", 0), r.get("csv_max", "")
        )

    def _fail(r) -> None:
        logger.warning("etf_snapshot_sync failed: %s", r.get("error", "unknown"))

    record_dict_result(JOB_ID, result, ok_log=_ok, fail_log=_fail)
