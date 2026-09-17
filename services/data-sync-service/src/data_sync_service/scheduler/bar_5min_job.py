"""Last-hour 5-minute bars — weekdays 18:40 Asia/Shanghai.

After close_sync (17:10) today's daily exists, so we can restrict the
pull to intraday gap names (open/pre_close > 3%) plus open CN paper holdings.
Historical year backfill is scripts/backfill_bar_5min.py (baostock).

2026-09-17 (OPT-219): after the bars are stored, refresh the 星舰 pool —
the 17:30 watchlist_automation built it before today's 14:30 prints existed,
so new satellite legs were mirrored a day late.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.db.sync_job_record import insert_record
from data_sync_service.service.bar_5min import (
    SOURCE_BAOSTOCK,
    backfill_symbols,
    list_gap_codes,
)

logger = logging.getLogger(__name__)

JOB_ID = "bar_5min_close"
CRON_EXPRESSION = "40 18 * * 1-5"
TIMEZONE = "Asia/Shanghai"
CN_TZ = ZoneInfo("Asia/Shanghai")


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _today_cn() -> str:
    return datetime.now(tz=CN_TZ).date().isoformat()


def _open_cn_paper_ts_codes() -> list[str]:
    from data_sync_service.db.paper_trading import list_paper_trades

    out: list[str] = []
    for row in list_paper_trades(status="open"):
        ts = str(row.get("ts_code") or "")
        if ts.endswith((".SH", ".SZ")):
            out.append(ts)
    return out


def run() -> None:
    today = _today_cn()
    try:
        codes = list(dict.fromkeys(list_gap_codes(today) + _open_cn_paper_ts_codes()))
        if not codes:
            insert_record(JOB_ID, success=True, last_ts_code="0", error_message="no-symbols")
            logger.info("[bar_5min_close] no gap/paper symbols for %s", today)
            return
        res = backfill_symbols(
            ts_codes=codes,
            start_date=today,
            end_date=today,
            source=SOURCE_BAOSTOCK,
            skip_covered=True,
        )
        insert_record(
            JOB_ID,
            success=res["failed"] == 0,
            last_ts_code=str(res["stored"]),
            error_message=None if res["failed"] == 0 else f"failed={res['failed']}",
        )
        logger.info(
            "[bar_5min_close] pending=%d ok=%d stored=%d failed=%d skipped=%d",
            res["pending"],
            res["ok"],
            res["stored"],
            res["failed"],
            res["skipped"],
        )
        _refresh_satellite_pool(today)
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=str(exc)[:500])
        logger.warning("[bar_5min_close] failed: %s", exc)


def _refresh_satellite_pool(day: str) -> None:
    """Re-apply today's 星舰 pool now that the 14:30 prints are stored.

    Isolated from the fetch result: a pool failure must not fail the bar job.
    Skips non-trading days (no pool row should be written for a holiday).
    """
    try:
        from datetime import date as _date

        from data_sync_service.db.trade_calendar import is_trading_day

        if is_trading_day("SSE", _date.fromisoformat(day)) is not True:
            logger.info("[bar_5min_close] satellite pool refresh skipped: %s not a session", day)
            return
        from data_sync_service.service.watchlist_automation import refresh_satellite_pool

        out = refresh_satellite_pool(day=day)
        if out.get("ok"):
            logger.info(
                "[bar_5min_close] satellite pool refreshed: legs=%s added=%s removed=%s",
                out.get("satellitePoolSize"),
                out.get("added"),
                out.get("removed"),
            )
        else:
            logger.warning("[bar_5min_close] satellite pool refresh skipped: %s", out.get("error"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("[bar_5min_close] satellite pool refresh failed: %s", exc)
