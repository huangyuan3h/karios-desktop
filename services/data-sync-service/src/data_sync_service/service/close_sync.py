"""Close-time sync: use trade calendar to sync by trade_date (market-wide), not per-stock loops."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.daily import upsert_from_dataframe as upsert_daily
from data_sync_service.db.daily import update_adj_factor_from_dataframe
from data_sync_service.db.sync_job_record import get_last_success, get_today_run, insert_record
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day

JOB_TYPE = "stock_close_sync"

DAILY_FIELDS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]


def _cn_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()


def _parse_yyyymmdd(s: str) -> date:
    return date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _fetch_paged_daily(pro, trade_date: str, limit: int = 5000) -> int:
    offset = 0
    total = 0
    while True:
        df: pd.DataFrame = pro.daily(
            trade_date=trade_date,
            limit=limit,
            offset=offset,
            fields=",".join(DAILY_FIELDS),
        )
        if df is None or df.empty:
            break
        total += upsert_daily(df)
        if len(df) < limit:
            break
        offset += limit
    return total


def _fetch_paged_adj_factor(pro, trade_date: str, limit: int = 5000) -> int:
    offset = 0
    total = 0
    while True:
        df: pd.DataFrame = pro.adj_factor(
            trade_date=trade_date,
            limit=limit,
            offset=offset,
        )
        if df is None or df.empty:
            break
        total += update_adj_factor_from_dataframe(df)
        if len(df) < limit:
            break
        offset += limit
    return total


def sync_close(exchange: str = "SSE") -> dict:
    """
    Close-time sync:
    - Requires trade calendar to be present.
    - If today is not a trading day: skip.
    - If today's run already succeeded: skip.
    - If today's run failed: resume from the next trading date after last_ts_code (stored as YYYYMMDD marker).
    - Otherwise: sync from the next trading day after last successful sync time (usually 1 day).
    - For each trading day: pull market-wide daily bars (paged) and adj_factor (paged).
    """
    today_run = get_today_run(JOB_TYPE)
    if today_run and today_run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    today = _cn_today()
    open_flag = is_trading_day(exchange, today)
    if open_flag is None:
        return {"ok": False, "error": "trade calendar missing for today; sync trade_cal first"}
    if open_flag is False:
        return {"ok": True, "skipped": True, "message": "not a trading day"}

    # Determine start date by resume marker or last success time.
    start_date = today
    if today_run and today_run.get("success") is False and today_run.get("last_ts_code"):
        marker = str(today_run["last_ts_code"])
        start_date = _parse_yyyymmdd(marker) + timedelta(days=1)
    else:
        last_ok = get_last_success(JOB_TYPE)
        if last_ok and last_ok.get("sync_at"):
            # sync_at is ISO; use its date in Asia/Shanghai as a conservative baseline
            sync_at = datetime.fromisoformat(str(last_ok["sync_at"]))
            start_date = sync_at.astimezone(ZoneInfo("Asia/Shanghai")).date() + timedelta(days=1)

    if start_date > today:
        return {"ok": True, "skipped": True, "message": "already up to date"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}
    pro = ts.pro_api(settings.tu_share_api_key)

    trade_dates = get_open_dates(exchange=exchange, start_date=start_date, end_date=today)
    if not trade_dates:
        return {"ok": True, "updated": 0, "message": "no trading dates in range"}

    total_daily = 0
    total_factor = 0
    last_completed: str | None = None

    for d in trade_dates:
        td = _to_yyyymmdd(d)
        try:
            total_daily += _fetch_paged_daily(pro, td)
            total_factor += _fetch_paged_adj_factor(pro, td)
            last_completed = td
        except Exception as e:  # noqa: BLE001
            insert_record(JOB_TYPE, success=False, last_ts_code=last_completed, error_message=str(e))
            return {"ok": False, "error": str(e), "last_marker": last_completed}

    insert_record(JOB_TYPE, success=True, last_ts_code=last_completed, error_message=None)
    return {
        "ok": True,
        "updated_daily_rows": total_daily,
        "updated_adj_factor_rows": total_factor,
        "trade_dates": [d.isoformat() for d in trade_dates],
    }


def get_close_sync_status() -> dict:
    return {
        "job_type": JOB_TYPE,
        "today_run": get_today_run(JOB_TYPE),
        "last_success": get_last_success(JOB_TYPE),
    }

