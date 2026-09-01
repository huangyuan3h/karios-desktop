"""ETF K-line sync: tushare `fund_daily` → `daily` table.

Reuses the generic daily schema and upsert path. We iterate over all ETF
ts_codes (where market='ETF' in stock_basic), calling `pro.fund_daily` per
ts_code with resume-from-failure semantics similar to hk_daily_full.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.stock_basic import fetch_ts_codes_by_market
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "etf_daily_full"
FULL_START_DATE = "20230101"
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


def _today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _sync_end_date(ts_code: str) -> str:
    """Latest trading day <= today (weekend/holiday → previous trading day)
    so weekend crons skip instead of making empty tushare calls."""
    from data_sync_service.db.trade_calendar import last_trading_day_str

    exchange = "SZSE" if str(ts_code).startswith(("0", "1", "3")) else "SSE"
    return last_trading_day_str(exchange, datetime.now(UTC).date())


def _fetch_etf_ts_codes() -> list[str]:
    """Return ordered ETF ts_codes (those whose stock_basic.market='ETF')."""
    return fetch_ts_codes_by_market("ETF")


def sync_etf_daily_full() -> dict[str, Any]:
    """
    Full sync for ETF K-lines:
    - If today's run already succeeded: skip.
    - If today's run failed: resume from the ts_code after last_ts_code.
    - If we already have today's data for a stock, skip that stock.
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    ts_codes = _fetch_etf_ts_codes()
    if not ts_codes:
        return {"ok": True, "updated": 0, "message": "no ETF stock list"}

    start_index = 0
    if run and run.get("success") is False and run.get("last_ts_code"):
        try:
            idx = ts_codes.index(run["last_ts_code"])
            start_index = idx + 1
        except ValueError:
            pass

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    pro = ts.pro_api(settings.tu_share_api_key)
    end_date = _sync_end_date(ts_codes[0] if ts_codes else "")
    total_rows = 0
    last_successful_ts_code: str | None = None

    for i in range(start_index, len(ts_codes)):
        ts_code = ts_codes[i]
        try:
            last_date = get_last_trade_date(ts_code)
            if last_date is None:
                start_date = FULL_START_DATE
            else:
                next_date = last_date + timedelta(days=1)
                start_date = _date_to_yyyymmdd(next_date)

            if start_date > end_date:
                last_successful_ts_code = ts_code
                continue

            df: pd.DataFrame = pro.fund_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields=",".join(DAILY_FIELDS),
            )
            if df is not None and not df.empty:
                n = upsert_from_dataframe(df)
                total_rows += n

            last_successful_ts_code = ts_code
        except Exception as exc:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=last_successful_ts_code,
                error_message=str(exc),
            )
            return {"ok": False, "error": str(exc), "last_ts_code": last_successful_ts_code}

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total_rows}


SLEEVE_ETF_TS_CODES = ["518880.SH", "513350.SH", "513110.SH", "513100.SH", "511260.SH"]


def sync_sleeve_etfs() -> dict[str, Any]:
    """Incremental daily sync for the 5 Twin-Star core-leg ETFs only.

    The full-market ``etf_daily_full`` cron runs monthly and keeps failing on
    the tushare per-minute rate limit (200 calls/min for ~1000 ETFs), which
    left GOLD/BOND10 stale since 2026-08-21 and distorted mom_compare picks.
    Five per-ts_code calls with a small sleep stay far below the limit.
    """

    JOB_TYPE = "sleeve_etf_daily_sync"
    settings = get_settings()
    if not settings.tu_share_api_key:
        insert_record(job_type=JOB_TYPE, success=False, error_message="TU_SHARE_API_KEY is not set")
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    pro = ts.pro_api(settings.tu_share_api_key)
    end_date = _sync_end_date(SLEEVE_ETF_TS_CODES[0])
    total = 0
    for ts_code in SLEEVE_ETF_TS_CODES:
        try:
            last_date = get_last_trade_date(ts_code)
            if last_date is None:
                start_date = FULL_START_DATE
            else:
                start_date = _date_to_yyyymmdd(last_date + timedelta(days=1))
            if start_date > end_date:
                continue
            df = pro.fund_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields=",".join(DAILY_FIELDS),
            )
            if df is not None and not df.empty:
                total += upsert_from_dataframe(df)
        except Exception as exc:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=ts_code,
                error_message=str(exc),
            )
            return {"ok": False, "error": str(exc), "ts_code": ts_code, "updated": total}
        time.sleep(0.35)
    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total}


def get_etf_daily_sync_status() -> dict[str, Any]:
    """Return today's run record for etf_daily_full if any."""
    run = get_today_run(JOB_TYPE)
    if run is None:
        return {"job_type": JOB_TYPE, "today_run": None}
    return {"job_type": JOB_TYPE, "today_run": run}


def sync_etf_daily_for_ts_code(ts_code: str) -> dict[str, Any]:
    """Incremental tushare fund_daily sync for one ETF ts_code (used by bars?force=true hot path)."""
    code = (ts_code or "").strip().upper()
    if not code:
        return {"ok": False, "error": "ts_code is required"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    last_date = get_last_trade_date(code)
    if last_date is None:
        start_date = FULL_START_DATE
    else:
        start_date = _date_to_yyyymmdd(last_date + timedelta(days=1))
    end_date = _sync_end_date(code)
    if start_date > end_date:
        return {"ok": True, "updated": 0, "skipped": True, "ts_code": code}

    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        df: pd.DataFrame = pro.fund_daily(
            ts_code=code,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(DAILY_FIELDS),
        )
        updated = 0
        if df is not None and not df.empty:
            updated = upsert_from_dataframe(df)
        if updated > 0:
            from data_sync_service.service.trendok import clear_trendok_cache

            clear_trendok_cache()
        return {"ok": True, "updated": updated, "ts_code": code}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e), "ts_code": code}