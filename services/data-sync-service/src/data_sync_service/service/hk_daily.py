"""Daily HK K-line sync: full sync with resume and skip-if-today-ok."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.stock_basic import fetch_ts_codes_by_market
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "hk_daily_full"
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
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def sync_hk_daily_full() -> dict[str, Any]:
    """
    Full sync for HK stocks:
    - If today's run already succeeded: skip.
    - If today's run failed: resume from the ts_code after last_ts_code.
    - If we already have today's data for a stock, skip that stock.
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    ts_codes = fetch_ts_codes_by_market("HK")
    if not ts_codes:
        return {"ok": True, "updated": 0, "message": "no HK stock list"}

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
    end_date = _today_yyyymmdd()
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

            df: pd.DataFrame = pro.hk_daily(
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


def get_hk_daily_sync_status() -> dict[str, Any]:
    """Return today's run record for hk_daily_full if any."""
    run = get_today_run(JOB_TYPE)
    if run is None:
        return {"job_type": JOB_TYPE, "today_run": None}
    return {"job_type": JOB_TYPE, "today_run": run}
