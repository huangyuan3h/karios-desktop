"""Adjust factor (adj_factor) sync into daily table."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.daily import get_last_adj_factor_date, update_adj_factor_from_dataframe
from data_sync_service.db.stock_basic import fetch_ts_codes
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "stock_adj_factor_full"
FULL_START_DATE = "20230101"


def _today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def sync_adj_factor_full() -> dict[str, Any]:
    """
    Sync adj_factor into daily.adj_factor.

    - If today's run already succeeded: skip.
    - If today's run failed: resume from the ts_code after last_ts_code.
    - For each stock, fetch adj_factor from last known factor date + 1; otherwise from 2023-01-01.
    - Writes are idempotent: UPDATE by (ts_code, trade_date).
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    ts_codes = fetch_ts_codes()
    if not ts_codes:
        return {"ok": True, "updated": 0, "message": "no stock list"}

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
            last_factor_date = get_last_adj_factor_date(ts_code)
            if last_factor_date is None:
                start_date = FULL_START_DATE
            else:
                next_date = last_factor_date + timedelta(days=1)
                start_date = _date_to_yyyymmdd(next_date)

            if start_date > end_date:
                last_successful_ts_code = ts_code
                continue

            df: pd.DataFrame = pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if df is not None and not df.empty:
                n = update_adj_factor_from_dataframe(df)
                total_rows += n
            last_successful_ts_code = ts_code
        except Exception as e:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=last_successful_ts_code,
                error_message=str(e),
            )
            return {"ok": False, "error": str(e), "last_ts_code": last_successful_ts_code}

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total_rows}


def get_adj_factor_sync_status() -> dict[str, Any]:
    """Return today's run record for stock_adj_factor_full if any."""
    run = get_today_run(JOB_TYPE)
    if run is None:
        return {"job_type": JOB_TYPE, "today_run": None}
    return {"job_type": JOB_TYPE, "today_run": run}

