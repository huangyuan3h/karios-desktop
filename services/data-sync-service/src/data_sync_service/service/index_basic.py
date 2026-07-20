"""Sync index_dailybasic from Tushare for market breadth indicators."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.index_basic import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "index_basic_sync"
FULL_START_DATE = "20230101"
INDEX_CODES = ["000001.SH", "399006.SZ"]

INDEX_BASIC_FIELDS = [
    "ts_code",
    "trade_date",
    "total_mv",
    "float_mv",
    "total_share",
    "float_share",
    "free_share",
    "turnover_rate",
    "turnover_rate_f",
    "pe",
    "pe_ttm",
    "pb",
]


def _today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def sync_index_basic_full() -> dict[str, Any]:
    """Sync index_dailybasic for selected indices."""
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    pro = ts.pro_api(settings.tu_share_api_key)
    end_date = _today_yyyymmdd()
    total_rows = 0
    last_successful_ts_code: str | None = None

    for ts_code in INDEX_CODES:
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

            df: pd.DataFrame = pro.index_dailybasic(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields=",".join(INDEX_BASIC_FIELDS),
            )
            if df is not None and not df.empty:
                n = upsert_from_dataframe(df)
                total_rows += n

            last_successful_ts_code = ts_code
        except Exception as e:
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=last_successful_ts_code,
                error_message=str(e),
            )
            return {"ok": False, "error": str(e), "last_ts_code": last_successful_ts_code}

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total_rows}