"""Sync stock basic list from tushare to database."""

from __future__ import annotations

from typing import Any

import pandas as pd  # type: ignore[import-not-found]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.stock_basic import fetch_all, upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_today_run, insert_record

JOB_TYPE = "stock_basic_sync"

FIELDS = [
    "ts_code",
    "symbol",
    "name",
    "industry",
    "market",
    "list_date",
    "delist_date",
]


def get_stock_basic_list() -> list[dict]:
    """Return all stock_basic rows from our database (ordered by ts_code)."""
    return fetch_all()


def sync_stock_basic() -> dict[str, Any]:
    """
    Fetch stock_basic from tushare and upsert into database.
    Returns {"ok": True, "updated": n} or {"ok": False, "error": "..."}.
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        msg = "TU_SHARE_API_KEY is not set"
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=msg)
        return {"ok": False, "error": msg}

    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        df: pd.DataFrame = pro.stock_basic(fields=",".join(FIELDS))
        if df is None or df.empty:
            insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
            return {"ok": True, "updated": 0, "message": "no data from tushare"}

        n = upsert_from_dataframe(df)
        insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
        return {"ok": True, "updated": n}
    except Exception as e:  # noqa: BLE001
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=str(e))
        return {"ok": False, "error": str(e)}


def get_stock_basic_sync_status() -> dict[str, Any]:
    """Return today's run record for stock_basic_sync if any."""
    run = get_today_run(JOB_TYPE)
    if run is None:
        return {"job_type": JOB_TYPE, "today_run": None}
    return {"job_type": JOB_TYPE, "today_run": run}
