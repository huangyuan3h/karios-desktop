"""Sync Hong Kong stock list (hk_basic) from tushare into stock_basic table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd  # type: ignore[import-not-found]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.stock_basic import upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_last_success, insert_record

JOB_TYPE = "hk_basic_sync"


def _parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # sync_job_record normalizes sync_at to isoformat string.
        return datetime.fromisoformat(str(value))
    except Exception:  # noqa: BLE001
        return None


def _is_same_utc_month(a: datetime, b: datetime) -> bool:
    a2 = a.astimezone(timezone.utc)
    b2 = b.astimezone(timezone.utc)
    return (a2.year, a2.month) == (b2.year, b2.month)


def map_hk_basic_to_stock_basic_df(hk_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map tushare hk_basic DataFrame to our stock_basic schema.

    Target schema columns:
    - ts_code, symbol, name, industry, market, list_date, delist_date
    """
    if hk_df is None or hk_df.empty:
        return pd.DataFrame(columns=["ts_code", "symbol", "name", "industry", "market", "list_date", "delist_date"])

    def _symbol_from_ts_code(ts_code: object) -> str | None:
        if ts_code is None or pd.isna(ts_code):
            return None
        s = str(ts_code).strip()
        if not s:
            return None
        return s.split(".", 1)[0].strip() or None

    ts_codes = hk_df["ts_code"] if "ts_code" in hk_df.columns else pd.Series([None] * len(hk_df))
    names = hk_df["name"] if "name" in hk_df.columns else pd.Series([None] * len(hk_df))
    list_dates = hk_df["list_date"] if "list_date" in hk_df.columns else pd.Series([None] * len(hk_df))
    delist_dates = hk_df["delist_date"] if "delist_date" in hk_df.columns else pd.Series([None] * len(hk_df))

    out = pd.DataFrame()
    out["ts_code"] = ts_codes
    out["symbol"] = ts_codes.apply(_symbol_from_ts_code)
    out["name"] = names
    out["industry"] = None
    # Important: our query layer expects market == "HK" for Hong Kong.
    out["market"] = "HK"
    out["list_date"] = list_dates
    out["delist_date"] = delist_dates
    return out


def sync_hk_basic(
    ts_code: str | None = None,
    list_status: str = "L",
    force: bool = False,
) -> dict[str, Any]:
    """
    Fetch hk_basic from tushare and upsert into stock_basic table.

    - Default list_status is "L" (listed / trading).
    - Idempotent upsert by ts_code.
    - Skip if already synced in the current UTC month unless force=True.
    """
    list_status2 = (list_status or "L").strip().upper() or "L"
    if list_status2 not in {"L", "D", "P"}:
        return {"ok": False, "error": "list_status must be one of: L, D, P"}

    if not force:
        last_ok = get_last_success(JOB_TYPE)
        last_at = _parse_iso_datetime((last_ok or {}).get("sync_at"))
        if last_at and _is_same_utc_month(last_at, datetime.now(timezone.utc)):
            return {"ok": True, "skipped": True, "message": "already synced this month"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        msg = "TU_SHARE_API_KEY is not set"
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=msg)
        return {"ok": False, "error": msg}

    ts_code2 = (ts_code or "").strip().upper() or None
    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        hk_df: pd.DataFrame = pro.hk_basic(
            ts_code=ts_code2,
            list_status=list_status2,
            fields="ts_code,name,list_date,delist_date",
        )
        mapped = map_hk_basic_to_stock_basic_df(hk_df)
        if mapped.empty:
            insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
            return {"ok": True, "updated": 0, "message": "no data from tushare"}

        n = upsert_from_dataframe(mapped)
        insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
        return {"ok": True, "updated": n, "list_status": list_status2}
    except Exception as exc:  # noqa: BLE001
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=str(exc))
        return {"ok": False, "error": str(exc)}

