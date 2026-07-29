"""Sync ETF list (fund_basic market='E') from tushare into stock_basic table.

ETF rows are upserted with `market='ETF'` so the existing stock_basic schema
serves as the universal symbol universe (CN/HK/ETF).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.stock_basic import upsert_from_dataframe
from data_sync_service.db.sync_job_record import get_last_success, insert_record

JOB_TYPE = "etf_fund_basic_sync"

# fund_type values we keep (matches Tushare fund_basic). Anything not in this set
# still gets synced, but the industry column is normalized to one of these buckets.
_FUND_TYPE_KEEP = {"股票型", "债券型", "混合型", "REITs", "货币型", "其他"}


def _parse_iso_datetime(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except Exception:  # noqa: BLE001
        return None


def _is_same_utc_month(a: datetime, b: datetime) -> bool:
    a2 = a.astimezone(UTC)
    b2 = b.astimezone(UTC)
    return (a2.year, a2.month) == (b2.year, b2.month)


def map_etf_basic_to_stock_basic_df(etf_df: pd.DataFrame) -> pd.DataFrame:
    """Map tushare fund_basic (market='E') DataFrame to our stock_basic schema.

    Output columns: ts_code, symbol, name, industry, market, list_date, delist_date.
    - market = 'ETF' for all rows
    - industry = fund_type (股票型/债券型/混合型/REITs/货币型/其他) — coarse bucket
    - list_date / delist_date normalized to YYYY-MM-DD; missing values are None.
    """
    if etf_df is None or etf_df.empty:
        return pd.DataFrame(
            columns=["ts_code", "symbol", "name", "industry", "market", "list_date", "delist_date"]
        )

    def _symbol_from_ts_code(ts_code: object) -> str | None:
        if ts_code is None or pd.isna(ts_code):
            return None
        s = str(ts_code).strip()
        if not s:
            return None
        return s.split(".", 1)[0].strip() or None

    def _date_str(v: object) -> str | None:
        if v is None or pd.isna(v):
            return None
        if hasattr(v, "strftime"):
            return v.strftime("%Y-%m-%d")
        s = str(v).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        return s or None

    def _industry(v: object) -> str | None:
        if v is None or pd.isna(v):
            return None
        s = str(v).strip()
        return s or None

    ts_codes = etf_df["ts_code"] if "ts_code" in etf_df.columns else pd.Series([None] * len(etf_df))
    names = etf_df["name"] if "name" in etf_df.columns else pd.Series([None] * len(etf_df))
    fund_types = (
        etf_df["fund_type"] if "fund_type" in etf_df.columns else pd.Series([None] * len(etf_df))
    )
    list_dates = (
        etf_df["list_date"] if "list_date" in etf_df.columns else pd.Series([None] * len(etf_df))
    )
    delist_dates = (
        etf_df["delist_date"]
        if "delist_date" in etf_df.columns
        else pd.Series([None] * len(etf_df))
    )

    out = pd.DataFrame()
    out["ts_code"] = ts_codes
    out["symbol"] = ts_codes.apply(_symbol_from_ts_code)
    out["name"] = names
    out["industry"] = fund_types.apply(_industry)
    out["market"] = "ETF"
    out["list_date"] = list_dates.apply(_date_str)
    out["delist_date"] = delist_dates.apply(_date_str)
    return out


def sync_etf_fund_basic(
    *,
    list_status: str = "L",
    force: bool = False,
) -> dict[str, Any]:
    """
    Fetch fund_basic (market='E') from tushare and upsert into stock_basic.

    - list_status defaults to "L" (listed); pass "D" / "P" for delisted / paused.
    - market column is set to "ETF" for every row.
    - Skip if already synced this UTC month unless force=True.
    """
    list_status2 = (list_status or "L").strip().upper() or "L"
    if list_status2 not in {"L", "D", "P"}:
        return {"ok": False, "error": "list_status must be one of: L, D, P"}

    if not force:
        last_ok = get_last_success(JOB_TYPE)
        last_at = _parse_iso_datetime((last_ok or {}).get("sync_at"))
        if last_at and _is_same_utc_month(last_at, datetime.now(UTC)):
            return {"ok": True, "skipped": True, "message": "already synced this month"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        msg = "TU_SHARE_API_KEY is not set"
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=msg)
        return {"ok": False, "error": msg}

    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        etf_df: pd.DataFrame = pro.fund_basic(
            market="E",
            status=list_status2,
            fields="ts_code,name,fund_type,list_date,delist_date",
        )
        mapped = map_etf_basic_to_stock_basic_df(etf_df)
        if mapped.empty:
            insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
            return {"ok": True, "updated": 0, "message": "no data from tushare"}

        n = upsert_from_dataframe(mapped)
        insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
        return {"ok": True, "updated": n, "list_status": list_status2}
    except Exception as exc:  # noqa: BLE001
        insert_record(job_type=JOB_TYPE, success=False, last_ts_code=None, error_message=str(exc))
        return {"ok": False, "error": str(exc)}


def get_etf_fund_basic_sync_status() -> dict[str, Any]:
    """Return today's last sync status for ETF fund_basic."""
    last = get_last_success(JOB_TYPE)
    if last is None:
        return {"job_type": JOB_TYPE, "last_success": None}
    return {"job_type": JOB_TYPE, "last_success": last}