from __future__ import annotations

from fastapi import APIRouter, Query

from data_sync_service.db import check_db
from data_sync_service.service.daily import get_daily_from_db, get_daily_sync_status, sync_daily_full
from data_sync_service.service.stock_basic import get_stock_basic_list, sync_stock_basic

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    ok, error = check_db()
    return {
        "status": "ok" if ok else "degraded",
        "db": ok,
        "db_error": error if not ok else None,
    }


@router.get("/sync/stock-basic")
def get_stock_basic_endpoint() -> list:
    """Return all stock_basic rows from our database (~5k rows)."""
    return get_stock_basic_list()


@router.post("/sync/stock-basic")
def sync_stock_basic_endpoint() -> dict:
    """Sync stock basic list from tushare into database. Idempotent upsert by ts_code."""
    return sync_stock_basic()


@router.get("/sync/daily")
def get_daily_endpoint(
    ts_code: str | None = Query(None, description="Filter by ts_code"),
    start_date: str | None = Query(None, description="Start date YYYY-MM-DD"),
    end_date: str | None = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000),
) -> list:
    """Return daily bars from our database. Optional filters; default limit 5000."""
    return get_daily_from_db(ts_code=ts_code, start_date=start_date, end_date=end_date, limit=limit)


@router.get("/sync/daily/status")
def get_daily_status_endpoint() -> dict:
    """Return today's full sync run record (success/fail, last_ts_code on failure)."""
    return get_daily_sync_status()


@router.post("/sync/daily")
def sync_daily_endpoint() -> dict:
    """Trigger full sync of daily bars (2024-01-01 to today). Skips if today already succeeded; resumes from failure."""
    return sync_daily_full()
