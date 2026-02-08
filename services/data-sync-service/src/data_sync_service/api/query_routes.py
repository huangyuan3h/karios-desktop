from __future__ import annotations

from fastapi import APIRouter, Query

from data_sync_service.db import check_db
from data_sync_service.service.adj_factor import get_adj_factor_sync_status
from data_sync_service.service.close_sync import get_close_sync_status
from data_sync_service.service.daily import get_daily_from_db, get_daily_sync_status
from data_sync_service.service.stock_basic import get_stock_basic_list

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    # Purpose: health check; returns DB connectivity status.
    ok, error = check_db()
    return {
        "status": "ok" if ok else "degraded",
        "db": ok,
        "db_error": error if not ok else None,
    }


@router.get("/stock-basic")
def get_stock_basic_endpoint() -> list:
    # Purpose: return full stock basic list from DB (about 5k rows).
    """Return all stock_basic rows from our database (~5k rows)."""
    return get_stock_basic_list()


@router.get("/daily")
def get_daily_endpoint(
    ts_code: str | None = Query(None, description="Filter by ts_code"),
    start_date: str | None = Query(None, description="Start date YYYY-MM-DD"),
    end_date: str | None = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000),
) -> list:
    # Purpose: query daily bars from DB; filters by ts_code/date range; limit caps result size.
    """Return daily bars from our database. Optional filters; default limit 5000."""
    return get_daily_from_db(ts_code=ts_code, start_date=start_date, end_date=end_date, limit=limit)


@router.get("/daily/status")
def get_daily_status_endpoint() -> dict:
    # Purpose: return today's daily sync status from sync_job_record.
    """Return today's full sync run record (success/fail, last_ts_code on failure)."""
    return get_daily_sync_status()


@router.get("/adj-factor/status")
def get_adj_factor_status_endpoint() -> dict:
    # Purpose: return today's adj_factor sync status from sync_job_record.
    """Return today's adj_factor sync run record (success/fail, last_ts_code on failure)."""
    return get_adj_factor_sync_status()


@router.get("/close/status")
def get_close_status_endpoint() -> dict:
    # Purpose: return close-sync status (today run + last success).
    """Return close-sync status (today run + last success)."""
    return get_close_sync_status()
