from __future__ import annotations

from fastapi import APIRouter

from data_sync_service.db import check_db
from data_sync_service.service.stock_basic import sync_stock_basic

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    ok, error = check_db()
    return {
        "status": "ok" if ok else "degraded",
        "db": ok,
        "db_error": error if not ok else None,
    }


@router.post("/sync/stock-basic")
def sync_stock_basic_endpoint() -> dict:
    """Sync stock basic list from tushare into database. Idempotent upsert by ts_code."""
    return sync_stock_basic()
