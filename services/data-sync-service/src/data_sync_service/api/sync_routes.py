from __future__ import annotations

from fastapi import APIRouter, Query  # type: ignore[import-not-found]

from data_sync_service.service.adj_factor import sync_adj_factor_full
from data_sync_service.service.close_sync import sync_close
from data_sync_service.service.daily import sync_daily_full
from data_sync_service.service.hk_basic import sync_hk_basic
from data_sync_service.service.hk_daily import sync_hk_daily_full
from data_sync_service.service.index_daily import sync_index_daily_full
from data_sync_service.service.stock_basic import sync_stock_basic
from data_sync_service.service.trade_calendar import sync_trade_calendar

router = APIRouter()


@router.post("/sync/stock-basic")
def sync_stock_basic_endpoint() -> dict:
    # Purpose: pull stock_basic from tushare and upsert into DB.
    """Sync stock basic list from tushare into database. Idempotent upsert by ts_code."""
    return sync_stock_basic()


@router.post("/sync/hk-basic")
def sync_hk_basic_endpoint(
    ts_code: str | None = Query(None, description="Optional ts_code filter, e.g. 00005.HK"),
    list_status: str = Query("L", description="Listing status: L listed, D delisted, P suspended"),
    force: bool = Query(False, description="Force sync even if already synced this month"),
) -> dict:
    # Purpose: pull hk_basic from tushare and upsert into stock_basic table.
    """Sync Hong Kong stock list (hk_basic) from tushare into stock_basic table."""
    return sync_hk_basic(ts_code=ts_code, list_status=list_status, force=bool(force))


@router.post("/market/sync")
def market_sync_endpoint() -> dict:
    # Purpose: compatibility endpoint for MarketPage; calls sync_stock_basic.
    """Sync market stocks (alias for /sync/stock-basic)."""
    from datetime import datetime, timezone

    result = sync_stock_basic()
    synced_at = datetime.now(timezone.utc).isoformat()

    # Return format compatible with quant-service response
    if result.get("ok"):
        updated_count = result.get("updated", 0)
        return {
            "ok": True,
            "stocks": updated_count,
            "syncedAt": synced_at,
        }
    return {
        "ok": False,
        "error": result.get("error", "Unknown error"),
    }


@router.post("/sync/daily")
def sync_daily_endpoint() -> dict:
    # Purpose: full daily sync from 2023-01-01 to today; skip if today already succeeded.
    """Trigger full sync of daily bars (2023-01-01 to today). Skips if today already succeeded; resumes from failure."""
    return sync_daily_full()


@router.post("/sync/hk-daily")
def sync_hk_daily_endpoint() -> dict:
    # Purpose: full HK daily sync into daily table; skip if today already succeeded.
    """Trigger full HK daily sync into daily table. Skips if today already succeeded; resumes from failure."""
    return sync_hk_daily_full()


@router.post("/sync/adj-factor")
def sync_adj_factor_endpoint() -> dict:
    # Purpose: sync adj_factor into daily table; updates by (ts_code, trade_date).
    """Trigger full sync of adj_factor into daily table. Skips if today already succeeded; resumes from failure."""
    return sync_adj_factor_full()


@router.post("/sync/index-daily")
def sync_index_daily_endpoint() -> dict:
    # Purpose: full index daily sync for selected indices; skip if today already succeeded.
    """Trigger full sync of index daily bars. Skips if today already succeeded; resumes from failure."""
    return sync_index_daily_full()


@router.post("/sync/trade-cal")
def sync_trade_cal_endpoint(
    exchange: str = Query("SSE"),
    start_date: str | None = Query(None, description="Start date YYYYMMDD"),
    end_date: str | None = Query(None, description="End date YYYYMMDD"),
) -> dict:
    # Purpose: manually sync trade calendar into DB for given exchange/date range.
    """Manually sync trade calendar into DB."""
    return sync_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)


@router.post("/sync/close")
def sync_close_endpoint(exchange: str = Query("SSE"), force: bool = Query(False)) -> dict:
    # Purpose: close-time sync by trade_date window; pulls daily + adj_factor (paged).
    """Close-time sync by trade_date window: daily + adj_factor (paged)."""
    result = sync_close(exchange=exchange, force=bool(force))
    index_result = sync_index_daily_full()
    if isinstance(result, dict):
        return {**result, "indexDaily": index_result}
    return {"ok": True, "result": result, "indexDaily": index_result}
