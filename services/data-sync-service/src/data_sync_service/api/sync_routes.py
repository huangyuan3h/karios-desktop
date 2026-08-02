from __future__ import annotations

from datetime import UTC
from typing import Any

from fastapi import APIRouter, Query  # type: ignore[import-not-found]

from data_sync_service.service.adj_factor import sync_adj_factor_full
from data_sync_service.service.close_sync import sync_close
from data_sync_service.service.etf_daily import get_etf_daily_sync_status, sync_etf_daily_full
from data_sync_service.service.etf_fund_flow import sync_etf_fund_flow_watchlist
from data_sync_service.service.fund_basic import (
    get_etf_fund_basic_sync_status,
    sync_etf_fund_basic,
)
from data_sync_service.service.hk_basic import sync_hk_basic
from data_sync_service.service.hk_daily import get_hk_daily_sync_status, sync_hk_daily_full
from data_sync_service.service.index_basic import sync_index_basic_full
from data_sync_service.service.index_daily import sync_index_daily_full
from data_sync_service.service.macro_daily import sync_macro_daily_full
from data_sync_service.service.option_iv import sync_option_iv_daily
from data_sync_service.service.post_close_sync import run_post_close_sync
from data_sync_service.service.stock_basic import sync_stock_basic
from data_sync_service.service.top_inst_flow import sync_top_inst_watchlist
from data_sync_service.service.trade_calendar import sync_trade_calendar

router = APIRouter()


@router.get("/sync/eastmoney-industry/status")
def eastmoney_industry_status_endpoint() -> dict:
    """Coverage and latest sync job record for East Money industry mapping."""
    from data_sync_service.service.eastmoney_industry import get_eastmoney_industry_sync_status

    return get_eastmoney_industry_sync_status()


@router.post("/sync/eastmoney-industry")
def sync_eastmoney_industry_endpoint(
    mode: str = Query(
        "symbols",
        description="symbols: explicit list or stock_basic slice; missing|stale: incremental offline sync",
    ),
    symbols: list[str] | None = Query(
        None,
        description="Optional CN symbols, e.g. CN:000021. Used when mode=symbols.",
    ),
    limit: int | None = Query(
        500,
        ge=1,
        le=5000,
        description="Batch size for missing/stale modes, or stock_basic slice when mode=symbols without symbols.",
    ),
    max_stale_days: int = Query(30, ge=1, le=365, description="Stale threshold when mode=stale"),
) -> dict:
    """Sync East Money industry labels (offline HTTP; not for TrendOK hot path)."""
    from data_sync_service.service.eastmoney_industry import (
        sync_eastmoney_industry,
        sync_eastmoney_industry_incremental,
    )

    mode_norm = (mode or "symbols").strip().lower()
    if mode_norm in ("missing", "stale"):
        return sync_eastmoney_industry_incremental(
            mode=mode_norm,  # type: ignore[arg-type]
            batch_size=limit or 500,
            max_batches=1,
            max_stale_days=max_stale_days,
        )
    return sync_eastmoney_industry(symbols=symbols, limit=limit)


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


@router.post("/sync/etf-fund-basic")
def sync_etf_fund_basic_endpoint(
    list_status: str = Query("L", description="Listing status: L listed, D delisted, P suspended"),
    force: bool = Query(False, description="Force sync even if already synced this month"),
) -> dict:
    # Purpose: pull fund_basic(market='E') from tushare into stock_basic table (market='ETF').
    """Sync ETF fund_basic from tushare into stock_basic table."""
    return sync_etf_fund_basic(list_status=list_status, force=bool(force))


@router.get("/sync/etf-fund-basic/status")
def sync_etf_fund_basic_status_endpoint() -> dict:
    """Return the last sync status for ETF fund_basic."""
    return get_etf_fund_basic_sync_status()


@router.post("/sync/etf-daily")
def sync_etf_daily_endpoint() -> dict:
    """Trigger full ETF daily K-line sync into daily table."""
    return sync_etf_daily_full()


@router.get("/sync/etf-daily/status")
def sync_etf_daily_status_endpoint() -> dict:
    """Return today's run record for etf_daily_full."""
    return get_etf_daily_sync_status()


@router.post("/market/sync")
def market_sync_endpoint() -> dict:
    # Purpose: compatibility endpoint for MarketPage; calls sync_stock_basic.
    """Sync market stocks (alias for /sync/stock-basic)."""
    from datetime import datetime

    result = sync_stock_basic()
    synced_at = datetime.now(UTC).isoformat()

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
    # Deprecated: per-stock full sync replaced by close_sync (market-wide by trade_date).
    """Trigger close-time daily sync (replaces legacy per-stock sync_daily_full)."""
    result = sync_close(exchange="SSE", force=False)
    if isinstance(result, dict):
        return {**result, "deprecated": "use /sync/close", "legacy": "sync_daily_full"}
    return {"ok": True, "result": result, "deprecated": "use /sync/close", "legacy": "sync_daily_full"}


@router.post("/sync/hk-daily")
def sync_hk_daily_endpoint() -> dict:
    # Purpose: full HK daily sync into daily table; skip if today already succeeded.
    """Trigger full HK daily sync into daily table. Skips if today already succeeded; resumes from failure."""
    return sync_hk_daily_full()


@router.get("/sync/hk-daily/status")
def sync_hk_daily_status_endpoint() -> dict:
    """Return today's run record for hk_daily_full (success / last_ts_code / error)."""
    return get_hk_daily_sync_status()


@router.post("/sync/hk-industry")
def sync_hk_industry_endpoint(
    symbols: list[str] | None = Query(
        None,
        description="Optional explicit HK ts_codes, e.g. 00700.HK. Overrides limit when set.",
    ),
    limit: int = Query(500, ge=1, le=5000, description="Max HK codes to update when symbols is empty"),
) -> dict:
    """Sync HK stock industry labels from Xueqiu mbu into stock_basic.industry."""
    from data_sync_service.service.hk_industry import sync_hk_industry

    return sync_hk_industry(symbols=symbols, limit=limit)


@router.get("/sync/hk-industry/status")
def sync_hk_industry_status_endpoint() -> dict:
    """Return HK industry coverage (mapped / total)."""
    from data_sync_service.service.hk_industry import get_hk_industry_status

    return get_hk_industry_status()


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


@router.post("/sync/index-basic")
def sync_index_basic_endpoint() -> dict:
    """Trigger full sync of index_dailybasic (market breadth indicators)."""
    return sync_index_basic_full()


@router.post("/sync/macro-daily")
def sync_macro_daily_endpoint(force: bool = Query(False, description="Force sync even if already synced today")) -> dict:
    """Trigger full sync of macro/global daily series. Skips if today already succeeded; resumes from failure."""
    from data_sync_service.db.sync_job_record import ensure_table, get_connection
    if force:
        ensure_table()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sync_job_record WHERE job_type = 'macro_daily_full'")
            conn.commit()
    return sync_macro_daily_full()


@router.post("/sync/trade-cal")
def sync_trade_cal_endpoint(
    exchange: str = Query("SSE"),
    start_date: str | None = Query(None, description="Start date YYYYMMDD"),
    end_date: str | None = Query(None, description="End date YYYYMMDD"),
) -> dict:
    # Purpose: manually sync trade calendar into DB for given exchange/date range.
    """Manually sync trade calendar into DB."""
    return sync_trade_calendar(exchange=exchange, start_date=start_date, end_date=end_date)


@router.post("/sync/etf-fund-flow-watchlist")
def sync_etf_fund_flow_watchlist_endpoint(
    force: bool = Query(False, description="Force sync even if already synced today"),
) -> dict:
    """Sync ETF fund share / net inflow for dashboard watchlist."""
    return sync_etf_fund_flow_watchlist(force=bool(force))


@router.post("/sync/option-iv-daily")
def sync_option_iv_daily_endpoint(
    force: bool = Query(False, description="Force sync even if already synced today"),
    trade_date: str | None = Query(None, description="Trade date YYYYMMDD"),
) -> dict:
    """Sync 510300 ATM put IV into macro_daily."""
    return sync_option_iv_daily(force=bool(force), trade_date=trade_date)


@router.post("/sync/top-inst-watchlist")
def sync_top_inst_watchlist_endpoint(
    force: bool = Query(False, description="Force sync even if already synced today"),
    trade_date: str | None = Query(None, description="Trade date YYYYMMDD; default latest open day"),
) -> dict:
    """Sync dragon-tiger institutional flow for watchlist symbols."""
    return sync_top_inst_watchlist(force=bool(force), trade_date=trade_date)


@router.post("/sync/close")
def sync_close_endpoint(exchange: str = Query("SSE"), force: bool = Query(False)) -> dict:
    # Purpose: close-time sync by trade_date window; pulls daily + adj_factor (paged).
    """Close-time sync by trade_date window: daily + adj_factor (paged)."""
    result = sync_close(exchange=exchange, force=bool(force))
    if isinstance(result, dict):
        if not result.get("ok"):
            return result
        post = run_post_close_sync()
        return {**result, **post}
    post = run_post_close_sync()
    return {"ok": True, "result": result, **post}


# Job types that write their own row into sync_job_record. Every job that
# runs from the scheduler should land here so the UI can show per-job OK/FAIL
# status. (job_type may differ from the scheduler JOB_ID — keep the value the
# scheduler write_record actually inserts.)
SYNC_JOB_TYPES: tuple[str, ...] = (
    "stock_basic_sync",
    "hk_basic_sync",
    "hk_daily_full",
    "hk_industry_sync",
    "etf_fund_basic_sync",
    "etf_daily_full",
    "stock_daily_full",
    "stock_adj_factor_full",
    "stock_close_sync",
    "stock_close_catchup",
    "index_daily_full",
    "index_basic_sync",
    "macro_daily_full",
    "eastmoney_industry_sync",
    "alpha_radar_pipeline",
    "alpha_radar_ingest",
    "alpha_radar_process",
    "watchlist_automation",
    "cn_industry_post_close_sync",
    "tv_screener_capture_am",
    "tv_screener_capture_pm",
    "news_fetch_job",
    "news_enrich_job",
    "morning_brief_am",
    "morning_brief_pm",
)


@router.get("/sync/jobs")
def sync_jobs_status_endpoint() -> dict:
    """Aggregate status for all scheduled sync jobs.

    Returns today's run and last success for each known job_type (from
    sync_job_record), plus extras for HK industry coverage, Alpha Radar
    pipeline state, and the latest watchlist automation run.
    """
    # Lazy imports keep startup lean and tolerate missing optional deps.
    from data_sync_service.db.sync_job_record import get_last_success, get_today_run

    jobs: dict[str, dict[str, Any]] = {}
    for job_type in SYNC_JOB_TYPES:
        jobs[job_type] = {
            "todayRun": get_today_run(job_type),
            "lastSuccess": get_last_success(job_type),
        }

    hk_industry_coverage: dict[str, Any] | None = None
    try:
        from data_sync_service.service.hk_industry import get_hk_industry_status

        hk_industry_coverage = get_hk_industry_status()
    except Exception as exc:  # noqa: BLE001
        hk_industry_coverage = {"ok": False, "error": str(exc)}

    alpha_radar: dict[str, Any] | None = None
    try:
        from data_sync_service.service.alpha_radar_pipeline import pipeline_status

        alpha_radar = pipeline_status()
    except Exception as exc:  # noqa: BLE001
        alpha_radar = {"ok": False, "error": str(exc)}

    watchlist_automation: dict[str, Any] | None = None
    try:
        from data_sync_service.db.watchlist_automation import get_latest_run

        watchlist_automation = get_latest_run()
    except Exception as exc:  # noqa: BLE001
        watchlist_automation = {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "jobs": jobs,
        "hkIndustryCoverage": hk_industry_coverage,
        "alphaRadar": alpha_radar,
        "watchlistAutomation": watchlist_automation,
    }
