from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]

from data_sync_service.db import check_db
from data_sync_service.service.adj_factor import get_adj_factor_sync_status
from data_sync_service.service.close_sync import get_close_sync_status
from data_sync_service.service.daily import get_daily_from_db, get_daily_sync_status
from data_sync_service.service.market_bars import get_market_bars
from data_sync_service.service.realtime_quote import fetch_realtime_quotes
from data_sync_service.service.stock_basic import get_stock_basic_list, get_stock_basic_sync_status
from data_sync_service.service.trendok import compute_trendok_for_symbols

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


@router.get("/stock-basic/status")
def get_stock_basic_status_endpoint() -> dict:
    # Purpose: return today's stock_basic sync status from sync_job_record.
    """Return today's stock_basic sync run record (success/fail)."""
    return get_stock_basic_sync_status()


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


@router.get("/quote")
def get_quote_endpoint(
    ts_code: str | None = Query(None, description="Single ts_code, e.g. 000001.SZ"),
    ts_codes: str | None = Query(None, description="Comma-separated ts_code list"),
) -> dict:
    # Purpose: query realtime quote directly from tushare (query-only; no DB writes).
    # Inputs: ts_code or ts_codes; Outputs: normalized quote items (strings).
    codes: list[str] = []
    if ts_code:
        codes.append(ts_code)
    if ts_codes:
        codes.extend([c.strip() for c in ts_codes.split(",") if c.strip()])
    return fetch_realtime_quotes(codes)


@router.get("/market/stocks/{symbol}/bars")
def get_market_bars_endpoint(symbol: str, days: int = Query(60, ge=10, le=200), force: bool = False) -> dict:
    # Purpose: compatibility endpoint for StockPage candlestick chart.
    # Inputs: symbol like CN:000001, days; force is accepted for compatibility but ignored (query-only).
    _ = force
    try:
        return get_market_bars(symbol=symbol, days=days)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/market/stocks/trendok")
def get_trendok_endpoint(
    symbols: list[str] | None = Query(None),
    refresh: bool = False,
    realtime: bool = False,
) -> list[dict]:
    # Purpose: TrendOK/Score computation for Watchlist (CN daily only), fully based on data-sync-service DB.
    syms = symbols if isinstance(symbols, list) else []
    return compute_trendok_for_symbols(syms, bool(refresh), bool(realtime))


@router.get("/market/stocks/resolve")
def resolve_symbols_endpoint(symbols: list[str] | None = Query(None)) -> list[dict]:
    """
    Purpose: Resolve symbols (CN:xxxxxx) to name/ticker/market for Watchlist.
    Source: data-sync-service stock_basic table (tushare).
    """
    syms0 = symbols if isinstance(symbols, list) else []
    syms = [str(s or "").strip().upper() for s in syms0]
    syms = [s for s in syms if s]
    if not syms:
        return []
    if len(syms) > 500:
        syms = syms[:500]

    # Map CN:xxxxxx -> ts_code
    want: dict[str, str] = {}
    for sym in syms:
        if sym.startswith("CN:"):
            ticker = sym.split(":", 1)[1].strip()
            if len(ticker) == 6 and ticker.isdigit():
                suffix = "SH" if ticker.startswith("6") else "SZ"
                want[sym] = f"{ticker}.{suffix}"

    if not want:
        return []

    try:
        from data_sync_service.db import get_connection
        from data_sync_service.db.stock_basic import ensure_table as ensure_sb

        ensure_sb()
        ts_codes = list(want.values())
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, symbol, name FROM stock_basic WHERE ts_code = ANY(%s)",
                    (ts_codes,),
                )
                rows = cur.fetchall()
        by_code = {str(r[0]): {"ticker": str(r[1]), "name": str(r[2])} for r in rows if r and r[0]}
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e

    out: list[dict] = []
    for sym, code in want.items():
        hit = by_code.get(code)
        if not hit:
            continue
        out.append(
            {
                "symbol": sym,
                "market": "CN",
                "ticker": hit["ticker"],
                "name": hit["name"],
                "currency": "CNY",
            }
        )
    return out
