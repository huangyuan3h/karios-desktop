from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel  # type: ignore[import-not-found]

from data_sync_service.db import check_db
from data_sync_service.service.adj_factor import get_adj_factor_sync_status
from data_sync_service.service.close_sync import get_close_sync_status
from data_sync_service.service.daily import get_daily_from_db, get_daily_sync_status
from data_sync_service.service.market_bars import get_market_bars
from data_sync_service.service.market_detail import get_market_chips, get_market_fund_flow
from data_sync_service.service.realtime_quote import fetch_realtime_quotes
from data_sync_service.db.stock_basic import fetch_market_stocks, get_market_status
from data_sync_service.service.market_quotes import get_market_quotes_batch, symbol_to_ts_code
from data_sync_service.service.stock_basic import get_stock_basic_list, get_stock_basic_sync_status
from data_sync_service.service.trendok import compute_trendok_for_symbols
from data_sync_service.service.watchlist_v5_alerts import compute_watchlist_v5_alerts, compute_watchlist_v5_plan
from data_sync_service.service.watchlist_momentum_alerts import compute_watchlist_momentum_alerts
from data_sync_service.db.index_daily import fetch_index_daily
from data_sync_service.testback.engine import BacktestParams as EngineParams, DailyRuleFilter as EngineRules, UniverseFilter as EngineUniverse, run_backtest
from data_sync_service.testback.strategies.base import ScoreConfig as EngineScore
from data_sync_service.testback.strategies import get_strategy_class
from data_sync_service.testback.db import (
    fetch_run as fetch_backtest_run,
    fetch_trades as fetch_backtest_trades,
    fetch_runs as fetch_backtest_runs,
    delete_run as delete_backtest_run,
    insert_run as insert_backtest_run,
    insert_trades as insert_backtest_trades,
    update_run_failed as update_backtest_failed,
    update_run_success as update_backtest_success,
)
from uuid import uuid4


class BacktestUniverse(BaseModel):
    market: str | None = "CN"
    exclude_keywords: list[str] = []
    min_list_days: int = 0


class BacktestRules(BaseModel):
    min_price: float | None = None
    max_price: float | None = None
    min_volume: float | None = None
    max_volume: float | None = None
    min_amount: float | None = None
    max_amount: float | None = None


class BacktestScoring(BaseModel):
    top_n: int = 1000
    momentum_weight: float = 1.0
    volume_weight: float = 0.0
    amount_weight: float = 0.0


class BacktestParams(BaseModel):
    initial_cash: float = 1.0
    fee_rate: float = 0.0
    slippage_rate: float = 0.0005
    adj_mode: str = "qfq"
    warmup_days: int = 20


class BacktestRunRequest(BaseModel):
    strategy: str
    start_date: str
    end_date: str
    universe: BacktestUniverse | None = None
    rules: BacktestRules | None = None
    scoring: BacktestScoring | None = None
    params: BacktestParams | None = None


class WatchlistAlertItem(BaseModel):
    symbol: str
    position_pct: float | None = None


class WatchlistAlertsRequest(BaseModel):
    items: list[WatchlistAlertItem] = []

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


@router.get("/index-daily")
def get_index_daily_endpoint(
    ts_code: str | None = Query(None, description="Filter by ts_code"),
    start_date: str | None = Query(None, description="Start date YYYY-MM-DD"),
    end_date: str | None = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=50000),
) -> list:
    """Return index daily bars from our database. Optional filters; default limit 5000."""
    return fetch_index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date, limit=limit)


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


@router.get("/market/stocks/{symbol}/chips")
def get_market_chips_endpoint(
    symbol: str,
    days: int = Query(60, ge=10, le=200),
    force: bool = False,
) -> dict:
    # Purpose: compatibility endpoint for StockPage "chip distribution" enrichment.
    try:
        return get_market_chips(symbol=symbol, days=days, force=bool(force))
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/market/stocks/{symbol}/fund-flow")
def get_market_fund_flow_endpoint(
    symbol: str,
    days: int = Query(60, ge=10, le=200),
    force: bool = False,
) -> dict:
    # Purpose: compatibility endpoint for StockPage "fund flow" enrichment.
    try:
        return get_market_fund_flow(symbol=symbol, days=days, force=bool(force))
    except HTTPException:
        raise
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


@router.get("/market/status")
def get_market_status_endpoint() -> dict:
    """Return market status: total stocks count and last sync time."""
    return get_market_status()


@router.get("/market/stocks")
def get_market_stocks_endpoint(
    market: str | None = Query(None, description="Filter by market: CN or HK"),
    q: str | None = Query(None, description="Search by ticker or name"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    use_realtime: bool = Query(False, description="Use realtime quotes instead of daily close"),
) -> dict:
    """
    List market stocks with pagination and filters.
    Returns MarketStocksResponse-compatible format.
    Price and change% are fetched from daily table (latest close) or realtime API.
    """
    total, items = fetch_market_stocks(market=market, q=q, offset=offset, limit=limit, use_realtime=use_realtime)
    return {
        "items": items,
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/market/stocks/quotes")
def get_market_stocks_quotes_endpoint(
    symbols: list[str] | None = Query(None, description="List of symbols (e.g., CN:000001,CN:600000)"),
    use_realtime: bool = Query(False, description="Use realtime quotes instead of daily close"),
) -> dict:
    """
    Batch fetch price and change% for multiple symbols.
    Returns mapping: symbol -> {price, changePct, volume, turnover}.
    """
    syms0 = symbols if isinstance(symbols, list) else []
    syms = [str(s or "").strip() for s in syms0 if s and s.strip()]
    if not syms:
        return {"quotes": {}}
    if len(syms) > 500:
        syms = syms[:500]

    # Convert symbols to ts_codes
    ts_codes: list[str] = []
    symbol_to_code: dict[str, str] = {}
    for sym in syms:
        code = symbol_to_ts_code(sym)
        if code:
            ts_codes.append(code)
            symbol_to_code[sym] = code

    if not ts_codes:
        return {"quotes": {}}

    # Fetch quotes
    quotes_map = get_market_quotes_batch(ts_codes, use_realtime=use_realtime)

    # Map back to symbols
    out: dict[str, dict[str, str | None]] = {}
    for sym in syms:
        code = symbol_to_code.get(sym)
        if code:
            quote = quotes_map.get(code, {})
            out[sym] = {
                "price": quote.get("price"),
                "changePct": quote.get("changePct"),
                "volume": quote.get("volume"),
                "turnover": quote.get("turnover"),
            }
        else:
            out[sym] = {
                "price": None,
                "changePct": None,
                "volume": None,
                "turnover": None,
            }

    return {"quotes": out}


@router.get("/search/stocks")
def search_stocks_endpoint(
    q: str | None = Query(None, description="Search query: ticker/name"),
    market: str | None = Query(None, description="Optional market filter: CN or HK"),
    limit: int = Query(8, ge=1, le=20),
    use_realtime: bool = Query(False, description="Use realtime quotes instead of daily close"),
) -> dict:
    """
    Global stock search for AppShell's quick search box.
    Returns a minimal shape: { items: [...] }.
    """
    q2 = (q or "").strip()
    if not q2:
        return {"items": []}
    _total, items = fetch_market_stocks(market=market, q=q2, offset=0, limit=int(limit), use_realtime=use_realtime)
    return {"items": items}


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


@router.post("/market/stocks/watchlist/v5-alerts")
def watchlist_v5_alerts_endpoint(req: WatchlistAlertsRequest) -> list[dict]:
    items = [x.model_dump() for x in (req.items or [])]
    return compute_watchlist_v5_alerts(items)


@router.post("/market/stocks/watchlist/v5-plan")
def watchlist_v5_plan_endpoint(req: WatchlistAlertsRequest) -> dict:
    items = [x.model_dump() for x in (req.items or [])]
    return compute_watchlist_v5_plan(items)


@router.post("/market/stocks/watchlist/momentum-alerts")
def watchlist_momentum_alerts_endpoint(req: WatchlistAlertsRequest, realtime: bool = False) -> list[dict]:
    items = [x.model_dump() for x in (req.items or [])]
    return compute_watchlist_momentum_alerts(items, bool(realtime))  # type: ignore[call-arg]


@router.post("/backtest/run")
def run_backtest_endpoint(req: BacktestRunRequest) -> dict:
    """
    Run a backtest in one call and return run_id + summary.
    """
    try:
        strategy_cls = get_strategy_class(req.strategy)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(e)) from e
    run_id = str(uuid4())
    params = req.params or BacktestParams()
    universe = req.universe or BacktestUniverse()
    rules = req.rules or BacktestRules()
    scoring = req.scoring or BacktestScoring()
    insert_backtest_run(
        run_id=run_id,
        strategy_name=req.strategy,
        start_date=req.start_date,
        end_date=req.end_date,
        params={
            "strategy": req.strategy,
            "start_date": req.start_date,
            "end_date": req.end_date,
            "params": params.model_dump(),
            "universe": universe.model_dump(),
            "rules": rules.model_dump(),
            "scoring": scoring.model_dump(),
        },
    )
    try:
        result = run_backtest(
            strategy_cls=strategy_cls,
            params=EngineParams(
                start_date=req.start_date,
                end_date=req.end_date,
                initial_cash=params.initial_cash,
                fee_rate=params.fee_rate,
                slippage_rate=params.slippage_rate,
                adj_mode=params.adj_mode,
                warmup_days=params.warmup_days,
            ),
            universe_filter=EngineUniverse(
                market=universe.market,
                exclude_keywords=universe.exclude_keywords,
                min_list_days=universe.min_list_days,
            ),
            daily_rules=EngineRules(
                min_price=rules.min_price,
                max_price=rules.max_price,
                min_volume=rules.min_volume,
                max_volume=rules.max_volume,
                min_amount=rules.min_amount,
                max_amount=rules.max_amount,
            ),
            score_cfg=EngineScore(
                top_n=scoring.top_n,
                momentum_weight=scoring.momentum_weight,
                volume_weight=scoring.volume_weight,
                amount_weight=scoring.amount_weight,
            ),
        )
        insert_backtest_trades(run_id, result["trade_log"])
        update_backtest_success(
            run_id,
            summary=result["summary"],
            equity_curve=result["equity_curve"],
            drawdown_curve=result["drawdown_curve"],
            positions_curve=result["positions_curve"],
            daily_log=result["daily_log"],
        )
        return {"ok": True, "runId": run_id, "summary": result["summary"]}
    except Exception as e:  # noqa: BLE001
        update_backtest_failed(run_id, str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/backtest/result/{run_id}")
def get_backtest_result_endpoint(run_id: str) -> dict:
    run = fetch_backtest_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="backtest run not found")
    trades = fetch_backtest_trades(run_id)
    return {"run": run, "trades": trades}


@router.get("/backtest/runs")
def list_backtest_runs(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> dict:
    items = fetch_backtest_runs(limit=limit, offset=offset)
    return {"items": items, "limit": limit, "offset": offset}


@router.delete("/backtest/run/{run_id}")
def delete_backtest_run_endpoint(run_id: str) -> dict:
    ok = delete_backtest_run(run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="backtest run not found")
    return {"ok": True}
