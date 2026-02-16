from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from data_sync_service.db.daily import fetch_latest_trade_date_for_codes
from data_sync_service.service.market_quotes import symbol_to_ts_code
from data_sync_service.testback.engine import BacktestParams, DailyRuleFilter, UniverseFilter, run_backtest
from data_sync_service.testback.strategies.watchlist_momentum_rank import WatchlistMomentumRankStrategy


def _parse_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def compute_watchlist_momentum_rank_snapshot(
    items: list[dict[str, Any]],
    lookback_days: int = 30,
    warmup_days: int = 20,
) -> dict[str, Any]:
    symbols = []
    for it in items or []:
        sym = str(it.get("symbol") or "").strip().upper()
        if sym:
            symbols.append(sym)
    ts_codes = []
    for sym in symbols:
        ts_code = symbol_to_ts_code(sym)
        if ts_code:
            ts_codes.append(ts_code)

    if not ts_codes:
        return {"asOfDate": None, "positions": [], "recentOrders": [], "summary": {}, "error": "no_symbols"}

    end_date = fetch_latest_trade_date_for_codes(ts_codes)
    if not end_date:
        return {"asOfDate": None, "positions": [], "recentOrders": [], "summary": {}, "error": "no_trade_date"}

    end_dt = _parse_date(end_date)
    if not end_dt:
        return {"asOfDate": None, "positions": [], "recentOrders": [], "summary": {}, "error": "bad_trade_date"}

    lookback_days = max(1, int(lookback_days))
    warmup_days = max(0, int(warmup_days))
    start_dt = end_dt - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y-%m-%d")

    result = run_backtest(
        strategy_cls=WatchlistMomentumRankStrategy,
        params=BacktestParams(
            start_date=start_date,
            end_date=end_date,
            initial_cash=2_000_000,
            fee_rate=0.0,
            slippage_rate=0.0,
            adj_mode="qfq",
            warmup_days=warmup_days,
        ),
        universe_filter=UniverseFilter(market="CN"),
        daily_rules=DailyRuleFilter(),
        score_cfg=None,
        universe_override=ts_codes,
    )

    daily_log = result.get("daily_log", [])
    recent_days = []
    if isinstance(daily_log, list):
        for day in daily_log:
            date = day.get("date")
            if isinstance(date, str) and date >= start_date:
                recent_days.append(day)
    recent_orders: list[dict[str, Any]] = []
    last_buy_price: dict[str, float] = {}
    for day in recent_days:
        date = day.get("date")
        equity = day.get("equity")
        orders = day.get("orders") or []
        for o in orders:
            if o.get("status") != "executed":
                continue
            action = o.get("action")
            ts_code = o.get("ts_code")
            price = o.get("exec_price")
            qty = o.get("exec_qty") or o.get("qty")
            pnl_pct = None
            if action == "sell" and ts_code and ts_code in last_buy_price:
                base_price = last_buy_price.get(ts_code, 0.0)
                if base_price and price:
                    pnl_pct = (float(price) - float(base_price)) / float(base_price)
            if action == "buy" and ts_code and price:
                last_buy_price[ts_code] = float(price)
            position_pct = None
            if equity and price and qty:
                try:
                    position_pct = (float(qty) * float(price)) / float(equity)
                except (TypeError, ValueError, ZeroDivisionError):
                    position_pct = None
            recent_orders.append(
                {
                    "date": date,
                    "ts_code": ts_code,
                    "action": action,
                    "qty": qty,
                    "price": price,
                    "target_pct": o.get("target_pct"),
                    "reason": o.get("reason"),
                    "pnl_pct": pnl_pct,
                    "position_pct": position_pct,
                }
            )

    return {
        "asOfDate": result.get("as_of_date"),
        "summary": result.get("summary") or {},
        "positions": result.get("final_positions") or [],
        "recentOrders": recent_orders,
    }
