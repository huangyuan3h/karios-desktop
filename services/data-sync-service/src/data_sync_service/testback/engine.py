from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import log1p
from typing import Any, Dict, List, Tuple

from data_sync_service.db.daily import fetch_daily_for_codes
from data_sync_service.testback.universe import build_universe
from data_sync_service.testback.strategies.base import Bar, Order, PortfolioSnapshot


@dataclass
class BacktestParams:
    start_date: str
    end_date: str
    initial_cash: float = 1.0
    fee_rate: float = 0.0
    slippage_rate: float = 0.0
    adj_mode: str = "qfq"
    warmup_days: int = 20


@dataclass
class UniverseFilter:
    market: str | None = "CN"
    exclude_keywords: list[str] | None = None
    min_list_days: int = 0


@dataclass
class DailyRuleFilter:
    min_price: float | None = None
    max_price: float | None = None
    min_volume: float | None = None
    max_volume: float | None = None
    min_amount: float | None = None
    max_amount: float | None = None


@dataclass
class ScoreConfig:
    top_n: int = 1000
    momentum_weight: float = 1.0
    volume_weight: float = 0.0
    amount_weight: float = 0.0


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
    except Exception:
        return default
    return float(val)


def _parse_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def _format_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _warmup_start_date(start_date: str, warmup_days: int) -> str:
    dt = _parse_date(start_date)
    if not dt:
        return start_date
    days = max(0, int(warmup_days))
    # Use calendar days to approximate 20 trading days.
    return _format_date(dt - timedelta(days=days * 2))


def _adjust_factor_ratio(rows: list[dict[str, Any]], adj_mode: str) -> float:
    if adj_mode == "hfq":
        return 1.0
    last_factor = None
    for r in reversed(rows):
        f = _safe_float(r.get("adj_factor"), 0.0)
        if f > 0:
            last_factor = f
            break
    if not last_factor:
        return 1.0
    return 1.0 / last_factor


def _build_bar_maps(
    rows: list[dict[str, Any]],
    adj_mode: str,
) -> Tuple[Dict[str, Dict[str, Bar]], Dict[str, Dict[str, float]]]:
    by_code: Dict[str, List[dict[str, Any]]] = {}
    for r in rows:
        code = str(r.get("ts_code") or "").strip()
        if not code:
            continue
        by_code.setdefault(code, []).append(r)
    bars_by_date: Dict[str, Dict[str, Bar]] = {}
    prev_close_map: Dict[str, Dict[str, float]] = {}
    for code, items in by_code.items():
        items_sorted = sorted(items, key=lambda x: str(x.get("trade_date") or ""))
        ratio = _adjust_factor_ratio(items_sorted, adj_mode)
        prev_close = None
        for r in items_sorted:
            trade_date = str(r.get("trade_date") or "")
            if not trade_date:
                continue
            factor = _safe_float(r.get("adj_factor"), 1.0)
            if adj_mode == "qfq":
                adj_multiplier = factor * ratio
            else:
                adj_multiplier = factor
            bar = Bar(
                ts_code=code,
                trade_date=trade_date,
                open=_safe_float(r.get("open")) * adj_multiplier,
                high=_safe_float(r.get("high")) * adj_multiplier,
                low=_safe_float(r.get("low")) * adj_multiplier,
                close=_safe_float(r.get("close")) * adj_multiplier,
                avg_price=(
                    _safe_float(r.get("open")) * adj_multiplier
                    + _safe_float(r.get("high")) * adj_multiplier
                    + _safe_float(r.get("low")) * adj_multiplier
                    + _safe_float(r.get("close")) * adj_multiplier
                )
                / 4.0,
                volume=_safe_float(r.get("vol")),
                amount=_safe_float(r.get("amount")),
            )
            bars_by_date.setdefault(trade_date, {})[code] = bar
            prev_close_map.setdefault(trade_date, {})[code] = prev_close if prev_close else bar.close
            prev_close = bar.close
    return bars_by_date, prev_close_map


def _apply_daily_rules(bar: Bar, rules: DailyRuleFilter) -> bool:
    if rules.min_price is not None and bar.close < rules.min_price:
        return False
    if rules.max_price is not None and bar.close > rules.max_price:
        return False
    if rules.min_volume is not None and bar.volume < rules.min_volume:
        return False
    if rules.max_volume is not None and bar.volume > rules.max_volume:
        return False
    if rules.min_amount is not None and bar.amount < rules.min_amount:
        return False
    if rules.max_amount is not None and bar.amount > rules.max_amount:
        return False
    return True


def _score_bar(bar: Bar, prev_close: float, score: ScoreConfig) -> float:
    momentum = 0.0
    if prev_close > 0:
        momentum = (bar.close / prev_close) - 1.0
    score_val = score.momentum_weight * momentum
    score_val += score.volume_weight * log1p(max(bar.volume, 0.0))
    score_val += score.amount_weight * log1p(max(bar.amount, 0.0))
    return score_val


def _pick_top_n(
    bars: Dict[str, Bar],
    prev_close_map: Dict[str, float],
    rules: DailyRuleFilter,
    score_cfg: ScoreConfig,
) -> Tuple[Dict[str, Bar], List[Tuple[str, float]]]:
    scored: List[Tuple[str, float]] = []
    for code, bar in bars.items():
        if not _apply_daily_rules(bar, rules):
            continue
        prev_close = prev_close_map.get(code, bar.close)
        scored.append((code, _score_bar(bar, prev_close, score_cfg)))
    if not scored:
        return {}, []
    scored.sort(key=lambda x: (-x[1], x[0]))
    top_n = max(1, int(score_cfg.top_n))
    keep = set(code for code, _ in scored[:top_n])
    selected = {code: bars[code] for code in sorted(keep) if code in bars}
    return selected, scored


def _execute_order(
    order: Order,
    bar: Bar,
    cash: float,
    positions: Dict[str, float],
    fee_rate: float,
    slippage_rate: float,
    current_equity: float,
) -> Tuple[float, Dict[str, float], dict[str, Any] | None]:
    action = (order.action or "").lower().strip()
    if action not in ("buy", "sell"):
        return cash, positions, None
    price = max(bar.avg_price, 0.0)
    if price <= 0:
        return cash, positions, None
    current_qty = positions.get(bar.ts_code, 0.0)
    qty = order.qty
    if order.target_pct is not None:
        target_pct = min(max(order.target_pct, 0.0), 1.0)
        target_value = current_equity * target_pct
        desired_qty = target_value / price
        delta_qty = desired_qty - current_qty
        action = "buy" if delta_qty > 0 else "sell"
        qty = abs(delta_qty)
    if qty is None or qty <= 0:
        return cash, positions, None
    if action == "buy":
        exec_price = price * (1.0 + slippage_rate)
        cost = qty * exec_price
        fee = cost * fee_rate
        total_cost = cost + fee
        if total_cost > cash and exec_price > 0:
            qty = cash / (exec_price * (1.0 + fee_rate))
            cost = qty * exec_price
            fee = cost * fee_rate
            total_cost = cost + fee
        if qty <= 0:
            return cash, positions, None
        cash -= total_cost
        positions[bar.ts_code] = current_qty + qty
        return cash, positions, {
            "ts_code": bar.ts_code,
            "trade_date": bar.trade_date,
            "action": "buy",
            "qty": qty,
            "price": exec_price,
            "fee": fee,
            "cash_after": cash,
            "reason": order.reason,
        }
    exec_price = price * (1.0 - slippage_rate)
    qty2 = min(qty, current_qty)
    if qty2 <= 0:
        return cash, positions, None
    proceeds = qty2 * exec_price
    fee = proceeds * fee_rate
    cash += proceeds - fee
    new_qty = current_qty - qty2
    if new_qty <= 0:
        positions.pop(bar.ts_code, None)
    else:
        positions[bar.ts_code] = new_qty
    return cash, positions, {
        "ts_code": bar.ts_code,
        "trade_date": bar.trade_date,
        "action": "sell",
        "qty": qty2,
        "price": exec_price,
        "fee": fee,
        "cash_after": cash,
        "reason": order.reason,
    }


def run_backtest(
    strategy_cls,
    params: BacktestParams,
    universe_filter: UniverseFilter,
    daily_rules: DailyRuleFilter,
    score_cfg: ScoreConfig,
) -> dict[str, Any]:
    strategy = strategy_cls()
    warmup_start = _warmup_start_date(params.start_date, params.warmup_days)
    universe = build_universe(
        as_of_date=params.start_date,
        market=universe_filter.market,
        exclude_keywords=universe_filter.exclude_keywords,
        min_list_days=universe_filter.min_list_days,
    )
    rows = fetch_daily_for_codes(universe, warmup_start, params.end_date)
    bars_by_date, prev_close_map = _build_bar_maps(rows, params.adj_mode)
    dates = sorted(bars_by_date.keys())
    cash = max(0.0, params.initial_cash)
    positions: Dict[str, float] = {}
    last_prices: Dict[str, float] = {}
    last_buy_date: Dict[str, str] = {}
    equity_curve: list[dict[str, Any]] = []
    drawdown_curve: list[dict[str, Any]] = []
    positions_curve: list[dict[str, Any]] = []
    daily_log: list[dict[str, Any]] = []
    trade_log: list[dict[str, Any]] = []
    peak_equity = cash
    strategy.on_start(params.start_date, params.end_date)
    for d in dates:
        bars = bars_by_date.get(d, {})
        prev_map = prev_close_map.get(d, {})
        selected, scored = _pick_top_n(bars, prev_map, daily_rules, score_cfg)
        for code, bar in bars.items():
            last_prices[code] = bar.close
        equity = cash + sum(positions.get(code, 0.0) * last_prices.get(code, 0.0) for code in positions)
        snapshot = PortfolioSnapshot(cash=cash, equity=equity, positions=dict(positions))
        if d < params.start_date:
            # Warmup: feed bars to strategy, but do not trade or log.
            _ = strategy.on_bar(d, selected, snapshot)
            continue
        ordered_selected = {code: selected[code] for code in sorted(selected)}
        orders = strategy.on_bar(d, ordered_selected, snapshot)
        order_by_code: Dict[str, Order] = {}
        for o in orders:
            if o.ts_code:
                order_by_code[o.ts_code] = o
        day_orders: list[dict[str, Any]] = []
        for key in sorted(order_by_code.keys()):
            order = order_by_code[key]
            bar_opt = bars.get(order.ts_code)
            if bar_opt is None:
                continue
            bar = bar_opt
            intended_action = (order.action or "").lower().strip()
            if order.target_pct is not None:
                target_pct = min(max(order.target_pct, 0.0), 1.0)
                current_qty = positions.get(order.ts_code, 0.0)
                desired_qty = (equity * target_pct) / max(bar.avg_price, 0.000001)
                if desired_qty < current_qty:
                    intended_action = "sell"
                elif desired_qty > current_qty:
                    intended_action = "buy"
            if intended_action == "sell" and last_buy_date.get(order.ts_code) == d:
                day_orders.append(
                    {
                        "ts_code": order.ts_code,
                        "action": order.action,
                        "qty": order.qty,
                        "target_pct": order.target_pct,
                        "reason": "t+1: same-day sell blocked",
                        "status": "skipped",
                        "exec_qty": None,
                        "exec_price": None,
                    }
                )
                continue
            if intended_action == "buy" and cash <= 0:
                day_orders.append(
                    {
                        "ts_code": order.ts_code,
                        "action": order.action,
                        "qty": order.qty,
                        "target_pct": order.target_pct,
                        "reason": "no cash: buy blocked",
                        "status": "skipped",
                        "exec_qty": None,
                        "exec_price": None,
                    }
                )
                continue
            cash, positions, trade = _execute_order(
                order,
                bar,
                cash,
                positions,
                params.fee_rate,
                params.slippage_rate,
                equity,
            )
            day_orders.append(
                {
                    "ts_code": order.ts_code,
                    "action": order.action,
                    "qty": order.qty,
                    "target_pct": order.target_pct,
                    "reason": order.reason,
                    "status": "executed" if trade else "ignored",
                    "exec_qty": trade.get("qty") if trade else None,
                    "exec_price": trade.get("price") if trade else None,
                }
            )
            if trade:
                trade_log.append(trade)
                if trade.get("action") == "buy":
                    last_buy_date[order.ts_code] = d
        equity = cash + sum(positions.get(code, 0.0) * last_prices.get(code, 0.0) for code in positions)
        peak_equity = max(peak_equity, equity)
        drawdown = 0.0 if peak_equity <= 0 else (equity / peak_equity) - 1.0
        invested = 0.0
        if equity > 0:
            invested_value = sum(positions.get(code, 0.0) * last_prices.get(code, 0.0) for code in positions)
            invested = invested_value / equity
        equity_curve.append({"date": d, "equity": equity})
        drawdown_curve.append({"date": d, "drawdown": drawdown})
        positions_curve.append({"date": d, "invested_ratio": invested})
        daily_log.append(
            {
                "date": d,
                "selected": [
                    {
                        "ts_code": code,
                        "score": score,
                        "close": bars[code].close,
                        "avg_price": bars[code].avg_price,
                    }
                    for code, score in scored[: max(1, int(score_cfg.top_n))]
                    if code in bars
                ],
                "orders": day_orders,
                "positions": [
                    {"ts_code": code, "qty": qty}
                    for code, qty in sorted(positions.items(), key=lambda x: (-x[1], x[0]))
                ],
                "cash": cash,
                "equity": equity,
            }
        )
    total_return = 0.0
    if params.initial_cash > 0 and equity_curve:
        total_return = (equity_curve[-1]["equity"] / params.initial_cash) - 1.0
    max_drawdown = min((item["drawdown"] for item in drawdown_curve), default=0.0)
    summary = {
        "total_return": total_return,
        "max_drawdown": max_drawdown,
        "total_trades": len(trade_log),
        "final_equity": equity_curve[-1]["equity"] if equity_curve else cash,
    }
    return {
        "summary": summary,
        "equity_curve": equity_curve,
        "drawdown_curve": drawdown_curve,
        "positions_curve": positions_curve,
        "daily_log": daily_log,
        "trade_log": trade_log,
    }
