from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.trendok import _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class AlphaHunterV1Strategy(BaseStrategy):
    """
    AlphaHunter V1 (defensive momentum).

    Design goals:
    - Filter out low-liquidity and low-price names.
    - Use relative strength + pullback stabilization for entry.
    - Apply inverse-volatility weighting for position sizing.
    - Enforce hard stop, time stop, trailing stop, and MA10 trend break.
    - Limit daily turnover to control churn.
    """

    name = "alpha_hunter_v1"
    use_full_bars = True
    top_k = 1000

    def __init__(self) -> None:
        self.max_positions = 12
        self.min_price = 5.0
        self.min_amount = 100_000_000.0
        self.rs_threshold = 1.2
        self.rsi_low = 45.0
        self.rsi_high = 65.0
        self.volume_boost = 1.1
        self.risk_budget = 0.02
        self.hard_stop_pct = 0.04
        self.trailing_stop_pct = 0.05
        self.time_stop_days = 5
        self.min_profit_after_time_stop = 0.01
        self.max_daily_sells = 5
        self.max_daily_buys = 5
        self._entry_info: Dict[str, dict] = {}
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=260))

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []

        for code, bar in bars.items():
            self._history[code].append(bar)

        market_strength = self._calc_market_strength(bars)
        if market_strength < 0.3:
            max_invested_ratio = 0.3
        elif market_strength < 0.6:
            max_invested_ratio = 0.6
        else:
            max_invested_ratio = 1.0

        target_equity = portfolio.equity * max_invested_ratio
        orders: List[Order] = []
        candidates: list[tuple[str, float, float]] = []

        market_ret20 = self._calc_market_ret20()
        amount_threshold = self._calc_dynamic_amount_threshold(bars)

        for code, bar in bars.items():
            if bar.close < self.min_price:
                continue
            if bar.amount < amount_threshold:
                continue

            ma20 = self._get_ma(code, 20)
            ma60 = self._get_ma(code, 60)
            ma10 = self._get_ma(code, 10)
            rsi = self._get_rsi(code)
            if ma20 <= 0 or ma60 <= 0:
                continue

            trend_up = bar.close >= ma20 and ma20 > ma60
            if not trend_up:
                continue
            if not (self.rsi_low <= rsi <= self.rsi_high):
                continue

            avg_vol5 = self._get_avg_volume(code, 5)
            if avg_vol5 > 0 and bar.volume < avg_vol5 * self.volume_boost:
                continue

            ret20 = self._get_return(code, 20)
            ret60 = self._get_return(code, 60)
            rs_score = self._calc_rs(ret20, market_ret20)
            if rs_score < self.rs_threshold:
                continue

            flow_score = self._get_flow_score(code)
            score = 0.5 * ret20 + 0.3 * ret60 + 0.2 * flow_score
            atr = self._get_atr(code, 14)
            if atr <= 0:
                continue
            candidates.append((code, score, atr))

        candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)
        desired = candidates[: self.max_positions]
        desired_set = {code for code, _score, _atr in desired}

        # Exit logic
        for code, qty in portfolio.positions.items():
            if qty <= 0:
                continue
            bar = bars.get(code)
            if bar is None:
                continue

            entry_info = self._entry_info.get(code)
            if not entry_info:
                self._entry_info[code] = {"price": bar.close, "peak": bar.close, "bar_count": 0}
                continue

            entry_info["peak"] = max(entry_info["peak"], bar.high)
            entry_info["bar_count"] += 1
            entry_price = entry_info["price"]
            profit_pct = (bar.close - entry_price) / entry_price if entry_price > 0 else 0.0

            hard_stop = entry_price * (1 - self.hard_stop_pct)
            trail_stop = entry_info["peak"] * (1 - self.trailing_stop_pct)
            trend_broken = bar.close < self._get_ma(code, 10)
            time_stop = entry_info["bar_count"] >= self.time_stop_days and profit_pct < self.min_profit_after_time_stop

            if bar.close <= hard_stop:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="hard_stop"))
                self._clean_entry(code)
                continue
            if bar.close <= trail_stop:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trail_stop"))
                self._clean_entry(code)
                continue
            if trend_broken:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="ma10_break"))
                self._clean_entry(code)
                continue
            if time_stop:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="time_stop"))
                self._clean_entry(code)
                continue

            if code not in desired_set and profit_pct > 0.02:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="rotate"))
                self._clean_entry(code)
                continue

        # Entry logic with inverse-volatility weights
        weights: list[tuple[str, float]] = []
        if desired:
            for code, _score, atr in desired:
                bar = bars.get(code)
                if bar is None or bar.close <= 0:
                    weights.append((code, 0.0))
                    continue
                atr_pct = atr / bar.close
                weight = 1.0 / max(atr_pct, 0.005)
                weights.append((code, weight))
            total_weight = sum(w for _, w in weights)
        else:
            total_weight = 0.0

        for code, weight in weights:
            if total_weight <= 0:
                break
            bar = bars.get(code)
            if bar is None:
                continue
            target_value = target_equity * (weight / total_weight)
            target_pct = target_value / portfolio.equity if portfolio.equity > 0 else 0.0
            orders.append(Order(ts_code=code, action="buy", target_pct=target_pct, reason="entry"))
            if code not in self._entry_info:
                self._entry_info[code] = {"price": bar.close, "peak": bar.close, "bar_count": 0}

        orders = self._limit_daily_turnover(orders)
        return orders

    def _limit_daily_turnover(self, orders: List[Order]) -> List[Order]:
        sells = [o for o in orders if (o.action or "").lower() == "sell"]
        buys = [o for o in orders if (o.action or "").lower() == "buy"]
        return sells[: self.max_daily_sells] + buys[: self.max_daily_buys]

    def _clean_entry(self, code: str) -> None:
        self._entry_info.pop(code, None)

    def _get_series(self, code: str) -> list[Bar]:
        return list(self._history.get(code, []))

    def _get_ma(self, code: str, window: int) -> float:
        series = self._get_series(code)
        if not series:
            return 0.0
        closes = [b.close for b in series]
        if len(closes) < window:
            return closes[-1]
        return sum(closes[-window:]) / window

    def _get_rsi(self, code: str, period: int = 14) -> float:
        series = self._get_series(code)
        closes = [b.close for b in series]
        if len(closes) < period:
            return 50.0
        return _rsi(closes, period)[-1]

    def _get_return(self, code: str, window: int) -> float:
        series = self._get_series(code)
        closes = [b.close for b in series]
        if len(closes) < window + 1:
            return 0.0
        base = closes[-(window + 1)]
        if base <= 0:
            return 0.0
        return (closes[-1] / base) - 1.0

    def _calc_rs(self, stock_ret: float, market_ret: float) -> float:
        if market_ret <= -0.99:
            return 1.0
        return (1.0 + stock_ret) / max(1.0 + market_ret, 0.01)

    def _get_flow_score(self, code: str) -> float:
        series = self._get_series(code)
        if len(series) < 20:
            return 0.0
        avg_prices = []
        for bar in series:
            if bar.volume > 0:
                avg_prices.append(bar.amount / bar.volume)
            else:
                avg_prices.append(bar.close)
        avg5 = sum(avg_prices[-5:]) / 5.0
        avg20 = sum(avg_prices[-20:]) / 20.0
        if avg20 <= 0:
            return 0.0
        return (avg5 / avg20) - 1.0

    def _get_avg_volume(self, code: str, window: int) -> float:
        series = self._get_series(code)
        if len(series) < window:
            return 0.0
        vols = [b.volume for b in series[-window:]]
        return sum(vols) / window

    def _get_atr(self, code: str, window: int = 14) -> float:
        series = self._get_series(code)
        if len(series) < window + 1:
            return 0.0
        trs: List[float] = []
        for i in range(1, len(series)):
            prev_close = series[i - 1].close
            high = series[i].high
            low = series[i].low
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if len(trs) < window:
            return 0.0
        recent = trs[-window:]
        return sum(recent) / len(recent)

    def _calc_market_strength(self, bars: Dict[str, Bar]) -> float:
        total = 0
        above = 0
        for code, bar in bars.items():
            ma20 = self._get_ma(code, 20)
            if ma20 <= 0:
                continue
            total += 1
            if bar.close > ma20:
                above += 1
        if total == 0:
            return 0.5
        return above / total

    def _calc_market_ret20(self) -> float:
        returns = []
        for code in self._history:
            ret = self._get_return(code, 20)
            if ret != 0.0:
                returns.append(ret)
        if not returns:
            return 0.0
        return sum(returns) / len(returns)

    def _calc_dynamic_amount_threshold(self, bars: Dict[str, Bar]) -> float:
        amounts = [b.amount for b in bars.values() if b.amount > 0]
        if not amounts:
            return self.min_amount
        amounts_sorted = sorted(amounts)
        mid = amounts_sorted[len(amounts_sorted) // 2]
        if mid < self.min_amount * 0.1:
            return max(mid * 0.5, mid)
        return self.min_amount