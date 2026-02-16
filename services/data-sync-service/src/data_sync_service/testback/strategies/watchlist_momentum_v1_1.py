from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List
import math

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class MomentumRankStrategyV1_1(BaseStrategy):
    """
    V1.1 strategy: quality momentum with ranking buffer.
    Goal: reduce churn and focus on stable momentum leaders.
    """

    name = "momentum_rank_v1_1"
    use_full_bars = True
    top_k = 50

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 60,
        stop_loss_pct: float = 0.10,
        max_positions: int = 6,
        hold_buffer: int = 4,
        momentum_window: int = 20,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))
        self.max_positions = max(1, int(max_positions))
        self.exit_rank = max(self.max_positions + max(0, int(hold_buffer)), self.max_positions)
        self.momentum_window = max(10, int(momentum_window))

        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._max_price: Dict[str, float] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    @staticmethod
    def _linear_fit_slope_r2(values: list[float]) -> tuple[float, float]:
        n = len(values)
        if n < 2:
            return 0.0, 0.0
        x_mean = (n - 1) / 2.0
        y_mean = sum(values) / float(n)
        sxy = 0.0
        sxx = 0.0
        for i, y in enumerate(values):
            dx = i - x_mean
            dy = y - y_mean
            sxy += dx * dy
            sxx += dx * dx
        if sxx == 0:
            return 0.0, 0.0
        slope = sxy / sxx
        ss_tot = sum((y - y_mean) ** 2 for y in values)
        ss_res = sum((values[i] - (slope * (i - x_mean) + y_mean)) ** 2 for i in range(n))
        r2 = 0.0 if ss_tot == 0 else max(0.0, 1.0 - ss_res / ss_tot)
        return slope, r2

    def _quality_momentum(self, closes: list[float]) -> float:
        if len(closes) < self.momentum_window:
            return 0.0
        window = closes[-self.momentum_window :]
        if any(c <= 0 for c in window):
            return 0.0
        log_prices = [math.log(c) for c in window]
        slope, r2 = self._linear_fit_slope_r2(log_prices)
        annualized = (math.exp(slope) ** 252) - 1.0
        return annualized * r2

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []

        regime = self._get_regime(trade_date)
        orders: List[Order] = []

        exposure_limit = 1.0 if regime == "Strong" else (0.6 if regime == "Diverging" else 0.3)
        target_per_stock = exposure_limit / float(self.max_positions)

        scored_list: list[tuple[str, float]] = []
        trend_ok_by_code: dict[str, bool] = {}

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            ema20 = _ema(closes, self.mid_window)[-1]
            ema60 = _ema(closes, self.slow_window)[-1]
            macd_line, _signal, _hist = _macd(closes)
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            trend_ok = bar.close > ema20 and ema20 > ema60 and macd_line[-1] > 0 and 50.0 <= rsi14 <= 85.0
            trend_ok_by_code[code] = trend_ok

            is_holding = code in portfolio.positions and portfolio.positions[code] > 0
            if is_holding:
                self._max_price[code] = max(self._max_price.get(code, 0), bar.close)
                trailing_stop = self._max_price[code] * (1.0 - self.stop_loss_pct)
                if bar.close < trailing_stop or bar.close < ema20 * 0.96:
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v1_1_stop"))
                    self._entry_price.pop(code, None)
                    self._max_price.pop(code, None)
                    continue

            if trend_ok:
                score = self._quality_momentum(closes)
                scored_list.append((code, score))

        scored_list.sort(key=lambda x: (-x[1], x[0]))
        top_codes = [x[0] for x in scored_list]

        current_holdings = [c for c, q in portfolio.positions.items() if q > 0]

        for code in current_holdings:
            if code not in top_codes[: self.exit_rank] and not trend_ok_by_code.get(code, False):
                if not any(o.ts_code == code and o.action == "sell" for o in orders):
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="rank_out"))
                    self._max_price.pop(code, None)

        pending_sells = len([o for o in orders if o.action == "sell"])
        for code in top_codes[: self.max_positions]:
            if code not in current_holdings:
                if len(current_holdings) - pending_sells < self.max_positions:
                    orders.append(Order(ts_code=code, action="buy", target_pct=target_per_stock, reason="top_rank"))
                    self._max_price[code] = bars[code].close

        return orders
