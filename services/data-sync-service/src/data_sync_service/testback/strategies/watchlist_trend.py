from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class WatchlistTrendStrategy(BaseStrategy):
    # Baseline watchlist-style strategy: score-filtered universe, breakout or pullback buys,
    # and regime-aware risk control.
    name = "watchlist_trend"

    def __init__(self, fast_window: int = 5, mid_window: int = 20, slow_window: int = 60) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        _ = portfolio
        if not bars:
            return []
        regime = self._get_regime(trade_date)
        orders: List[Order] = []
        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            volumes = [b.volume for b in history]

            ema5 = _ema(closes, self.fast_window)[-1]
            ema20 = _ema(closes, self.mid_window)[-1]
            ema60 = _ema(closes, self.slow_window)[-1]
            macd_line, _signal, hist = _macd(closes)
            macd_last = macd_line[-1] if macd_line else 0.0
            hist_last = hist[-1] if hist else 0.0
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            avg5 = sum(volumes[-5:]) / 5.0
            avg30 = sum(volumes[-30:]) / 30.0 if len(volumes) >= 30 else avg5
            high20 = max(highs[-20:])

            breakout_ok = (
                bar.close >= 0.98 * high20
                and ema20 > ema60
                and macd_last > 0.0
                and hist_last > 0.0
                and 55.0 <= rsi14 <= 82.0
                and avg5 >= 0.9 * avg30
            )
            pullback_ok = (
                ema20 > ema60
                and macd_last > 0.0
                and 45.0 <= rsi14 <= 75.0
                and (ema20 * 0.97) <= bar.close <= (ema20 * 1.03)
            )

            sell_ok = bar.close < ema20 * 0.97 or ema20 < ema60 or macd_last < 0.0

            if sell_ok:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trend weak"))
                continue

            if regime == "Strong" and (breakout_ok or pullback_ok):
                orders.append(Order(ts_code=code, action="buy", target_pct=1.0, reason="trend buy"))
            elif regime == "Diverging" and pullback_ok:
                orders.append(Order(ts_code=code, action="buy", target_pct=1.0, reason="pullback buy"))
        return orders
