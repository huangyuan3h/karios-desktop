from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class WatchlistTrendV5Strategy(BaseStrategy):
    # V5 strategy: 3-tranche allocation with stop-loss and strength filtering.
    name = "watchlist_trend_v5"
    use_full_bars = True

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 30,
        stop_loss_pct: float = 0.08,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._last_stats: Dict[str, int | str] = {}

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
        breakout_count = 0
        sell_count = 0
        buy_count = 0

        base_target = 0.0
        if regime == "Strong":
            base_target = 1.0
        elif regime == "Diverging":
            base_target = 0.66
        elif regime == "Weak":
            base_target = 0.33

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]

            ema20_series = _ema(closes, self.mid_window)
            ema30_series = _ema(closes, self.slow_window)
            ema20 = ema20_series[-1]
            ema30 = ema30_series[-1]
            ema20_up = len(ema20_series) > 1 and ema20_series[-1] >= ema20_series[-2]
            ema30_up = len(ema30_series) > 1 and ema30_series[-1] >= ema30_series[-2]
            macd_line, _signal, hist = _macd(closes)
            macd_last = macd_line[-1] if macd_line else 0.0
            hist_last = hist[-1] if hist else 0.0
            hist_prev = hist[-2] if len(hist) > 1 else hist_last
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            high20 = max(highs[-20:])
            breakout_ok = (
                bar.close >= 0.98 * high20
                and ema20 > ema30
                and ema20_up
                and ema30_up
                and macd_last > 0.0
                and hist_last > 0.0
                and hist_last >= hist_prev
                and 58.0 <= rsi14 <= 85.0
            )

            entry_price = self._entry_price.get(code)
            stop_price = entry_price * (1.0 - self.stop_loss_pct) if entry_price else 0.0
            stop_ok = entry_price is not None and bar.close <= stop_price

            sell_ok = bar.close < ema20 * 0.97 or ema20 < ema30 or macd_last < 0.0 or stop_ok
            if breakout_ok:
                breakout_count += 1
            if sell_ok:
                sell_count += 1

            if sell_ok:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trend weak/stop"))
                self._entry_price.pop(code, None)
                continue

            if breakout_ok:
                orders.append(Order(ts_code=code, action="buy", target_pct=base_target, reason="breakout tranche"))
                self._entry_price[code] = bar.close
                buy_count += 1
            else:
                if base_target < 0.66:
                    orders.append(Order(ts_code=code, action="sell", target_pct=base_target, reason="trim weak"))

        self._last_stats = {
            "date": trade_date,
            "regime": regime,
            "bars": len(bars),
            "breakout_ok": breakout_count,
            "pullback_ok": 0,
            "sell_ok": sell_count,
            "buy_signal": buy_count,
        }
        return orders
