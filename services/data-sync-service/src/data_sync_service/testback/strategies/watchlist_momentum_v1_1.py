from __future__ import annotations
from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot, ScoreConfig


class WatchlistTrendV6_3Strategy(BaseStrategy):
    """
    V6.3 strategy: alpha capture with concentration and relative strength ranking.
    Goal: return to a 130%+ performance range.
    """

    name = "watchlist_trend_v6_3"
    use_full_bars = True
    top_k = 50

    @classmethod
    def default_score_config(cls) -> ScoreConfig:
        return ScoreConfig()

    def __init__(
        self,
        mid_window: int = 20,
        slow_window: int = 30,
        stop_loss_pct: float = 0.12,
    ) -> None:
        self.mid_window = max(2, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))

        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._max_price_since_entry: Dict[str, float] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []
        regime = self._get_regime(trade_date)
        orders: List[Order] = []

        if regime == "Strong":
            base_target = 0.33
        elif regime == "Diverging":
            base_target = 0.20
        else:
            base_target = 0.05

        potential_buys: list[tuple[str, float]] = []

        for code, bar in bars.items():
            self._history[code].append(bar)
            history = self._history[code]
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            vols = [b.volume for b in history]

            ema20 = _ema(closes, self.mid_window)[-1]
            ema30 = _ema(closes, self.slow_window)[-1]
            macd_line, _, macd_hist = _macd(closes)
            high20 = max(highs[-20:])

            rs_score = (bar.close / closes[-10]) - 1 if len(closes) >= 10 else 0.0
            avg_vol20 = sum(vols[-20:]) / 20.0 if len(vols) >= 20 else 0.0
            vol_ok = avg_vol20 > 0 and vols[-1] > avg_vol20 * 1.2

            breakout_ok = (
                bar.close >= 0.99 * high20
                and ema20 > ema30
                and macd_hist[-1] > 0
                and vol_ok
            )

            if code in self._entry_price:
                self._max_price_since_entry[code] = max(self._max_price_since_entry.get(code, 0), bar.close)
                profit_pct = (bar.close - self._entry_price[code]) / self._entry_price[code]
                current_stop = 0.08 if profit_pct > 0.30 else self.stop_loss_pct
                trailing_stop = self._max_price_since_entry[code] * (1 - current_stop)

                if bar.close <= trailing_stop or (bar.close < ema20 * 0.97 and macd_line[-1] < 0):
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_3_exit"))
                    self._entry_price.pop(code, None)
                    self._max_price_since_entry.pop(code, None)
                    continue

            if breakout_ok:
                potential_buys.append((code, rs_score))

        potential_buys.sort(key=lambda x: x[1], reverse=True)
        current_holdings = [c for c, q in portfolio.positions.items() if q > 0]

        for code, _score in potential_buys:
            if code not in current_holdings:
                current_qty = portfolio.positions.get(code, 0.0)
                current_pct = (current_qty * bars[code].close / portfolio.equity) if portfolio.equity > 0 else 0.0
                target = base_target if current_pct > 0 else base_target * 0.5
                if target > current_pct:
                    orders.append(Order(ts_code=code, action="buy", target_pct=target, reason="v6_3_alpha"))
                    if code not in self._entry_price:
                        self._entry_price[code] = bars[code].close
                        self._max_price_since_entry[code] = bars[code].close

        return orders
