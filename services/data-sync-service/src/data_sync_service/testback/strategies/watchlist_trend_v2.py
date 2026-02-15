from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class WatchlistTrendV2Strategy(BaseStrategy):
    # V2 strategy: stricter entries, cooldown, and regime-aware risk control.
    name = "watchlist_trend_v2"
    use_full_bars = True

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 15,
        slow_window: int = 30,
        cooldown_days: int = 5,
        min_hold_days: int = 3,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.cooldown_days = max(0, int(cooldown_days))
        self.min_hold_days = max(0, int(min_hold_days))
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._last_action_date: Dict[str, str] = {}
        self._last_buy_date: Dict[str, str] = {}
        self._last_stats: Dict[str, int | str] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _days_between(self, a: str, b: str) -> int:
        try:
            da = datetime.strptime(a, "%Y-%m-%d")
            db = datetime.strptime(b, "%Y-%m-%d")
            return abs((da - db).days)
        except Exception:
            return 0

    def _in_cooldown(self, ts_code: str, trade_date: str) -> bool:
        last = self._last_action_date.get(ts_code)
        if not last:
            return False
        return self._days_between(last, trade_date) < self.cooldown_days

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        _ = portfolio
        if not bars:
            return []
        regime = self._get_regime(trade_date)
        orders: List[Order] = []
        breakout_count = 0
        pullback_count = 0
        sell_count = 0
        buy_count = 0
        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]

            ema5 = _ema(closes, self.fast_window)[-1]
            ema20 = _ema(closes, self.mid_window)[-1]
            ema60 = _ema(closes, self.slow_window)[-1]
            macd_line, _signal, hist = _macd(closes)
            macd_last = macd_line[-1] if macd_line else 0.0
            hist_last = hist[-1] if hist else 0.0
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            high20 = max(highs[-20:])

            breakout_ok = (
                bar.close >= 0.98 * high20
                and ema20 > ema60
                and ema5 >= ema20
                and macd_last > 0.0
                and hist_last > 0.0
                and 60.0 <= rsi14 <= 85.0
            )
            pullback_ok = (
                ema20 > ema60
                and macd_last > 0.0
                and 50.0 <= rsi14 <= 75.0
                and (ema20 * 0.99) <= bar.close <= (ema20 * 1.01)
            )

            sell_ok = bar.close < ema20 * 0.98 or ema20 < ema60 or macd_last < 0.0
            if breakout_ok:
                breakout_count += 1
            if pullback_ok:
                pullback_count += 1
            if sell_ok:
                sell_count += 1

            if sell_ok:
                last_buy = self._last_buy_date.get(code)
                if last_buy and self._days_between(last_buy, trade_date) < self.min_hold_days:
                    continue
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trend weak"))
                self._last_action_date[code] = trade_date
                continue

            if self._in_cooldown(code, trade_date):
                continue

            if regime == "Strong" and (breakout_ok or pullback_ok):
                orders.append(Order(ts_code=code, action="buy", target_pct=1.0, reason="trend buy"))
                self._last_action_date[code] = trade_date
                self._last_buy_date[code] = trade_date
                buy_count += 1
            elif regime == "Diverging" and breakout_ok and rsi14 >= 65.0:
                orders.append(Order(ts_code=code, action="buy", target_pct=1.0, reason="breakout buy"))
                self._last_action_date[code] = trade_date
                self._last_buy_date[code] = trade_date
                buy_count += 1

        self._last_stats = {
            "date": trade_date,
            "regime": regime,
            "bars": len(bars),
            "breakout_ok": breakout_count,
            "pullback_ok": pullback_count,
            "sell_ok": sell_count,
            "buy_signal": buy_count,
        }
        return orders
