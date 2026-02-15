from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class WatchlistTrendV6Strategy(BaseStrategy):
    # V6 strategy: regime-aware tranches with volatility-adjusted stops and pullback adds.
    name = "watchlist_trend_v6"
    use_full_bars = True
    top_k = 50

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 30,
        atr_window: int = 14,
        stop_loss_pct: float = 0.12,
        atr_stop_mult: float = 2.5,
        trailing_atr_mult: float = 3.0,
        max_extension_pct: float = 0.18,
        cooldown_bars: int = 5,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.atr_window = max(5, int(atr_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))
        self.atr_stop_mult = max(0.5, float(atr_stop_mult))
        self.trailing_atr_mult = max(0.5, float(trailing_atr_mult))
        self.max_extension_pct = max(0.05, float(max_extension_pct))
        self.cooldown_bars = max(0, int(cooldown_bars))
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=260))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._entry_atr: Dict[str, float] = {}
        self._peak_price: Dict[str, float] = {}
        self._cooldown: Dict[str, int] = {}
        self._last_stats: Dict[str, int | str] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _next_tranche(self, current_pct: float, base_target: float) -> float:
        if base_target <= 0:
            return 0.0
        step = base_target / 3.0
        if current_pct < step:
            return step
        if current_pct < 2 * step:
            return 2 * step
        return base_target

    def _calc_atr(self, history: Deque[Bar]) -> float:
        if len(history) < self.atr_window + 1:
            return 0.0
        bars = list(history)
        trs: List[float] = []
        for i in range(1, len(bars)):
            prev_close = bars[i - 1].close
            high = bars[i].high
            low = bars[i].low
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if len(trs) < self.atr_window:
            return 0.0
        window = trs[-self.atr_window :]
        return sum(window) / len(window)

    def _stop_price(
        self,
        entry_price: float | None,
        entry_atr: float | None,
        peak_price: float | None,
        atr_now: float,
    ) -> float:
        if not entry_price:
            return 0.0
        pct_stop = entry_price * (1.0 - self.stop_loss_pct)
        atr_stop = entry_price - (entry_atr or atr_now) * self.atr_stop_mult if (entry_atr or atr_now) else 0.0
        trail_stop = peak_price - atr_now * self.trailing_atr_mult if peak_price and atr_now else 0.0
        return max(pct_stop, atr_stop, trail_stop)

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []
        regime = self._get_regime(trade_date)
        orders: List[Order] = []
        breakout_count = 0
        pullback_count = 0
        sell_count = 0
        buy_count = 0

        base_target = 0.0
        if regime == "Strong":
            base_target = 1.0
        elif regime == "Diverging":
            base_target = 0.66
        elif regime == "Weak":
            base_target = 0.3

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]

            ema_fast_series = _ema(closes, self.fast_window)
            ema20_series = _ema(closes, self.mid_window)
            ema30_series = _ema(closes, self.slow_window)
            ema_fast = ema_fast_series[-1]
            ema20 = ema20_series[-1]
            ema30 = ema30_series[-1]
            ema20_up = len(ema20_series) > 1 and ema20_series[-1] >= ema20_series[-2]
            ema30_up = len(ema30_series) > 1 and ema30_series[-1] >= ema30_series[-2]
            macd_line, _signal, hist = _macd(closes)
            macd_last = macd_line[-1] if macd_line else 0.0
            hist_last = hist[-1] if hist else 0.0
            hist_prev = hist[-2] if len(hist) > 1 else hist_last
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0
            atr_now = self._calc_atr(history)
            atr_pct = atr_now / bar.close if bar.close > 0 and atr_now > 0 else 0.0

            high20 = max(highs[-20:])
            over_extended = bar.close > ema20 * (1.0 + self.max_extension_pct)
            trend_up = ema20 > ema30 and ema20_up and ema30_up
            breakout_ok = (
                bar.close >= 0.98 * high20
                and trend_up
                and macd_last > 0.0
                and hist_last > 0.0
                and hist_last >= hist_prev
                and 58.0 <= rsi14 <= 85.0
                and not over_extended
            )
            pullback_ok = (
                trend_up
                and ema_fast >= ema20
                and 0.98 * ema20 <= bar.close <= 1.03 * ema20
                and macd_last >= 0.0
                and 45.0 <= rsi14 <= 65.0
                and not over_extended
            )

            entry_price = self._entry_price.get(code)
            entry_atr = self._entry_atr.get(code)
            peak_price = self._peak_price.get(code)
            current_qty = portfolio.positions.get(code, 0.0)
            if current_qty > 0:
                peak_price = max(peak_price or bar.close, bar.close)
                self._peak_price[code] = peak_price
            stop_price = self._stop_price(entry_price, entry_atr, peak_price, atr_now)
            stop_ok = entry_price is not None and stop_price > 0.0 and bar.close <= stop_price

            sell_ok = bar.close < ema20 * 0.97 or ema20 < ema30 or macd_last < 0.0 or stop_ok
            if breakout_ok:
                breakout_count += 1
            if pullback_ok:
                pullback_count += 1
            if sell_ok:
                sell_count += 1

            if sell_ok:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trend weak/stop"))
                self._entry_price.pop(code, None)
                self._entry_atr.pop(code, None)
                self._peak_price.pop(code, None)
                if self.cooldown_bars > 0:
                    self._cooldown[code] = self.cooldown_bars
                continue

            current_value = current_qty * bar.close
            current_pct = current_value / portfolio.equity if portfolio.equity > 0 else 0.0
            cooldown_left = self._cooldown.get(code, 0)
            if cooldown_left > 0:
                self._cooldown[code] = cooldown_left - 1

            if atr_pct >= 0.08:
                adj_target = base_target * 0.6
            elif atr_pct >= 0.06:
                adj_target = base_target * 0.8
            else:
                adj_target = base_target

            if (breakout_ok or pullback_ok) and cooldown_left <= 0:
                target = self._next_tranche(current_pct, adj_target)
                if target > current_pct:
                    reason = "breakout tranche" if breakout_ok else "pullback tranche"
                    orders.append(Order(ts_code=code, action="buy", target_pct=target, reason=reason))
                    self._entry_price[code] = bar.close
                    if atr_now > 0:
                        self._entry_atr[code] = atr_now
                    self._peak_price[code] = max(self._peak_price.get(code, 0.0), bar.close)
                    buy_count += 1
            else:
                if adj_target < 0.66 and current_pct > adj_target:
                    orders.append(Order(ts_code=code, action="sell", target_pct=adj_target, reason="trim weak"))

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
