from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot


class WatchlistTrendV5_1Strategy(BaseStrategy):
    # V5.1 strategy: V5 core with portfolio cap + ranked allocation.
    name = "watchlist_trend_v5_1"
    use_full_bars = True
    top_k = 50

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 30,
        stop_loss_pct: float = 0.12,
        max_positions: int = 4,
        rank_weights: list[float] | None = None,
        volume_weight: float = 0.2,
        volatility_weight: float = 0.2,
        volume_window: int = 20,
        volatility_window: int = 20,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))
        self.max_positions = max(1, int(max_positions))
        self.rank_weights = [float(w) for w in (rank_weights or [0.4, 0.3, 0.2, 0.1]) if w > 0]
        self.volume_weight = max(0.0, float(volume_weight))
        self.volatility_weight = max(0.0, float(volatility_weight))
        self.volume_window = max(5, int(volume_window))
        self.volatility_window = max(5, int(volatility_window))
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._last_stats: Dict[str, int | str] = {}

        # Fixed portfolio weights for top holdings; normalized per active count.
        self._default_rank_weights = [0.4, 0.3, 0.2, 0.1]

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _next_tranche(self, current_pct: float, target_pct: float) -> float:
        if target_pct <= 0:
            return 0.0
        step = target_pct / 3.0
        if current_pct < step:
            return step
        if current_pct < 2 * step:
            return 2 * step
        return target_pct

    @staticmethod
    def _clip01(val: float) -> float:
        if val <= 0:
            return 0.0
        if val >= 1:
            return 1.0
        return val

    def _calc_volatility(self, closes: list[float]) -> float:
        if len(closes) < self.volatility_window + 1:
            return 0.0
        rets: list[float] = []
        for i in range(-self.volatility_window, 0):
            c0 = closes[i - 1]
            c1 = closes[i]
            if c0 > 0:
                rets.append((c1 / c0) - 1.0)
        if len(rets) < 3:
            return 0.0
        mean = sum(rets) / float(len(rets))
        var = sum((r - mean) ** 2 for r in rets) / float(len(rets))
        return var ** 0.5

    def _score_strength(
        self,
        close: float,
        high20: float,
        ema20: float,
        ema30: float,
        macd_last: float,
        hist_last: float,
        rsi14: float,
        vol_ratio: float,
        vol_std: float,
    ) -> float:
        if high20 <= 0 or ema30 <= 0:
            return 0.0
        breakout_ratio = close / high20
        trend_gap = (ema20 - ema30) / ema30
        macd_score = min(max(macd_last, 0.0), 0.5) / 0.5
        hist_score = min(max(hist_last, 0.0), 0.5) / 0.5
        rsi_score = min(max((rsi14 - 50.0) / 35.0, 0.0), 1.0)
        strength = breakout_ratio + min(max(trend_gap, 0.0), 0.05) / 0.05 + macd_score + hist_score + rsi_score
        volume_score = self._clip01((vol_ratio - 1.0) / 0.5)
        volatility_score = self._clip01(1.0 - (vol_std / 0.06))
        return strength + self.volume_weight * volume_score + self.volatility_weight * volatility_score

    def _rank_targets(self, codes: list[str], base_target: float) -> dict[str, float]:
        if not codes or base_target <= 0:
            return {}
        weights = (self.rank_weights or self._default_rank_weights)[: len(codes)]
        total = sum(weights) if weights else 0.0
        if total <= 0:
            total = float(len(codes))
            weights = [1.0] * len(codes)
        scale = base_target / total
        return {code: weight * scale for code, weight in zip(codes, weights, strict=False)}

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
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
            base_target = 1.0

        stats_by_code: dict[str, dict[str, float | bool]] = {}
        scored_breakouts: list[tuple[str, float]] = []

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            vols = [b.volume for b in history]

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
            avg_vol_short = sum(vols[-5:]) / 5.0 if len(vols) >= 5 else 0.0
            avg_vol_long = (
                sum(vols[-self.volume_window :]) / float(self.volume_window)
                if len(vols) >= self.volume_window
                else avg_vol_short
            )
            vol_ratio = (avg_vol_short / avg_vol_long) if avg_vol_long > 0 else 1.0
            vol_std = self._calc_volatility(closes)
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
                score = self._score_strength(
                    bar.close,
                    high20,
                    ema20,
                    ema30,
                    macd_last,
                    hist_last,
                    rsi14,
                    vol_ratio,
                    vol_std,
                )
                scored_breakouts.append((code, score))
            if sell_ok:
                sell_count += 1

            stats_by_code[code] = {
                "breakout_ok": breakout_ok,
                "sell_ok": sell_ok,
            }

        scored_breakouts.sort(key=lambda x: (-x[1], x[0]))
        selected_codes = [code for code, _score in scored_breakouts[: self.max_positions]]
        target_by_code = self._rank_targets(selected_codes, base_target)

        for code, bar in bars.items():
            if code not in stats_by_code:
                continue
            sell_ok = bool(stats_by_code[code]["sell_ok"])
            breakout_ok = bool(stats_by_code[code]["breakout_ok"])

            if sell_ok:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="trend weak/stop"))
                self._entry_price.pop(code, None)
                continue

            current_qty = portfolio.positions.get(code, 0.0)
            current_value = current_qty * bar.close
            current_pct = current_value / portfolio.equity if portfolio.equity > 0 else 0.0

            if code in target_by_code:
                target = target_by_code[code]
                if breakout_ok:
                    next_target = self._next_tranche(current_pct, target)
                    if next_target > current_pct:
                        orders.append(
                            Order(ts_code=code, action="buy", target_pct=next_target, reason="breakout tranche")
                        )
                        self._entry_price[code] = bar.close
                        buy_count += 1
                else:
                    if current_pct > target:
                        orders.append(
                            Order(ts_code=code, action="sell", target_pct=target, reason="trim to rank target")
                        )
            else:
                if current_pct > 0:
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="rank out"))

        self._last_stats = {
            "date": trade_date,
            "regime": regime,
            "bars": len(bars),
            "breakout_ok": breakout_count,
            "pullback_ok": 0,
            "sell_ok": sell_count,
            "buy_signal": buy_count,
            "selected": len(selected_codes),
        }
        return orders
