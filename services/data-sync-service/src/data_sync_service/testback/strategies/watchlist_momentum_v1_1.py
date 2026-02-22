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


class WatchlistTrendV6_4Strategy(BaseStrategy):
    """
    V6.4 strategy: concentrated momentum/trend rotation with explicit stop-loss rules.

    Design goals:
    - Concentrate into top-ranked names (avoid 1%-5% tiny positions).
    - Use a hard stop-loss to cap worst-case drawdowns on single names.
    - Use a trailing stop to protect profits while allowing trends to run.
    - Apply a rebalance threshold to avoid micro rebalances that are hard to replicate.
    """

    name = "watchlist_trend_v6_4"
    use_full_bars = True
    top_k = 50

    @classmethod
    def default_score_config(cls) -> ScoreConfig:
        return ScoreConfig()

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 30,
        hard_stop_loss_pct: float = 0.10,
        trailing_stop_pct: float = 0.10,
        rebalance_threshold_pct: float = 0.05,
        max_positions_strong: int = 5,
        max_positions_diverging: int = 4,
        max_positions_weak: int = 2,
        invested_ratio_strong: float = 0.90,
        invested_ratio_diverging: float = 0.70,
        invested_ratio_weak: float = 0.25,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))

        self.hard_stop_loss_pct = min(max(float(hard_stop_loss_pct), 0.01), 0.50)
        self.trailing_stop_pct = min(max(float(trailing_stop_pct), 0.02), 0.50)
        self.rebalance_threshold_pct = min(max(float(rebalance_threshold_pct), 0.0), 0.30)

        self.max_positions_strong = max(1, int(max_positions_strong))
        self.max_positions_diverging = max(1, int(max_positions_diverging))
        self.max_positions_weak = max(1, int(max_positions_weak))

        self.invested_ratio_strong = min(max(float(invested_ratio_strong), 0.0), 1.0)
        self.invested_ratio_diverging = min(max(float(invested_ratio_diverging), 0.0), 1.0)
        self.invested_ratio_weak = min(max(float(invested_ratio_weak), 0.0), 1.0)

        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=260))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._peak_price_since_entry: Dict[str, float] = {}
        self._last_stats: Dict[str, int | str | float] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _regime_params(self, regime: str) -> tuple[int, float]:
        if regime == "Strong":
            return self.max_positions_strong, self.invested_ratio_strong
        if regime == "Diverging":
            return self.max_positions_diverging, self.invested_ratio_diverging
        return self.max_positions_weak, self.invested_ratio_weak

    def _score(self, closes: list[float], vols: list[float]) -> float:
        """
        A simple, stable ranking score:
        - favor medium-term momentum (20d) and short-term continuation (10d/5d),
        - lightly prefer volume expansion vs 20d average.
        """
        if not closes:
            return 0.0
        c0 = closes[-1]
        ret5 = (c0 / closes[-6] - 1.0) if len(closes) >= 6 and closes[-6] > 0 else 0.0
        ret10 = (c0 / closes[-11] - 1.0) if len(closes) >= 11 and closes[-11] > 0 else 0.0
        ret20 = (c0 / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] > 0 else 0.0
        vol_boost = 0.0
        if len(vols) >= 20:
            avg_vol20 = sum(vols[-20:]) / 20.0
            if avg_vol20 > 0:
                vol_boost = (vols[-1] / avg_vol20) - 1.0
        return 0.55 * ret20 + 0.30 * ret10 + 0.15 * ret5 + 0.05 * vol_boost

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []

        regime = self._get_regime(trade_date)
        max_positions, invested_ratio = self._regime_params(regime)
        target_per_name = (invested_ratio / max_positions) if max_positions > 0 else 0.0

        orders: List[Order] = []
        candidates: list[tuple[str, float]] = []

        breakout_count = 0
        hold_ok_count = 0
        stop_count = 0

        # 1) Per-name signal evaluation + stop-loss enforcement
        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            vols = [b.volume for b in history]

            ema_fast = _ema(closes, self.fast_window)[-1]
            ema20 = _ema(closes, self.mid_window)[-1]
            ema30 = _ema(closes, self.slow_window)[-1]
            macd_line, _signal, macd_hist = _macd(closes)
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
            avg_vol20 = sum(vols[-20:]) / 20.0 if len(vols) >= 20 else 0.0
            vol_ok = avg_vol20 > 0 and vols[-1] > avg_vol20 * 1.2

            over_extended = bar.close > ema20 * 1.18
            trend_up = ema20 > ema30 and bar.close >= ema20 * 0.99
            macd_ok = (macd_hist[-1] > 0) if macd_hist else False

            breakout_ok = (
                bar.close >= 0.99 * high20
                and trend_up
                and macd_ok
                and 55.0 <= rsi14 <= 82.0
                and vol_ok
                and not over_extended
            )

            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar.close / portfolio.equity) if portfolio.equity > 0 else 0.0

            # Initialize entry tracking for pre-existing holdings (e.g. after warmup).
            if current_qty > 0 and code not in self._entry_price:
                self._entry_price[code] = bar.close
                self._peak_price_since_entry[code] = bar.close

            if code in self._entry_price:
                entry = self._entry_price[code]
                peak = max(self._peak_price_since_entry.get(code, bar.close), bar.close)
                self._peak_price_since_entry[code] = peak

                hard_stop = entry * (1.0 - self.hard_stop_loss_pct)
                trailing_stop = peak * (1.0 - self.trailing_stop_pct)
                stop_price = max(hard_stop, trailing_stop)

                trend_exit = bar.close < ema20 * 0.97 or (macd_line[-1] < 0 if macd_line else False)
                stop_hit = bar.close <= stop_price

                if current_qty > 0 and (stop_hit or trend_exit):
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_4_stop"))
                    self._entry_price.pop(code, None)
                    self._peak_price_since_entry.pop(code, None)
                    stop_count += 1
                    continue

            hold_ok = current_qty > 0 and trend_up and (bar.close >= ema_fast * 0.995)
            if hold_ok:
                hold_ok_count += 1

            if breakout_ok:
                breakout_count += 1

            if breakout_ok or hold_ok:
                candidates.append((code, self._score(closes, vols)))

        if not candidates:
            self._last_stats = {
                "date": trade_date,
                "regime": regime,
                "bars": len(bars),
                "candidates": 0,
                "breakout_ok": breakout_count,
                "hold_ok": hold_ok_count,
                "stops": stop_count,
                "target_per_name": target_per_name,
            }
            return orders

        # 2) Rank and pick top-N desired holdings for concentration.
        candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)
        desired = [code for code, _ in candidates[:max_positions]]
        desired_set = set(desired)

        # 3) Rotate out names that are not in desired set.
        for code, qty in portfolio.positions.items():
            if qty > 0 and code not in desired_set:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_4_rotate"))
                self._entry_price.pop(code, None)
                self._peak_price_since_entry.pop(code, None)

        # 4) Rebalance desired names to equal weights (with threshold to avoid micro adjustments).
        for code in desired:
            bar = bars.get(code)
            if not bar:
                continue
            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar.close / portfolio.equity) if portfolio.equity > 0 else 0.0
            delta = target_per_name - current_pct
            if abs(delta) < self.rebalance_threshold_pct:
                continue
            action = "buy" if delta > 0 else "sell"
            orders.append(Order(ts_code=code, action=action, target_pct=target_per_name, reason="v6_4_rebalance"))
            if action == "buy" and code not in self._entry_price:
                self._entry_price[code] = bar.close
                self._peak_price_since_entry[code] = bar.close

        self._last_stats = {
            "date": trade_date,
            "regime": regime,
            "bars": len(bars),
            "candidates": len(candidates),
            "breakout_ok": breakout_count,
            "hold_ok": hold_ok_count,
            "stops": stop_count,
            "max_positions": max_positions,
            "invested_ratio": invested_ratio,
            "target_per_name": target_per_name,
            "orders": len(orders),
        }

        return orders


class WatchlistTrendV6_5Strategy(BaseStrategy):
    """
    V6.5 strategy: regime-aware, low-churn momentum rotation with strict risk controls.

    Design goals:
    - Keep a small, concentrated basket.
    - Avoid micro rebalances with a configurable threshold.
    - Enforce hard + trailing stops to cap single-name losses.
    - Reduce churn via minimum holding bars and weak-regime buy suppression.
    """

    name = "watchlist_trend_v6_5"
    use_full_bars = True
    top_k = 50

    @classmethod
    def default_score_config(cls) -> ScoreConfig:
        return ScoreConfig()

    def __init__(
        self,
        fast_window: int = 5,
        mid_window: int = 20,
        slow_window: int = 50,
        hard_stop_loss_pct: float = 0.10,
        trailing_stop_pct: float = 0.12,
        rebalance_threshold_pct: float = 0.08,
        min_hold_bars: int = 5,
        max_positions_strong: int = 4,
        max_positions_diverging: int = 3,
        max_positions_weak: int = 1,
        invested_ratio_strong: float = 0.85,
        invested_ratio_diverging: float = 0.60,
        invested_ratio_weak: float = 0.20,
        min_score_strong: float = 0.015,
        min_score_diverging: float = 0.008,
        min_score_weak: float = 0.0,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 5, int(slow_window))

        self.hard_stop_loss_pct = min(max(float(hard_stop_loss_pct), 0.01), 0.50)
        self.trailing_stop_pct = min(max(float(trailing_stop_pct), 0.02), 0.50)
        self.rebalance_threshold_pct = min(max(float(rebalance_threshold_pct), 0.0), 0.30)
        self.min_hold_bars = max(0, int(min_hold_bars))

        self.max_positions_strong = max(1, int(max_positions_strong))
        self.max_positions_diverging = max(1, int(max_positions_diverging))
        self.max_positions_weak = max(1, int(max_positions_weak))

        self.invested_ratio_strong = min(max(float(invested_ratio_strong), 0.0), 1.0)
        self.invested_ratio_diverging = min(max(float(invested_ratio_diverging), 0.0), 1.0)
        self.invested_ratio_weak = min(max(float(invested_ratio_weak), 0.0), 1.0)

        self.min_score_strong = float(min_score_strong)
        self.min_score_diverging = float(min_score_diverging)
        self.min_score_weak = float(min_score_weak)

        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=300))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._peak_price_since_entry: Dict[str, float] = {}
        self._entry_index: Dict[str, int] = {}
        self._bar_index: Dict[str, int] = {}
        self._last_stats: Dict[str, int | str | float] = {}

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _regime_params(self, regime: str) -> tuple[int, float, float, bool]:
        if regime == "Strong":
            return self.max_positions_strong, self.invested_ratio_strong, self.min_score_strong, True
        if regime == "Diverging":
            return self.max_positions_diverging, self.invested_ratio_diverging, self.min_score_diverging, True
        return self.max_positions_weak, self.invested_ratio_weak, self.min_score_weak, False

    def _score(self, closes: list[float], vols: list[float]) -> float:
        if not closes:
            return 0.0
        c0 = closes[-1]
        ret5 = (c0 / closes[-6] - 1.0) if len(closes) >= 6 and closes[-6] > 0 else 0.0
        ret10 = (c0 / closes[-11] - 1.0) if len(closes) >= 11 and closes[-11] > 0 else 0.0
        ret20 = (c0 / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] > 0 else 0.0
        vol_boost = 0.0
        if len(vols) >= 20:
            avg_vol20 = sum(vols[-20:]) / 20.0
            if avg_vol20 > 0:
                vol_boost = (vols[-1] / avg_vol20) - 1.0
        return 0.60 * ret20 + 0.25 * ret10 + 0.15 * ret5 + 0.05 * vol_boost

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []

        regime = self._get_regime(trade_date)
        max_positions, invested_ratio, min_score, allow_new_buys = self._regime_params(regime)
        target_per_name = (invested_ratio / max_positions) if max_positions > 0 else 0.0

        orders: List[Order] = []
        candidates: list[tuple[str, float]] = []

        breakout_count = 0
        hold_ok_count = 0
        stop_count = 0

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < self.slow_window:
                continue

            self._bar_index[code] = self._bar_index.get(code, 0) + 1
            bar_index = self._bar_index[code]

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            vols = [b.volume for b in history]

            ema_fast = _ema(closes, self.fast_window)[-1]
            ema20_series = _ema(closes, self.mid_window)
            ema50_series = _ema(closes, self.slow_window)
            ema20 = ema20_series[-1]
            ema50 = ema50_series[-1]
            ema20_up = len(ema20_series) > 1 and ema20_series[-1] >= ema20_series[-2]
            macd_line, _signal, macd_hist = _macd(closes)
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0

            high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
            avg_vol20 = sum(vols[-20:]) / 20.0 if len(vols) >= 20 else 0.0
            vol_ok = avg_vol20 > 0 and vols[-1] > avg_vol20 * 1.2

            over_extended = bar.close > ema20 * 1.18
            trend_up = ema20 > ema50 and ema20_up and bar.close >= ema20 * 0.99
            macd_ok = (macd_hist[-1] > 0) if macd_hist else False

            breakout_ok = (
                bar.close >= 0.99 * high20
                and trend_up
                and macd_ok
                and 55.0 <= rsi14 <= 80.0
                and vol_ok
                and not over_extended
            )

            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar.close / portfolio.equity) if portfolio.equity > 0 else 0.0

            if current_qty > 0 and code not in self._entry_price:
                self._entry_price[code] = bar.close
                self._peak_price_since_entry[code] = bar.close
                self._entry_index[code] = bar_index

            if code in self._entry_price:
                entry = self._entry_price[code]
                peak = max(self._peak_price_since_entry.get(code, bar.close), bar.close)
                self._peak_price_since_entry[code] = peak

                hard_stop = entry * (1.0 - self.hard_stop_loss_pct)
                trailing_stop = peak * (1.0 - self.trailing_stop_pct)
                stop_price = max(hard_stop, trailing_stop)

                trend_exit = bar.close < ema20 * 0.97 or (macd_line[-1] < 0 if macd_line else False)
                stop_hit = bar.close <= stop_price

                if current_qty > 0 and (stop_hit or trend_exit):
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_5_stop"))
                    self._entry_price.pop(code, None)
                    self._peak_price_since_entry.pop(code, None)
                    self._entry_index.pop(code, None)
                    stop_count += 1
                    continue

            hold_ok = current_qty > 0 and trend_up and (bar.close >= ema_fast * 0.995)
            if hold_ok:
                hold_ok_count += 1
            if breakout_ok:
                breakout_count += 1

            if breakout_ok or hold_ok:
                score = self._score(closes, vols)
                if score >= min_score and (allow_new_buys or current_qty > 0):
                    candidates.append((code, score))

        if not candidates:
            self._last_stats = {
                "date": trade_date,
                "regime": regime,
                "bars": len(bars),
                "candidates": 0,
                "breakout_ok": breakout_count,
                "hold_ok": hold_ok_count,
                "stops": stop_count,
                "target_per_name": target_per_name,
            }
            return orders

        candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)
        desired = [code for code, _ in candidates[:max_positions]]
        desired_set = set(desired)

        for code, qty in portfolio.positions.items():
            if qty <= 0 or code in desired_set:
                continue
            entry_idx = self._entry_index.get(code, 0)
            holding_bars = max(0, self._bar_index.get(code, entry_idx) - entry_idx)
            if holding_bars < self.min_hold_bars:
                continue
            orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_5_rotate"))
            self._entry_price.pop(code, None)
            self._peak_price_since_entry.pop(code, None)
            self._entry_index.pop(code, None)

        for code in desired:
            bar = bars.get(code)
            if not bar:
                continue
            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar.close / portfolio.equity) if portfolio.equity > 0 else 0.0
            delta = target_per_name - current_pct
            if abs(delta) < self.rebalance_threshold_pct:
                continue
            if delta > 0 and not allow_new_buys:
                continue
            action = "buy" if delta > 0 else "sell"
            orders.append(Order(ts_code=code, action=action, target_pct=target_per_name, reason="v6_5_rebalance"))
            if action == "buy" and code not in self._entry_price:
                self._entry_price[code] = bar.close
                self._peak_price_since_entry[code] = bar.close
                self._entry_index[code] = self._bar_index.get(code, 0)

        self._last_stats = {
            "date": trade_date,
            "regime": regime,
            "bars": len(bars),
            "candidates": len(candidates),
            "breakout_ok": breakout_count,
            "hold_ok": hold_ok_count,
            "stops": stop_count,
            "max_positions": max_positions,
            "invested_ratio": invested_ratio,
            "target_per_name": target_per_name,
            "orders": len(orders),
        }

        return orders
