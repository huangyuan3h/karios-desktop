from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot, ScoreConfig


class WatchlistRelVolumeMomentumV2Strategy(BaseStrategy):
    """
    Rel-volume momentum strategy with pullback entry. (V2: High Conviction & Low Churn)

    Optimizations applied:
    1. Rank buffer to reduce rotate-out churn.
    2. Regime decoupling: weak regime blocks NEW buys, but does not auto-liquidate trends.
    3. Time stop: exit non-performing names after N bars to free capital.
    """

    name = "watchlist_relvol_momentum_v2"
    use_full_bars = True

    @classmethod
    def default_score_config(cls) -> ScoreConfig:
        return ScoreConfig(top_n=500, momentum_weight=1.0, volume_weight=0.40, amount_weight=0.20)

    def __init__(
        self,
        fast_window: int = 10,
        mid_window: int = 20,
        slow_window: int = 60,
        rvol_window: int = 20,
        rvol_threshold: float = 1.8,
        breakout_window: int = 20,
        pullback_window: int = 12,
        hard_stop_loss_pct: float = 0.08,
        trailing_stop_pct: float = 0.12,
        take_profit_pct: float = 0.20,
        tight_trailing_pct: float = 0.06,
        rebalance_threshold_pct: float = 0.08,
        min_hold_bars: int = 4,
        max_hold_bars_no_profit: int = 10,
        max_positions_strong: int = 4,
        max_positions_diverging: int = 2,
        max_positions_weak: int = 0,
        invested_ratio_strong: float = 0.95,
        invested_ratio_diverging: float = 0.60,
        invested_ratio_weak: float = 0.0,
        min_score_strong: float = 0.015,
        min_score_diverging: float = 0.010,
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 10, int(slow_window))
        self.rvol_window = max(5, int(rvol_window))
        self.rvol_threshold = max(1.0, float(rvol_threshold))
        self.breakout_window = max(5, int(breakout_window))
        self.pullback_window = max(2, int(pullback_window))

        self.hard_stop_loss_pct = float(hard_stop_loss_pct)
        self.trailing_stop_pct = float(trailing_stop_pct)
        self.tight_trailing_pct = float(tight_trailing_pct)
        self.take_profit_pct = float(take_profit_pct)
        self.rebalance_threshold_pct = float(rebalance_threshold_pct)

        self.min_hold_bars = max(0, int(min_hold_bars))
        self.max_hold_bars_no_profit = max(5, int(max_hold_bars_no_profit))

        self.max_positions_strong = max(0, int(max_positions_strong))
        self.max_positions_diverging = max(0, int(max_positions_diverging))
        self.max_positions_weak = max(0, int(max_positions_weak))

        self.invested_ratio_strong = float(invested_ratio_strong)
        self.invested_ratio_diverging = float(invested_ratio_diverging)
        self.invested_ratio_weak = float(invested_ratio_weak)

        self.min_score_strong = float(min_score_strong)
        self.min_score_diverging = float(min_score_diverging)

        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._peak_price_since_entry: Dict[str, float] = {}
        self._entry_index: Dict[str, int] = {}
        self._bar_index: Dict[str, int] = {}
        self._last_breakout_index: Dict[str, int] = {}

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
        return self.max_positions_weak, self.invested_ratio_weak, 0.0, False

    def _score(self, closes: list[float], rvol: float) -> float:
        if len(closes) < 61:
            return 0.0
        c0 = closes[-1]
        ret10 = (c0 / closes[-11] - 1.0) if closes[-11] > 0 else 0.0
        ret60 = (c0 / closes[-61] - 1.0) if closes[-61] > 0 else 0.0
        # Focus on 60d trend with a short-term boost; cap rvol to avoid extreme outliers.
        momentum_score = 0.70 * ret60 + 0.30 * ret10
        capped_rvol = min(max(rvol - 1.0, 0.0), 3.0)
        return momentum_score + 0.15 * capped_rvol

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars:
            return []

        regime = self._get_regime(trade_date)
        max_positions, invested_ratio, min_score, allow_new_buys = self._regime_params(regime)
        # In weak regime, keep a defensive target for existing holdings instead of hard zero.
        calc_invested = invested_ratio if allow_new_buys else (self.invested_ratio_diverging * 0.5)
        calc_max_pos = max_positions if max_positions > 0 else self.max_positions_diverging
        target_per_name = (calc_invested / calc_max_pos) if calc_max_pos > 0 else 0.0

        orders: List[Order] = []
        candidates: list[tuple[str, float]] = []

        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar)
            if len(history) < max(self.slow_window, self.rvol_window) + 2:
                continue

            self._bar_index[code] = self._bar_index.get(code, 0) + 1
            bar_index = self._bar_index[code]

            closes = [b.close for b in history]
            highs = [b.high for b in history]
            vols = [b.volume for b in history]

            ema20 = _ema(closes, self.mid_window)[-1]
            ema60 = _ema(closes, self.slow_window)[-1]
            _macd_line, _, macd_hist = _macd(closes)
            rsi14 = _rsi(closes, 14)[-1]

            prev_vols = vols[-(self.rvol_window + 1) : -1]
            avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1.0
            rvol = (vols[-1] / avg_vol) if avg_vol > 0 else 0.0

            high20 = max(highs[-self.breakout_window :])
            prev_close = closes[-2]

            trend_up = (ema20 > ema60) and (bar.close > ema60)

            breakout_ok = (
                rvol >= self.rvol_threshold
                and bar.close > prev_close
                and bar.close >= 0.98 * high20
                and trend_up
                and macd_hist[-1] > 0
                and 55.0 <= rsi14 <= 85.0
            )

            if breakout_ok:
                self._last_breakout_index[code] = bar_index

            recent_breakout = False
            last_breakout_idx = self._last_breakout_index.get(code)
            if last_breakout_idx is not None and 0 < (bar_index - last_breakout_idx) <= self.pullback_window:
                recent_breakout = True

            pullback_ok = (
                recent_breakout
                and ema20 > ema60
                and ema20 * 0.98 <= bar.close <= ema20 * 1.05
                and vols[-1] < avg_vol * 1.2
            )

            current_qty = portfolio.positions.get(code, 0.0)

            if current_qty > 0 and code not in self._entry_price:
                self._entry_price[code] = bar.close
                self._peak_price_since_entry[code] = bar.close
                self._entry_index[code] = bar_index

            if code in self._entry_price:
                entry = self._entry_price[code]
                peak = max(self._peak_price_since_entry.get(code, bar.close), bar.close)
                self._peak_price_since_entry[code] = peak
                holding_bars = bar_index - self._entry_index[code]

                profit_pct = (bar.close - entry) / entry if entry > 0 else 0.0

                current_trailing_pct = (
                    self.tight_trailing_pct if profit_pct >= self.take_profit_pct else self.trailing_stop_pct
                )
                hard_stop = entry * (1.0 - self.hard_stop_loss_pct)
                trailing_stop = peak * (1.0 - current_trailing_pct)
                stop_price = max(hard_stop, trailing_stop)

                trend_broken = bar.close < ema20 * 0.95
                time_stop = (holding_bars >= self.max_hold_bars_no_profit) and (profit_pct < 0)
                stop_hit = bar.close <= stop_price

                if current_qty > 0 and (stop_hit or trend_broken or time_stop):
                    reason = (
                        "rvol_stop_loss"
                        if stop_hit
                        else ("rvol_time_stop" if time_stop else "rvol_trend_exit")
                    )
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason=reason))
                    self._entry_price.pop(code, None)
                    self._peak_price_since_entry.pop(code, None)
                    self._entry_index.pop(code, None)
                    continue

            if breakout_ok or pullback_ok or (current_qty > 0 and trend_up):
                score = self._score(closes, rvol)
                if score >= min_score and (allow_new_buys or current_qty > 0):
                    candidates.append((code, score))

        candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)
        rank_map = {code: rank for rank, (code, _) in enumerate(candidates, start=1)}

        desired = [code for code, _ in candidates[:calc_max_pos]] if calc_max_pos > 0 else []
        desired_set = set(desired)

        for code, qty in portfolio.positions.items():
            if qty <= 0:
                continue

            entry_idx = self._entry_index.get(code, 0)
            holding_bars = max(0, self._bar_index.get(code, entry_idx) - entry_idx)

            if holding_bars < self.min_hold_bars:
                continue

            current_rank = rank_map.get(code, 999)
            # Buffer zone: avoid rotation unless the name falls far outside the target ranks.
            buffer_threshold = max(calc_max_pos * 3, 10)

            if code not in desired_set and current_rank > buffer_threshold:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="rvol_rotate_out"))
                self._entry_price.pop(code, None)
                self._peak_price_since_entry.pop(code, None)
                self._entry_index.pop(code, None)

        for code in desired:
            bar2 = bars.get(code)
            if bar2 is None:
                continue
            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar2.close / portfolio.equity) if portfolio.equity > 0 else 0.0

            delta = target_per_name - current_pct

            if abs(delta) < self.rebalance_threshold_pct:
                continue

            if current_qty == 0 and not allow_new_buys:
                continue

            if delta > 0 and not allow_new_buys:
                continue

            action = "buy" if delta > 0 else "sell"
            reason = "rvol_entry" if current_qty == 0 else "rvol_rebalance"
            orders.append(Order(ts_code=code, action=action, target_pct=target_per_name, reason=reason))

            if action == "buy" and code not in self._entry_price:
                self._entry_price[code] = bar2.close
                self._peak_price_since_entry[code] = bar2.close
                self._entry_index[code] = self._bar_index.get(code, 0)

        return orders