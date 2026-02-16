from __future__ import annotations
from collections import defaultdict, deque
from typing import Deque, Dict, List

from data_sync_service.service.market_regime import get_market_regime
from data_sync_service.service.trendok import _ema, _macd, _rsi
from data_sync_service.testback.strategies.base import Bar, BaseStrategy, Order, PortfolioSnapshot, ScoreConfig

class WatchlistTrendV5_1Strategy(BaseStrategy):
    """
    V6 策略：极致趋势跟随。
    核心逻辑：回归 V5 的多仓位模式，但引入“成交量爆发”和“动能排名”作为加仓前置。
    目标：超越 V5 的 137% 收益，并降低回撤。
    """
    name = "watchlist_trend_v5_1"
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
        stop_loss_pct: float = 0.10, # 稍微收紧止损，保护本金
    ) -> None:
        self.fast_window = max(2, int(fast_window))
        self.mid_window = max(self.fast_window + 1, int(mid_window))
        self.slow_window = max(self.mid_window + 1, int(slow_window))
        self.stop_loss_pct = max(0.01, float(stop_loss_pct))
        
        self._history: Dict[str, Deque[Bar]] = defaultdict(lambda: deque(maxlen=200))
        self._regime_cache: Dict[str, str] = {}
        self._entry_price: Dict[str, float] = {}
        self._max_price_since_entry: Dict[str, float] = {} # 用于移动止损

    def _get_regime(self, trade_date: str) -> str:
        if trade_date in self._regime_cache:
            return self._regime_cache[trade_date]
        info = get_market_regime(as_of_date=trade_date)
        regime = str(info.get("regime") or "Weak")
        self._regime_cache[trade_date] = regime
        return regime

    def _next_tranche(self, current_pct: float, base_target: float) -> float:
        """阶梯式加仓：0 -> 1/3 -> 2/3 -> 1.0"""
        if base_target <= 0: return 0.0
        step = base_target / 3.0
        if current_pct < step * 0.9: return step
        if current_pct < step * 1.9: return step * 2.0
        return base_target

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        if not bars: return []
        
        regime = self._get_regime(trade_date)
        orders: List[Order] = []
        
        # 市场环境决定单只股票的最高上限
        if regime == "Strong":
            base_target = 0.25  # 单只最高 25%
        elif regime == "Diverging":
            base_target = 0.15  # 单只最高 15%
        else:
            base_target = 0.05  # 弱市极其保守

        for code, bar in bars.items():
            self._history[code].append(bar)
            history = self._history[code]
            if len(history) < self.slow_window: continue

            closes = [b.close for b in history]
            vols = [b.volume for b in history]
            highs = [b.high for b in history]

            # 1. 计算核心指标
            ema20 = _ema(closes, self.mid_window)[-1]
            ema30 = _ema(closes, self.slow_window)[-1]
            macd_line, _, macd_hist = _macd(closes)
            rsi14 = _rsi(closes, 14)[-1] if len(closes) >= 14 else 50.0
            
            # 2. 成交量 confirmation (V6 新增)
            avg_vol = sum(vols[-20:]) / 20
            vol_ok = vols[-1] > avg_vol * 1.2 # 成交量需放大 20%
            
            # 3. 突破信号
            high20 = max(highs[-20:])
            breakout_ok = (
                bar.close >= 0.99 * high20 and 
                ema20 > ema30 and 
                macd_hist[-1] > 0 and 
                55 <= rsi14 <= 82 and
                vol_ok
            )

            # 4. 卖出逻辑 (引入移动止损概念)
            if code in self._entry_price:
                self._max_price_since_entry[code] = max(self._max_price_since_entry.get(code, 0), bar.close)
                
                # 移动止损：从最高点回撤 10% 或 跌破 EMA20
                trailing_stop = self._max_price_since_entry[code] * (1 - self.stop_loss_pct)
                stop_ok = bar.close <= trailing_stop
                trend_broken = bar.close < ema20 * 0.98 or macd_line[-1] < 0
                
                if stop_ok or trend_broken:
                    orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="v6_exit"))
                    self._entry_price.pop(code, None)
                    self._max_price_since_entry.pop(code, None)
                    continue

            # 5. 买入/加仓逻辑
            current_qty = portfolio.positions.get(code, 0.0)
            current_pct = (current_qty * bar.close / portfolio.equity) if portfolio.equity > 0 else 0.0
            
            if breakout_ok:
                target = self._next_tranche(current_pct, base_target)
                if target > current_pct:
                    orders.append(Order(ts_code=code, action="buy", target_pct=target, reason="v6_breakout"))
                    if code not in self._entry_price:
                        self._entry_price[code] = bar.close
                        self._max_price_since_entry[code] = bar.close

        return orders