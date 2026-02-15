from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, List

from .base import Bar, BaseStrategy, Order, PortfolioSnapshot

# 最终资金
# 40.42万
class MovingAverageCrossoverStrategy(BaseStrategy):
    # Baseline strategy: buy when fast MA crosses above slow MA, sell when it crosses below.
    name = "ma_crossover"

    def __init__(self, fast_window: int = 5, slow_window: int = 20) -> None:
        self.fast_window = max(2, int(fast_window))
        self.slow_window = max(self.fast_window + 1, int(slow_window))
        self._history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=self.slow_window))

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        _ = trade_date
        _ = portfolio
        if not bars:
            return []
        orders: List[Order] = []
        for code, bar in bars.items():
            history = self._history[code]
            history.append(bar.close)
            if len(history) < self.slow_window:
                continue
            fast_ma = sum(list(history)[-self.fast_window:]) / self.fast_window
            slow_ma = sum(history) / self.slow_window
            if fast_ma > slow_ma:
                orders.append(Order(ts_code=code, action="buy", target_pct=1.0, reason="ma crossover up"))
            elif fast_ma < slow_ma:
                orders.append(Order(ts_code=code, action="sell", target_pct=0.0, reason="ma crossover down"))
        return orders
