from __future__ import annotations

from typing import Dict, List

from .base import Bar, BaseStrategy, Order, PortfolioSnapshot


class SampleMomentumStrategy(BaseStrategy):
    name = "sample_momentum"

    def on_bar(self, trade_date: str, bars: Dict[str, Bar], portfolio: PortfolioSnapshot) -> List[Order]:
        _ = trade_date
        _ = portfolio
        if not bars:
            return []
        # Buy the single strongest bar by close price as a demo.
        target = max(bars.values(), key=lambda b: b.close)
        return [
            Order(
                ts_code=target.ts_code,
                action="buy",
                target_pct=1.0,
                reason="demo momentum",
            )
        ]
