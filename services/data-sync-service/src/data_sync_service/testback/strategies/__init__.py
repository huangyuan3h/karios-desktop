from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .sample_momentum import SampleMomentumStrategy
from .ma_crossover import MovingAverageCrossoverStrategy
from .watchlist_trend import WatchlistTrendStrategy
from .watchlist_trend_v2 import WatchlistTrendV2Strategy
from .watchlist_trend_v3 import WatchlistTrendV3Strategy
from .watchlist_trend_v4 import WatchlistTrendV4Strategy
from .watchlist_trend_v5 import WatchlistTrendV5Strategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    SampleMomentumStrategy.name: SampleMomentumStrategy,
    MovingAverageCrossoverStrategy.name: MovingAverageCrossoverStrategy,
    WatchlistTrendStrategy.name: WatchlistTrendStrategy,
    WatchlistTrendV2Strategy.name: WatchlistTrendV2Strategy,
    WatchlistTrendV3Strategy.name: WatchlistTrendV3Strategy,
    WatchlistTrendV4Strategy.name: WatchlistTrendV4Strategy,
    WatchlistTrendV5Strategy.name: WatchlistTrendV5Strategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
