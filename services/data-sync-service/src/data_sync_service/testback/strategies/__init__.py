from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .sample_momentum import SampleMomentumStrategy
from .ma_crossover import MovingAverageCrossoverStrategy
from .watchlist_trend import WatchlistTrendStrategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    SampleMomentumStrategy.name: SampleMomentumStrategy,
    MovingAverageCrossoverStrategy.name: MovingAverageCrossoverStrategy,
    WatchlistTrendStrategy.name: WatchlistTrendStrategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
