from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .watchlist_trend_v3 import WatchlistTrendV3Strategy
from .watchlist_trend_v4 import WatchlistTrendV4Strategy
from .watchlist_trend_v5 import WatchlistTrendV5Strategy
from .watchlist_trend_v5_1 import WatchlistTrendV5_1Strategy
from .watchlist_trend_v6 import WatchlistTrendV6Strategy
from .watchlist_momentum_rank import WatchlistMomentumRankStrategy
from .watchlist_momentum_v1_1 import WatchlistTrendV6_1Strategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    WatchlistTrendV3Strategy.name: WatchlistTrendV3Strategy,
    WatchlistTrendV4Strategy.name: WatchlistTrendV4Strategy,
    WatchlistTrendV5Strategy.name: WatchlistTrendV5Strategy,
    WatchlistTrendV5_1Strategy.name: WatchlistTrendV5_1Strategy,
    WatchlistTrendV6Strategy.name: WatchlistTrendV6Strategy,
    WatchlistMomentumRankStrategy.name: WatchlistMomentumRankStrategy,
    WatchlistTrendV6_1Strategy.name: WatchlistTrendV6_1Strategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
