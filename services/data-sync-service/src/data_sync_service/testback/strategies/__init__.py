from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .watchlist_trend_v8 import WatchlistTrendV8Strategy
from .watchlist_trend_v9 import WatchlistTrendV9Strategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    WatchlistTrendV8Strategy.name: WatchlistTrendV8Strategy,
    WatchlistTrendV9Strategy.name: WatchlistTrendV9Strategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
