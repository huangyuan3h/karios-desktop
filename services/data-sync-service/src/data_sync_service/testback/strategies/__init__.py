from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .AlphaHunterV1Strategy import AlphaHunterV1Strategy
from .watchlist_trend_v8 import WatchlistTrendV8Strategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    AlphaHunterV1Strategy.name: AlphaHunterV1Strategy,
    WatchlistTrendV8Strategy.name: WatchlistTrendV8Strategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
