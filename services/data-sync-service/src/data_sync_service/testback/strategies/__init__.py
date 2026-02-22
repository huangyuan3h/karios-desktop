from __future__ import annotations

from typing import Type

from .base import BaseStrategy
from .watchlist_momentum_v1_1 import WatchlistTrendV6_5Strategy

STRATEGY_REGISTRY: dict[str, Type[BaseStrategy]] = {
    WatchlistTrendV6_5Strategy.name: WatchlistTrendV6_5Strategy,
}


def get_strategy_class(name: str) -> Type[BaseStrategy]:
    name2 = (name or "").strip().lower()
    for key, cls in STRATEGY_REGISTRY.items():
        if key.lower() == name2:
            return cls
    raise ValueError(f"unknown strategy: {name}")
