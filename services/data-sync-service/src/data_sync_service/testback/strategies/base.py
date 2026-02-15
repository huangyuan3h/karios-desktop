from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Bar:
    ts_code: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    avg_price: float
    volume: float
    amount: float


@dataclass
class Order:
    ts_code: str
    action: str
    qty: Optional[float] = None
    target_pct: Optional[float] = None
    reason: Optional[str] = None


@dataclass
class PortfolioSnapshot:
    cash: float
    equity: float
    positions: Dict[str, float]


class BaseStrategy(ABC):
    name = "base"

    def on_start(self, start_date: str, end_date: str) -> None:
        _ = start_date
        _ = end_date

    @abstractmethod
    def on_bar(
        self,
        trade_date: str,
        bars: Dict[str, Bar],
        portfolio: PortfolioSnapshot,
    ) -> List[Order]:
        raise NotImplementedError

    def on_finish(self, portfolio: PortfolioSnapshot) -> None:
        _ = portfolio
