from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


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
    qty: float | None = None
    target_pct: float | None = None
    reason: str | None = None


@dataclass
class PortfolioSnapshot:
    cash: float
    equity: float
    positions: dict[str, float]


@dataclass
class ScoreConfig:
    top_n: int = 1000
    momentum_weight: float = 1.0
    volume_weight: float = 0.0
    amount_weight: float = 0.0


class BaseStrategy(ABC):
    name = "base"
    use_full_bars = False

    def on_start(self, start_date: str, end_date: str) -> None:
        _ = start_date
        _ = end_date

    @abstractmethod
    def on_bar(
        self,
        trade_date: str,
        bars: dict[str, Bar],
        portfolio: PortfolioSnapshot,
    ) -> list[Order]:
        raise NotImplementedError

    @classmethod
    def default_score_config(cls) -> ScoreConfig:
        return ScoreConfig()

    def on_finish(self, portfolio: PortfolioSnapshot) -> None:
        _ = portfolio
