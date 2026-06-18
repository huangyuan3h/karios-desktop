"""Performance-path tests for watchlist alert modules (OPT-021)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import watchlist_momentum_alerts as momentum  # type: ignore[import-not-found]
from data_sync_service.service import watchlist_v5_alerts as v5  # type: ignore[import-not-found]

_SAMPLE_BARS = [
    ("2024-01-01", "10", "11", "9", "10.5", "1000"),
    ("2024-01-02", "10.5", "11.5", "10", "11", "1100"),
]


def _bars_120() -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    for i in range(120):
        d = f"2024-{(i // 28) + 1:02d}-{(i % 28) + 1:02d}"
        c = 10.0 + i * 0.05
        s = str(c)
        out.append((d, s, s, s, s, "1000"))
    return out


def test_v5_alerts_uses_lightweight_regime_once() -> None:
    bars = _bars_120()
    with (
        patch(
            "data_sync_service.service.watchlist_v5_alerts.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": bars, "000002.SZ": bars},
        ),
        patch(
            "data_sync_service.service.watchlist_v5_alerts.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.market_regime._get_breadth_above_ma20_ratio",
        ) as breadth,
    ):
        v5.compute_watchlist_v5_alerts(
            [
                {"symbol": "CN:000001", "position_pct": 0.1},
                {"symbol": "CN:000002", "position_pct": 0.2},
            ],
        )
    get_regime.assert_called_once()
    assert get_regime.call_args.kwargs.get("include_breadth") is False
    breadth.assert_not_called()


def test_momentum_alerts_uses_lightweight_regime_once() -> None:
    bars = _bars_120()
    with (
        patch(
            "data_sync_service.service.watchlist_momentum_alerts.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": bars, "000002.SZ": bars},
        ),
        patch(
            "data_sync_service.service.watchlist_momentum_alerts.get_market_regime",
            return_value={"regime": "Weak", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.market_regime._get_breadth_above_ma20_ratio",
        ) as breadth,
    ):
        momentum.compute_watchlist_momentum_alerts(
            [
                {"symbol": "CN:000001", "position_pct": 0.1},
                {"symbol": "CN:000002", "position_pct": 0.2},
            ],
        )
    get_regime.assert_called_once()
    assert get_regime.call_args.kwargs.get("include_breadth") is False
    breadth.assert_not_called()
