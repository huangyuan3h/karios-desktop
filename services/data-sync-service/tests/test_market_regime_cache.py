from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.market_regime import (  # type: ignore[import-not-found]
    REGIME_CACHE_TTL_SECONDS,
    clear_market_regime_cache,
    get_market_regime,
)


def test_get_market_regime_include_breadth_false_skips_breadth_scan() -> None:
    clear_market_regime_cache()
    with (
        patch(
            "data_sync_service.service.market_regime.get_index_signals",
            return_value=[
                {"name": "上证指数", "signal": "green"},
                {"name": "创业板指", "signal": "green"},
            ],
        ) as get_signals,
        patch(
            "data_sync_service.service.market_regime._get_breadth_above_ma20_ratio",
        ) as breadth,
    ):
        out = get_market_regime(as_of_date="2026-06-18", include_breadth=False)
    assert out["regime"] == "Strong"
    get_signals.assert_called_once_with(as_of_date="2026-06-18", include_breadth=False)
    breadth.assert_not_called()


def test_get_market_regime_ttl_cache_hits_on_second_call() -> None:
    clear_market_regime_cache()
    with patch(
        "data_sync_service.service.market_regime.get_index_signals",
        return_value=[
            {"name": "上证指数", "signal": "red"},
            {"name": "创业板指", "signal": "red"},
        ],
    ) as get_signals:
        first = get_market_regime(as_of_date="2026-06-18", include_breadth=False)
        second = get_market_regime(as_of_date="2026-06-18", include_breadth=False)
    assert first["regime"] == "Weak"
    assert second["regime"] == "Weak"
    assert get_signals.call_count == 1


def test_get_market_regime_cache_expires() -> None:
    clear_market_regime_cache()
    t = 1000.0
    with (
        patch(
            "data_sync_service.service.market_regime.get_index_signals",
            return_value=[
                {"name": "上证指数", "signal": "green"},
                {"name": "创业板指", "signal": "yellow"},
            ],
        ) as get_signals,
        patch("data_sync_service.service.market_regime.time.time", side_effect=[t, t + REGIME_CACHE_TTL_SECONDS + 1]),
    ):
        get_market_regime(include_breadth=False)
        get_market_regime(include_breadth=False)
    assert get_signals.call_count == 2


def test_clear_market_regime_cache() -> None:
    clear_market_regime_cache()
    with patch(
        "data_sync_service.service.market_regime.get_index_signals",
        return_value=[
            {"name": "上证指数", "signal": "green"},
            {"name": "创业板指", "signal": "green"},
        ],
    ) as get_signals:
        get_market_regime(include_breadth=False)
        clear_market_regime_cache()
        get_market_regime(include_breadth=False)
    assert get_signals.call_count == 2
