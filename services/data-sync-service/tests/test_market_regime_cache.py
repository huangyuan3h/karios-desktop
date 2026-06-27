from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.market_regime import (  # type: ignore[import-not-found]
    INDEX_SIGNALS_CACHE_TTL_SECONDS,
    REGIME_CACHE_TTL_SECONDS,
    clear_index_signals_cache,
    clear_market_breadth_cache,
    clear_market_regime_cache,
    get_index_signals,
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


def test_get_index_signals_ttl_cache_hits_on_second_call() -> None:
    clear_index_signals_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_index_signals",
        return_value=[
            {"name": "上证指数", "signal": "green"},
            {"name": "创业板指", "signal": "yellow"},
        ],
    ) as compute:
        first = get_index_signals(include_breadth=False)
        second = get_index_signals(include_breadth=False)
    assert len(first) == 2
    assert second == first
    assert compute.call_count == 1


def test_get_index_signals_cache_expires() -> None:
    clear_index_signals_cache()
    t = 2000.0
    with (
        patch(
            "data_sync_service.service.market_regime._compute_index_signals",
            return_value=[{"name": "上证指数", "signal": "red"}],
        ) as compute,
        patch(
            "data_sync_service.service.market_regime.time.time",
            side_effect=[t, t + INDEX_SIGNALS_CACHE_TTL_SECONDS + 1],
        ),
    ):
        get_index_signals(as_of_date="2026-06-18", include_breadth=False)
        get_index_signals(as_of_date="2026-06-18", include_breadth=False)
    assert compute.call_count == 2


def test_clear_index_signals_cache() -> None:
    clear_index_signals_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_index_signals",
        return_value=[{"name": "上证指数", "signal": "green"}],
    ) as compute:
        get_index_signals(include_breadth=False)
        clear_index_signals_cache()
        get_index_signals(include_breadth=False)
    assert compute.call_count == 2


def test_breadth_ma20_ttl_cache_hits_on_second_call() -> None:
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    clear_market_breadth_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_breadth_above_ma20_ratio",
        return_value={"ratio": 0.5, "total": 2, "above_count": 1},
    ) as compute:
        first = market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")
        second = market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")

    assert first == second
    assert compute.call_count == 1


def test_liquidity_ttl_cache_hits_on_second_call() -> None:
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    clear_market_breadth_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_market_liquidity_and_mainline",
        return_value={
            "total_turnover_cny": 1.6e12,
            "max_industry_inflow": 6e9,
            "turnover_above_1_5T": True,
            "mainline_inflow_above_5B": True,
        },
    ) as compute:
        first = market_regime._get_market_liquidity_and_mainline(as_of_date="2026-06-18", breadth_ratio=0.6)
        second = market_regime._get_market_liquidity_and_mainline(as_of_date="2026-06-18", breadth_ratio=0.6)

    assert first == second
    assert compute.call_count == 1


def test_clear_market_breadth_cache_forces_recompute() -> None:
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    clear_market_breadth_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_breadth_above_ma20_ratio",
        return_value={"ratio": 0.5, "total": 2, "above_count": 1},
    ) as compute:
        market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")
        clear_market_breadth_cache()
        market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")

    assert compute.call_count == 2


def test_clear_index_signals_cache_clears_breadth_cache() -> None:
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    clear_market_breadth_cache()
    with patch(
        "data_sync_service.service.market_regime._compute_breadth_above_ma20_ratio",
        return_value={"ratio": 0.5, "total": 2, "above_count": 1},
    ) as compute:
        market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")
        clear_index_signals_cache()
        market_regime._get_breadth_above_ma20_ratio(as_of_date="2026-06-18")

    assert compute.call_count == 2
