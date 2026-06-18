from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.trendok import (  # type: ignore[import-not-found]
    clear_trendok_cache,
    compute_trendok_for_symbols,
)


def test_compute_trendok_uses_lightweight_market_regime() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value={},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ),
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.eastmoney_industry.ensure_em_industries_for_ts_codes",
        ) as ensure_em,
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
        ) as fetch_em,
    ):
        out = compute_trendok_for_symbols(["CN:999999"], realtime=False)
    assert isinstance(out, list)
    get_regime.assert_called_once()
    assert get_regime.call_args.kwargs.get("include_breadth") is False
    ensure_em.assert_not_called()
    fetch_em.assert_not_called()


def test_compute_trendok_calls_stock_basic_lookup_once() -> None:
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value={"999999.SZ": [("2024-01-20", 10.0, 11.0, 9.0, 10.5, 1000.0)]},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ),
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ) as lookup_basic,
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        out = compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert isinstance(out, list)
    lookup_basic.assert_called_once()


def test_compute_trendok_cache_hit_skips_heavy_lookups() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ) as fetch_bars,
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ) as get_regime,
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ) as lookup_basic,
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        first = compute_trendok_for_symbols(["CN:999999"], realtime=False)
        second = compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert isinstance(first, list)
    assert isinstance(second, list)
    assert fetch_bars.call_count == 2
    lookup_basic.assert_called_once()
    flow_ctx.assert_called_once()
    get_regime.assert_called_once()


def test_clear_trendok_cache_forces_recompute() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ),
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        compute_trendok_for_symbols(["CN:999999"], realtime=False)
        clear_trendok_cache()
        compute_trendok_for_symbols(["CN:999999"], realtime=False)

    assert flow_ctx.call_count == 2


def test_compute_trendok_realtime_flag_separate_cache() -> None:
    bars = {"999999.SZ": [("2024-01-20", "10", "11", "9", "10.5", "1000")]}
    clear_trendok_cache()
    with (
        patch(
            "data_sync_service.service.trendok.fetch_last_ohlcv_batch",
            return_value=bars,
        ),
        patch(
            "data_sync_service.service.trendok.fetch_realtime_quotes",
            return_value={"ok": True, "items": []},
        ),
        patch(
            "data_sync_service.service.trendok._build_industry_flow_context",
            return_value={"ok": False},
        ) as flow_ctx,
        patch(
            "data_sync_service.service.trendok.get_market_regime",
            return_value={"regime": "Strong", "bias": None, "indexSignals": []},
        ),
        patch(
            "data_sync_service.service.trendok._lookup_stock_basic",
            return_value=({"999999.SZ": "Test"}, {"999999.SZ": "电子"}),
        ),
        patch(
            "data_sync_service.service.trendok._lookup_em_industry_boards",
            return_value={},
        ),
    ):
        compute_trendok_for_symbols(["CN:999999"], realtime=False)
        compute_trendok_for_symbols(["CN:999999"], realtime=True)

    assert flow_ctx.call_count == 2
