from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.trendok import compute_trendok_for_symbols  # type: ignore[import-not-found]


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
