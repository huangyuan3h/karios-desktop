"""Tests for ETF universe helpers in etf_fund_flow."""

from data_sync_service.service.etf_fund_flow import (
    _infer_etf_category,
    _CORE_ETF_TICKERS,
    ETF_WATCHLIST,
)


def test_etf_watchlist_has_six_core_entries():
    assert len(ETF_WATCHLIST) == 6
    assert _CORE_ETF_TICKERS == frozenset(item["symbol"] for item in ETF_WATCHLIST)


def test_core_etf_watchlist_contains_expected_tickers():
    expected = {"510300", "510050", "510500", "512480", "515880", "159819"}
    assert _CORE_ETF_TICKERS == expected


def test_infer_etf_category_5xxxxx_is_broad():
    assert _infer_etf_category("510300") == "broad"
    assert _infer_etf_category("510500") == "broad"
    assert _infer_etf_category("588000") == "broad"


def test_infer_etf_category_1xxxxx_is_sector():
    assert _infer_etf_category("159819") == "sector"
    assert _infer_etf_category("159099") == "sector"


def test_infer_etf_category_empty_defaults_to_broad():
    assert _infer_etf_category("") == "broad"