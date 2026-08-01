"""Unit tests for the OPT-057 TV capture dispatcher (service/tv.py)."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from data_sync_service.service import tv as tvsvc
from data_sync_service.tv import scanner_api


def test_dispatch_api_mode_calls_scanner_api():
    fake_cap = type("Cap", (), {
        "url": "scanner_api://cn",
        "captured_at": "2026-08-01T00:00:00+00:00",
        "screen_title": "Karios Pullback v3 (CN)",
        "filters": ["market_cap_basic > 30000000000"],
        "headers": ["Symbol", "Price"],
        "rows": [{"Symbol": "NVDA", "Price": "123"}],
    })()
    with patch.object(tvsvc, "_capture_via_api", return_value=(fake_cap, "api")):
        result, via = tvsvc._dispatch_capture(
            mode="api",
            url="",
            filter_json={"and": []},
            api_columns=["name", "close"],
        )
    assert via == "api"
    assert result.screen_title == "Karios Pullback v3 (CN)"


def test_dispatch_api_mode_falls_back_to_ego_lite_on_transient():
    fake_ego = type("Cap", (), {
        "url": "x", "captured_at": "x", "screen_title": "x",
        "filters": [], "headers": [], "rows": [],
    })()
    with patch.object(
        tvsvc, "_capture_via_api",
        side_effect=scanner_api.TransientApiError("http_503"),
    ), patch.object(
        tvsvc, "_capture_via_ego_lite", return_value=(fake_ego, "ego_lite"),
    ):
        result, via = tvsvc._dispatch_capture(
            mode="api",
            url="https://www.tradingview.com/screener/abc/",
            filter_json={"and": []},
            api_columns=["name"],
        )
    assert via == "ego_lite"


def test_dispatch_api_mode_returns_502_when_both_api_and_ego_lite_fail():
    with patch.object(
        tvsvc, "_capture_via_api",
        side_effect=scanner_api.TransientApiError("http_503"),
    ), patch.object(
        tvsvc, "_capture_via_ego_lite",
        side_effect=scanner_api.TransientApiError("ego_lite_unavailable"),
    ):
        with pytest.raises(HTTPException) as ei:
            tvsvc._dispatch_capture(
                mode="api",
                url="x",
                filter_json={"and": []},
                api_columns=["name"],
            )
    assert ei.value.status_code == 502
    assert "api+ego_lite_failed" in str(ei.value.detail)


def test_dispatch_api_mode_returns_422_on_permanent_api_error():
    with patch.object(
        tvsvc, "_capture_via_api",
        side_effect=scanner_api.PermanentApiError("http_400:bad filter"),
    ):
        with pytest.raises(HTTPException) as ei:
            tvsvc._dispatch_capture(
                mode="api",
                url="x",
                filter_json={"and": []},
                api_columns=["name"],
            )
    assert ei.value.status_code == 422


def test_dispatch_api_mode_rejects_empty_filter():
    with pytest.raises(HTTPException) as ei:
        tvsvc._dispatch_capture(
            mode="api",
            url="",
            filter_json=None,
            api_columns=None,
        )
    assert ei.value.status_code == 409


def test_dispatch_chrome_mode_requires_url():
    with pytest.raises(HTTPException) as ei:
        tvsvc._dispatch_capture(
            mode="chrome",
            url="",
            filter_json=None,
            api_columns=None,
        )
    assert ei.value.status_code == 409


def test_dispatch_chrome_mode_calls_chrome():
    fake_cap = type("Cap", (), {
        "url": "https://www.tradingview.com/screener/abc/",
        "captured_at": "x", "screen_title": "x",
        "filters": [], "headers": [], "rows": [],
    })()
    with patch.object(tvsvc, "_capture_via_chrome", return_value=(fake_cap, "chrome")):
        result, via = tvsvc._dispatch_capture(
            mode="chrome",
            url="https://www.tradingview.com/screener/abc/",
            filter_json=None,
            api_columns=None,
        )
    assert via == "chrome"


# --- _filters_from_filter_json --------------------------------------------


def test_filters_from_flat_filter():
    f = {
        "and": [
            {"left": "market_cap_basic", "operation": "greater", "right": 30_000_000_000},
            {"left": "RSI", "operation": "in_range", "right": [45, 75]},
        ]
    }
    out = tvsvc._filters_from_filter_json(f)
    assert any("market_cap_basic greater 30000000000" in s for s in out)
    assert any("RSI in_range [45, 75]" in s for s in out)


def test_filters_handles_unknown_op():
    f = {"left": "x", "operation": "noop", "right": "y"}
    out = tvsvc._filters_from_filter_json(f)
    assert out == []  # unknown op → skipped