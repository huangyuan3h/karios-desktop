"""Unit tests for tv/ego_lite.py (OPT-057).

We don't actually launch a headless browser in tests (would slow CI);
we just verify the dispatcher paths and error mapping.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from data_sync_service.tv import ego_lite
from data_sync_service.tv.ego_lite import EgoLiteUnavailable


def test_ego_lite_unavailable_when_playwright_missing(monkeypatch):
    """If the playwright module cannot be found, _ensure_playwright raises
    EgoLiteUnavailable."""

    def fake_find_spec(name, *args, **kwargs):
        if name == "playwright.async_api":
            return None
        return orig_find_spec(name, *args, **kwargs)

    import importlib.util

    orig_find_spec = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(EgoLiteUnavailable):
        ego_lite._ensure_playwright()


def test_ego_lite_unavailable_propagates_via_scanner_transient():
    """_capture_via_ego_lite (in service/tv.py) wraps EgoLiteUnavailable
    as TransientApiError so the dispatcher can fall back to chrome."""
    from data_sync_service.service import tv as tvsvc
    from data_sync_service.tv import scanner_api

    with patch.object(
        ego_lite, "capture_screener_ego_lite_sync",
        side_effect=EgoLiteUnavailable("playwright_not_installed"),
    ):
        with pytest.raises(scanner_api.TransientApiError):
            tvsvc._capture_via_ego_lite(url="https://www.tradingview.com/screener/abc/")


def test_ego_lite_sync_wrapper_exists():
    """Just verify the sync wrapper is exposed (signature only)."""
    assert callable(ego_lite.capture_screener_ego_lite_sync)
    import inspect
    sig = inspect.signature(ego_lite.capture_screener_ego_lite_sync)
    assert "url" in sig.parameters
    assert "max_rows" in sig.parameters
    assert "timeout_ms" in sig.parameters