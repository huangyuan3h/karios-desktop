"""tv/ego_lite.py branches (mocked playwright; never hits network)."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types

from data_sync_service.tv import ego_lite


class _FakeSpec:
    pass


def _fake_playwright(rows_out, raise_on_close=False):
    class FakePage:
        def __init__(self, ctx):
            self._ctx = ctx

        async def goto(self, *a, **k):
            pass

        async def wait_for_timeout(self, *a, **k):
            pass

        async def wait_for_load_state(self, *a, **k):
            raise Exception("networkidle timeout")

        async def reload(self, *a, **k):
            return None

        async def close(self):
            if raise_on_close:
                raise Exception("close failed")

    class FakeContext:
        def __init__(self, browser):
            self._browser = browser

        def set_default_timeout(self, ms):
            pass

        async def new_page(self):
            return FakePage(self)

        async def close(self):
            if raise_on_close:
                raise Exception("close failed")

    class FakeBrowser:
        async def new_context(self, **kw):
            assert kw["user_agent"] == ego_lite.EGO_LITE_USER_AGENT
            return FakeContext(self)

        async def close(self):
            if raise_on_close:
                raise Exception("close failed")

    class FakeChromium:
        async def launch(self, headless=True):
            return FakeBrowser()

    class FakePlaywright:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        chromium = FakeChromium()

    def fake_async_playwright():
        return FakePlaywright()

    async def fake_capture(page, url, max_rows):
        return "filters", ["h"], rows_out, "screen_title"

    fake_mod = types.SimpleNamespace(async_playwright=fake_async_playwright)

    return fake_mod, fake_capture


class TestEnsurePlaywright:
    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: _FakeSpec())
        assert ego_lite._ensure_playwright() is None

    def test_missing(self, monkeypatch) -> None:
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
        try:
            ego_lite._ensure_playwright()
            raise AssertionError("should raise")
        except ego_lite.EgoLiteUnavailable as e:
            assert "module_missing" in str(e)

    def test_import_error(self, monkeypatch) -> None:
        def boom(name):
            raise ImportError("broken")

        monkeypatch.setattr(importlib.util, "find_spec", boom)
        try:
            ego_lite._ensure_playwright()
            raise AssertionError("should raise")
        except ego_lite.EgoLiteUnavailable as e:
            assert "playwright_not_installed:broken" in str(e)


class TestCapture:
    def test_first_try_has_rows(self, monkeypatch) -> None:
        fake_mod, fake_capture = _fake_playwright(["r1"])
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: _FakeSpec())
        monkeypatch.setitem(sys.modules, "playwright.async_api", fake_mod)
        monkeypatch.setattr(ego_lite, "_capture_once_via_page", fake_capture)
        result = asyncio.run(
            ego_lite.capture_screener_ego_lite_async(url="https://example.com", max_rows=10, timeout_ms=5000)
        )
        assert result.rows == ["r1"]
        assert result.screen_title == "screen_title"
        assert result.filters == "filters"
        assert result.captured_at

    def test_empty_retries_after_reload(self, monkeypatch) -> None:
        fake_mod, fake_capture = _fake_playwright([])
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: _FakeSpec())
        monkeypatch.setitem(sys.modules, "playwright.async_api", fake_mod)
        monkeypatch.setattr(ego_lite, "_capture_once_via_page", fake_capture)
        result = asyncio.run(
            ego_lite.capture_screener_ego_lite_async(url="https://example.com")
        )
        assert result.rows == []

    def test_close_raises_ignored(self, monkeypatch) -> None:
        fake_mod, fake_capture = _fake_playwright(["r1"], raise_on_close=True)
        monkeypatch.setattr(importlib.util, "find_spec", lambda name: _FakeSpec())
        monkeypatch.setitem(sys.modules, "playwright.async_api", fake_mod)
        monkeypatch.setattr(ego_lite, "_capture_once_via_page", fake_capture)
        result = asyncio.run(
            ego_lite.capture_screener_ego_lite_async(url="https://example.com", timeout_ms=5000)
        )
        assert result.rows == ["r1"]

    def test_sync_wrapper(self, monkeypatch) -> None:
        async def fake_async(**kw):
            return types.SimpleNamespace(url=kw["url"], captured_at="t", screen_title="s", filters=1, headers=2, rows=3)

        monkeypatch.setattr(ego_lite, "capture_screener_ego_lite_async", fake_async)
        out = ego_lite.capture_screener_ego_lite_sync(url="https://example.com")
        assert out.rows == 3
