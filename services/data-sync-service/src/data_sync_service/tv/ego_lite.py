"""ego-lite screener capture (OPT-057).

Headless Playwright chromium (no Chrome profile, no TV login) used as a
mid-tier fallback when the TV Scanner API fails. Preserves the original
screener URL semantics so users with legacy URL-registered screeners don't
need to migrate to filter JSON.

NOTE: Playwright is intentionally NOT a hard dependency of data-sync-service
(only the chrome capture path uses it). If playwright is unavailable at
runtime (e.g. slim Docker image), import-time errors are surfaced clearly so
the dispatcher can skip ego_lite and fall straight through to chrome.
"""

from __future__ import annotations

import asyncio
import importlib.util
from datetime import UTC, datetime

# Reuse the helpers from capture.py. They are module-private by convention
# but the alternative is duplicating ~300 lines of DOM-walking logic.
from .capture import (  # type: ignore[import-not-found]
    CaptureResult,
    _capture_once_via_page,
)

DEFAULT_TIMEOUT_MS = 60_000
EGO_LITE_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


class EgoLiteUnavailable(RuntimeError):
    """Playwright / chromium not installed in this environment."""


def _ensure_playwright() -> None:
    """Best-effort check that playwright + chromium browser are installed.

    Raises EgoLiteUnavailable if not; caller should treat as fallback signal
    (skip ego_lite, try chrome directly).
    """
    try:
        if importlib.util.find_spec("playwright.async_api") is None:
            raise EgoLiteUnavailable("playwright_not_installed:module_missing")
    except ImportError as e:
        raise EgoLiteUnavailable(f"playwright_not_installed:{e}") from e


async def capture_screener_ego_lite_async(
    *,
    url: str,
    max_rows: int = 300,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> CaptureResult:
    """Async capture using a fresh headless chromium instance.

    No login profile, no persistent cookies — pure ephemeral capture.
    """
    _ensure_playwright()
    from playwright.async_api import async_playwright  # type: ignore[import-not-found]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=EGO_LITE_USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        context.set_default_timeout(timeout_ms)
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_load_state("networkidle", timeout=4_000)
            except Exception:
                pass
            filters, headers_out, rows_out, screen_title = await _capture_once_via_page(
                page, url=url, max_rows=max_rows
            )
            if not rows_out:
                # Retry once after reload — same pattern as capture.py
                await page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(1200)
                try:
                    await page.wait_for_load_state("networkidle", timeout=4_000)
                except Exception:
                    pass
                filters, headers_out, rows_out, screen_title = await _capture_once_via_page(
                    page, url=url, max_rows=max_rows
                )
            captured_at = datetime.now(tz=UTC).isoformat()
            return CaptureResult(
                url=url,
                captured_at=captured_at,
                screen_title=screen_title,
                filters=filters,
                headers=headers_out,
                rows=rows_out,
            )
        finally:
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass


def capture_screener_ego_lite_sync(
    *,
    url: str,
    max_rows: int = 300,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> CaptureResult:
    """Sync wrapper for FastAPI endpoints / dispatcher."""
    return asyncio.run(
        capture_screener_ego_lite_async(
            url=url,
            max_rows=max_rows,
            timeout_ms=timeout_ms,
        )
    )