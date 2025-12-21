from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from playwright.async_api import async_playwright

from .normalize import drop_empty_columns, enrich_symbol_columns, normalize_headers


@dataclass(frozen=True)
class CaptureResult:
    url: str
    captured_at: str
    screen_title: str | None
    headers: list[str]
    rows: list[dict[str, str]]


async def _element_tag(locator) -> str:
    try:
        return str(await locator.evaluate("el => el.tagName")).upper()
    except Exception:
        return ""


async def _detect_screen_title(page) -> str | None:
    try:
        title = (await page.title()).strip()
        if title and len(title) <= 200 and title.lower() not in {"tradingview", "stock screener"}:
            return title
    except Exception:
        pass
    return None


async def _find_screener_grid(page) -> Any | None:
    keywords = ["symbol", "ticker", "code", "代码", "名称", "股票"]

    async def headers_match(container) -> bool:
        cols = container.locator("[role=columnheader]")
        if await cols.count() > 0:
            texts: list[str] = []
            for i in range(await cols.count()):
                t = (await cols.nth(i).inner_text()).strip()
                if t:
                    texts.append(t)
            joined = " | ".join(texts).lower()
            return any(k in joined for k in keywords)

        th = container.locator("th")
        if await th.count() > 0:
            texts = []
            for i in range(await th.count()):
                t = (await th.nth(i).inner_text()).strip()
                if t:
                    texts.append(t)
            joined = " | ".join(texts).lower()
            return any(k in joined for k in keywords)

        return False

    candidates = page.locator('[role="grid"], [role="treegrid"], table')
    best = None
    best_area = 0.0
    for i in range(await candidates.count()):
        c = candidates.nth(i)
        try:
            if not await c.is_visible():
                continue
            if not await headers_match(c):
                continue
            box = await c.bounding_box()
            if not box:
                continue
            area = float(box["width"] * box["height"])
            if area > best_area:
                best_area = area
                best = c
        except Exception:
            continue
    return best


async def _read_table_headers(table) -> list[str]:
    headers: list[str] = []
    th = table.locator("thead th")
    if await th.count() == 0:
        th = table.locator("th")
    for i in range(await th.count()):
        t = (await th.nth(i).inner_text()).strip()
        if t:
            headers.append(t)
    return headers


async def _read_visible_table_rows(table, headers: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    rows = table.locator("tbody tr")
    if await rows.count() == 0:
        rows = table.locator("tr")
    for i in range(await rows.count()):
        row = rows.nth(i)
        cells = row.locator("td")
        if await cells.count() == 0:
            continue
        values: list[str] = []
        for j in range(await cells.count()):
            values.append((await cells.nth(j).inner_text()).strip())
        if not any(values):
            continue
        row_dict: dict[str, str] = {}
        for k, v in enumerate(values):
            key = headers[k] if k < len(headers) else f"col_{k}"
            row_dict[key] = v
        out.append(row_dict)
    return out


async def _read_grid_headers(grid) -> list[str]:
    headers: list[str] = []
    cols = grid.locator("[role=columnheader]")
    for i in range(await cols.count()):
        t = (await cols.nth(i).inner_text()).strip()
        if t:
            headers.append(t)
    return headers


async def _read_visible_grid_rows(grid, headers: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    rows = grid.locator("[role=row]")
    for i in range(await rows.count()):
        row = rows.nth(i)
        if await row.locator("[role=columnheader]").count() > 0:
            continue
        cells = row.locator("[role=gridcell]")
        if await cells.count() == 0:
            continue
        values: list[str] = []
        for j in range(await cells.count()):
            values.append((await cells.nth(j).inner_text()).strip())
        first = values[0].strip() if values else ""
        if not first:
            continue
        row_dict: dict[str, str] = {}
        for k, v in enumerate(values):
            key = headers[k] if k < len(headers) else f"col_{k}"
            row_dict[key] = v
        out.append(row_dict)
    return out


async def _scroll_grid(page, grid, *, steps: int = 1) -> None:
    box = await grid.bounding_box()
    if not box:
        return
    await page.mouse.move(box["x"] + 20, box["y"] + 20)
    for _ in range(steps):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(200)


async def capture_screener_over_cdp(
    *,
    cdp_url: str,
    url: str,
    max_rows: int = 300,
    timeout_ms: int = 60_000,
) -> CaptureResult:
    """
    Capture TradingView screener rows by attaching to an already-running Chrome via CDP.
    This is the recommended path to avoid Playwright launch issues and reuse a logged-in session.
    """
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            raise RuntimeError(
                "CDP connected but no browser contexts were found. "
                "Start Chrome with --remote-debugging-port using a normal profile."
            )
        context = browser.contexts[0]
        context.set_default_timeout(timeout_ms)
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(1200)

            grid = await _find_screener_grid(page)
            if grid is None:
                raise RuntimeError(
                    "Cannot locate screener grid/table. Please ensure you are logged in.",
                )

            tag = await _element_tag(grid)
            raw_headers = await (
                _read_table_headers(grid) if tag == "TABLE" else _read_grid_headers(grid)
            )
            headers = normalize_headers(raw_headers or ["Symbol"])

            seen: set[str] = set()
            rows: list[dict[str, str]] = []
            for _ in range(200):
                visible = (
                    await _read_visible_table_rows(grid, headers)
                    if tag == "TABLE"
                    else await _read_visible_grid_rows(grid, headers)
                )
                added = 0
                for r in visible:
                    sym = str(r.get(headers[0], "") or "").strip()
                    if not sym or sym in seen:
                        continue
                    seen.add(sym)
                    rows.append(r)
                    added += 1
                    if len(rows) >= max_rows:
                        break
                if len(rows) >= max_rows:
                    break

                await _scroll_grid(page, grid, steps=1)
                after = (
                    await _read_visible_table_rows(grid, headers)
                    if tag == "TABLE"
                    else await _read_visible_grid_rows(grid, headers)
                )
                any_new = False
                for r in after:
                    sym = str(r.get(headers[0], "") or "").strip()
                    if sym and sym not in seen:
                        any_new = True
                        break
                if added == 0 and not any_new:
                    break

            # Normalize + enrich
            headers2, rows2 = enrich_symbol_columns(headers, rows)
            headers3, rows3 = drop_empty_columns(headers2, rows2)

            # Ensure rows values are strings (JSON-friendly).
            out_rows: list[dict[str, str]] = []
            for r in rows3:
                out_rows.append({str(k): str(v) for k, v in r.items()})

            captured_at = datetime.now(tz=UTC).isoformat()
            return CaptureResult(
                url=url,
                captured_at=captured_at,
                screen_title=await _detect_screen_title(page),
                headers=[str(h) for h in headers3],
                rows=out_rows,
            )
        finally:
            try:
                await page.close()
            except Exception:
                pass
            # IMPORTANT: do NOT close the CDP browser here; it would close the shared Chrome.
            # We also avoid calling browser.close() to keep the dedicated Chrome alive.


def capture_screener_over_cdp_sync(
    *,
    cdp_url: str,
    url: str,
    max_rows: int = 300,
    timeout_ms: int = 60_000,
) -> CaptureResult:
    """
    Sync wrapper for FastAPI endpoints that are implemented as sync functions.
    """
    return asyncio.run(
        capture_screener_over_cdp(
            cdp_url=cdp_url,
            url=url,
            max_rows=max_rows,
            timeout_ms=timeout_ms,
        ),
    )


