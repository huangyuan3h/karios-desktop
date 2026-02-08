from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from playwright.async_api import async_playwright  # type: ignore[import-not-found]

from .normalize import drop_empty_columns, enrich_symbol_columns, normalize_headers


@dataclass(frozen=True)
class CaptureResult:
    url: str
    captured_at: str
    screen_title: str | None
    filters: list[str]
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

    async def estimate_data_rows(container) -> int:
        """
        Estimate number of data rows for scoring. We intentionally keep this cheap and robust.
        """
        try:
            tag = await _element_tag(container)
            if tag == "TABLE":
                # Prefer tbody rows with td cells.
                rows = container.locator("tbody tr")
                if await rows.count() == 0:
                    rows = container.locator("tr")
                cnt = 0
                n = min(await rows.count(), 60)
                for i in range(n):
                    r = rows.nth(i)
                    if await r.locator("td").count() > 0:
                        cnt += 1
                return cnt
            # Grid/treegrid
            rows = container.locator("[role=row]")
            cnt = 0
            n = min(await rows.count(), 120)
            for i in range(n):
                r = rows.nth(i)
                if await r.locator("[role=columnheader]").count() > 0:
                    continue
                if await r.locator("[role=gridcell]").count() > 0:
                    cnt += 1
            return cnt
        except Exception:
            return 0

    candidates = page.locator('[role="grid"], [role="treegrid"], table')
    best = None
    best_score = -1.0
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
            rows = await estimate_data_rows(c)
            # Prefer containers that already have visible data rows; area is a tie-breaker.
            score = float(rows) * 1000.0 + area
            if score > best_score:
                best_score = score
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
        # TradingView often has a leading selection/checkbox column that is empty.
        # Do NOT require the first cell to be non-empty; only skip fully empty rows.
        if not any(v.strip() for v in values):
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


async def _read_filter_pills(page, grid) -> list[str]:
    """
    Best-effort extraction of screener filter pills shown above the result table.
    TradingView UI can change frequently, so we use a heuristic based on position:
    visible buttons above the grid, excluding tab list buttons and trivial controls.
    """
    grid_box = await grid.bounding_box()
    if not grid_box:
        return []

    grid_top = float(grid_box["y"])
    candidates = page.locator("button, [role=button]")
    count = min(await candidates.count(), 260)

    exclude_exact = {
        "+",
        "…",
        "...",
        "Custom",
        "Overview",
        "Performance",
        "Extended Hours",
        "Valuation",
        "Dividends",
        "Profitability",
        "Income Statement",
        "More",
    }

    def _parse_rgb(s: str) -> tuple[int, int, int, float] | None:
        s2 = (s or "").strip().lower()
        if not (s2.startswith("rgb(") or s2.startswith("rgba(")):
            return None
        try:
            inner = s2[s2.find("(") + 1 : s2.rfind(")")]
            parts = [p.strip() for p in inner.split(",")]
            if len(parts) < 3:
                return None
            r = int(float(parts[0]))
            g = int(float(parts[1]))
            b = int(float(parts[2]))
            a = float(parts[3]) if len(parts) >= 4 else 1.0
            return r, g, b, a
        except Exception:
            return None

    def _is_dark_background(bg: str) -> bool:
        parsed = _parse_rgb(bg)
        if not parsed:
            return False
        r, g, b, a = parsed
        if a <= 0.05:
            return False
        lum = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0
        return lum < 0.42

    min_top = grid_top - 180
    max_bottom = grid_top - 6

    def _looks_like_condition(text: str) -> bool:
        t = (text or "").strip()
        if not t:
            return False
        tl = t.lower()
        if any(ch.isdigit() for ch in t):
            return True
        if any(op in t for op in (">", "<", "≥", "≤", "=")):
            return True
        if " to " in tl:
            return True
        if any(k in tl for k in ("strong buy", "buy", "sell", "neutral")):
            return True
        return False

    out: list[tuple[float, float, str, bool]] = []
    for i in range(count):
        el = candidates.nth(i)
        try:
            if not await el.is_visible():
                continue
            box = await el.bounding_box()
            if not box:
                continue
            top = float(box["y"])
            height = float(box["height"])
            if top + height > max_bottom:
                continue
            if top < min_top:
                continue
            if height < 18 or height > 60:
                continue

            is_in_tablist = await el.evaluate("el => !!el.closest('[role=tablist]')")
            if is_in_tablist:
                continue

            text = (await el.inner_text()).strip()
            if not text:
                continue
            text2 = " ".join(text.replace("\n", " ").split())
            if not text2 or len(text2) > 120:
                continue
            if text2 in exclude_exact:
                continue
            if not _looks_like_condition(text2):
                continue

            meta = await el.evaluate(
                """
                (el) => {
                  const cs = getComputedStyle(el);
                  const child = el.firstElementChild;
                  const cs2 = child ? getComputedStyle(child) : null;
                  return {
                    bg: cs.backgroundColor,
                    bg2: cs2 ? cs2.backgroundColor : null,
                    ariaPressed: el.getAttribute('aria-pressed'),
                    dataState: el.getAttribute('data-state'),
                    className: el.className ? String(el.className) : '',
                  };
                }
                """,
            )
            aria_pressed = str(meta.get("ariaPressed") or "").lower()
            data_state = str(meta.get("dataState") or "").lower()
            class_name = str(meta.get("className") or "").lower()
            bg = str(meta.get("bg") or "")
            bg2 = str(meta.get("bg2") or "")

            is_selected = False
            if aria_pressed in {"true", "mixed"}:
                is_selected = True
            if data_state in {"on", "checked", "active", "selected"}:
                is_selected = True
            if "active" in class_name or "selected" in class_name or "is-active" in class_name:
                is_selected = True
            if _is_dark_background(bg) or _is_dark_background(bg2):
                is_selected = True

            out.append((top, float(box["x"]), text2, is_selected))
        except Exception:
            continue

    out.sort(key=lambda t: (t[0], t[1]))

    def _dedupe(items: list[tuple[float, float, str, bool]], *, only_selected: bool) -> list[str]:
        seen: set[str] = set()
        pills: list[str] = []
        for _, _, t, sel in items:
            if only_selected and not sel:
                continue
            if t in seen:
                continue
            seen.add(t)
            pills.append(t)
            if len(pills) >= 40:
                break
        return pills

    selected = _dedupe(out, only_selected=True)
    if selected:
        return selected
    return _dedupe(out, only_selected=False)


async def _wait_for_grid_data(page, grid, *, tag: str, headers: list[str], key: str) -> None:
    """
    Best-effort wait for the first non-empty row to appear.
    """
    for _ in range(32):  # ~8s
        try:
            visible = (
                await _read_visible_table_rows(grid, headers)
                if tag == "TABLE"
                else await _read_visible_grid_rows(grid, headers)
            )
            for r in visible:
                sym = str(r.get(key, "") or "").strip()
                if sym:
                    return
        except Exception:
            pass
        await page.wait_for_timeout(250)


async def capture_screener_over_cdp(
    *,
    cdp_url: str,
    url: str,
    max_rows: int = 300,
    timeout_ms: int = 60_000,
) -> CaptureResult:
    """
    Capture TradingView screener rows by attaching to an already-running Chrome via CDP.
    """
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            raise RuntimeError(
                "CDP connected but no browser contexts were found. "
                "Start Chrome with --remote-debugging-port using a normal profile.",
            )
        context = browser.contexts[0]
        context.set_default_timeout(timeout_ms)
        page = await context.new_page()
        try:
            async def _capture_once() -> tuple[list[str], list[str], list[dict[str, str]], str | None]:
                grid = await _find_screener_grid(page)
                if grid is None:
                    raise RuntimeError(
                        "Cannot locate screener grid/table. Please ensure you are logged in.",
                    )

                filters = await _read_filter_pills(page, grid)
                tag = await _element_tag(grid)
                raw_headers = await (_read_table_headers(grid) if tag == "TABLE" else _read_grid_headers(grid))
                headers = normalize_headers(raw_headers or ["Symbol"])

                key = next((k for k in ("Symbol", "Ticker", "代码") if k in headers), headers[0])
                await _wait_for_grid_data(page, grid, tag=tag, headers=headers, key=key)

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
                        sym = str(r.get(key, "") or "").strip()
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
                        sym = str(r.get(key, "") or "").strip()
                        if sym and sym not in seen:
                            any_new = True
                            break
                    if added == 0 and not any_new:
                        break

                headers2, rows2 = enrich_symbol_columns(headers, rows)
                headers3, rows3 = drop_empty_columns(headers2, rows2)

                out_rows: list[dict[str, str]] = []
                for r in rows3:
                    out_rows.append({str(k): str(v) for k, v in r.items()})

                return filters, [str(h) for h in headers3], out_rows, await _detect_screen_title(page)

            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_load_state("networkidle", timeout=4_000)
            except Exception:
                pass
            filters, headers_out, rows_out, screen_title = await _capture_once()
            if not rows_out:
                await page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
                await page.wait_for_timeout(1200)
                try:
                    await page.wait_for_load_state("networkidle", timeout=4_000)
                except Exception:
                    pass
                filters, headers_out, rows_out, screen_title = await _capture_once()

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

