"""
TradingView Screener capture (v0 validation).

This script uses a persistent Playwright profile so that your TradingView login
session and saved screeners are reused across runs.

Notes:
- First run: you will likely need to login manually.
- Subsequent runs: reuse the same profile directory (cookies/localStorage).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright


def now_utc_compact() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d-%H%M%S")


def default_profile_dir() -> Path:
    # Keep it outside the repo to avoid accidentally committing browser state.
    base = Path.home() / ".karios" / "playwright" / "tradingview-profile"
    base.mkdir(parents=True, exist_ok=True)
    return base


@dataclass(frozen=True)
class CaptureResult:
    url: str
    captured_at: str
    screen: str | None
    headers: list[str]
    rows: list[dict[str, str]]


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "-", value)
    value = re.sub(r"[^a-z0-9\-_.]+", "", value)
    return value or "screen"


async def select_saved_screen(page, screen_name: str) -> None:
    """
    Best-effort: open the screener dropdown and pick a saved screen by name.

    TradingView UI may change. If this fails, use --wait-login and select the screen manually.
    """
    trigger = page.get_by_text("Stock Screener").first
    await trigger.click(timeout=10_000)

    item = page.get_by_text(screen_name, exact=True).first
    await item.click(timeout=10_000)

    # Wait for the page header to reflect the selection.
    await page.get_by_text(screen_name, exact=False).first.wait_for(timeout=20_000)


async def find_screener_grid(page) -> Any | None:
    """
    Best-effort: find a visible ARIA grid that looks like a screener table.
    TradingView UI may change. We use heuristics rather than brittle selectors.
    """
    grids = page.locator("[role=grid]")
    count = await grids.count()
    for i in range(count):
        g = grids.nth(i)
        try:
            if not await g.is_visible():
                continue
            # Heuristic: the screener grid usually has a "Symbol" column header.
            if await g.get_by_text("Symbol", exact=True).count() > 0:
                return g
        except Exception:
            # Ignore a single grid that fails and continue.
            continue
    return None


async def read_grid_headers(grid) -> list[str]:
    headers: list[str] = []
    cols = grid.locator("[role=columnheader]")
    for i in range(await cols.count()):
        t = (await cols.nth(i).inner_text()).strip()
        if t:
            headers.append(t)
    return headers


async def read_visible_rows(grid, headers: list[str]) -> list[dict[str, str]]:
    """
    Read currently rendered (visible) rows.
    TradingView uses virtualized lists, so we must scroll and aggregate.
    """
    out: list[dict[str, str]] = []
    rows = grid.locator("[role=row]")
    row_count = await rows.count()

    for i in range(row_count):
        row = rows.nth(i)
        # Skip header rows: they usually contain columnheaders.
        if await row.locator("[role=columnheader]").count() > 0:
            continue

        cells = row.locator("[role=gridcell]")
        cell_count = await cells.count()
        if cell_count == 0:
            continue

        values: list[str] = []
        for j in range(cell_count):
            values.append((await cells.nth(j).inner_text()).strip())

        # Align values to headers as best as possible.
        row_dict: dict[str, str] = {}
        for k, v in enumerate(values):
            key = headers[k] if k < len(headers) else f"col_{k}"
            row_dict[key] = v

        # A screener row should have a symbol-like value in the first cell.
        first = values[0].strip() if values else ""
        if not first:
            continue
        out.append(row_dict)

    return out


async def scroll_grid(page, grid, *, steps: int = 1) -> None:
    # Move mouse onto the grid to make wheel scrolling affect it.
    box = await grid.bounding_box()
    if not box:
        return
    await page.mouse.move(box["x"] + 20, box["y"] + 20)
    for _ in range(steps):
        await page.mouse.wheel(0, 1200)
        await page.wait_for_timeout(200)


async def capture_screener(
    *,
    url: str,
    screen: str | None,
    profile_dir: Path,
    headless: bool,
    max_rows: int,
    wait_for_manual_login: bool,
    output_dir: Path,
    screenshot: bool,
) -> CaptureResult:
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=headless,
            channel="chrome",
            viewport={"width": 1280, "height": 820},
        )
        page = context.pages[0] if context.pages else await context.new_page()

        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)

        if wait_for_manual_login:
            print("\n[Manual step] Login / confirm the screener is visible.")
            print("Then press Enter here to continue capture...\n")
            await asyncio.to_thread(input)

        if screen:
            await select_saved_screen(page, screen)
            await page.wait_for_timeout(800)

        grid = await find_screener_grid(page)
        if grid is None:
            raise RuntimeError(
                "Cannot locate screener grid. "
                "Try using a direct screener URL and ensure the table is visible."
            )

        headers = await read_grid_headers(grid)
        if not headers:
            # Fallback: still capture but with generic column names.
            headers = ["Symbol"]

        # Aggregate unique rows by the first column (often symbol).
        seen: set[str] = set()
        rows: list[dict[str, str]] = []

        # Cap scroll loops so we don't get stuck on infinite lists.
        for _ in range(200):
            visible = await read_visible_rows(grid, headers)
            added = 0
            for r in visible:
                symbol = (r.get(headers[0]) or "").strip()
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                rows.append(r)
                added += 1
                if len(rows) >= max_rows:
                    break

            if len(rows) >= max_rows:
                break

            # If nothing new appeared after a scroll, we likely reached the end.
            await scroll_grid(page, grid, steps=1)
            after = await read_visible_rows(grid, headers)
            any_new = False
            for r in after:
                symbol = (r.get(headers[0]) or "").strip()
                if symbol and symbol not in seen:
                    any_new = True
                    break
            if added == 0 and not any_new:
                break

        captured_at = datetime.now(tz=UTC).isoformat()
        result = CaptureResult(
            url=url,
            captured_at=captured_at,
            screen=screen,
            headers=headers,
            rows=rows,
        )

        output_dir.mkdir(parents=True, exist_ok=True)
        ts = now_utc_compact()
        suffix = f"-{slugify(screen)}" if screen else ""
        json_path = output_dir / f"tv-screener{suffix}-{ts}.json"
        csv_path = output_dir / f"tv-screener{suffix}-{ts}.csv"

        json_payload = json.dumps(result.__dict__, ensure_ascii=False, indent=2)
        json_path.write_text(json_payload, encoding="utf-8")
        with csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        if screenshot:
            png_path = output_dir / f"tv-screener{suffix}-{ts}.png"
            await page.screenshot(path=str(png_path), full_page=True)

        await context.close()
        return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture TradingView screener rows using Playwright.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("TV_SCREENER_URL", "").strip(),
        help="TradingView screener URL (or set TV_SCREENER_URL env var).",
    )
    parser.add_argument(
        "--screens",
        default=os.getenv("TV_SCREENS", "").strip(),
        help="Comma-separated saved screen names to capture (or set TV_SCREENS).",
    )
    parser.add_argument(
        "--profile-dir",
        default=str(default_profile_dir()),
        help="Persistent profile directory for Playwright (stores cookies/localStorage).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "data"),
        help="Where to write capture artifacts (json/csv/png).",
    )
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode.")
    parser.add_argument(
        "--wait-login",
        action="store_true",
        help=(
            "Pause after opening the page for manual login/adjustments, "
            "then press Enter to continue."
        ),
    )
    parser.add_argument("--screenshot", action="store_true", help="Save a full-page screenshot.")
    parser.add_argument("--max-rows", type=int, default=200, help="Max rows to capture.")
    parser.add_argument(
        "--print",
        action="store_true",
        help="Print captured headers and rows to stdout (best-effort).",
    )
    parser.add_argument(
        "--print-rows",
        type=int,
        default=10,
        help="How many rows to print per screen when --print is enabled.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.url:
        raise SystemExit("Missing --url (or TV_SCREENER_URL).")

    screens = [s.strip() for s in str(args.screens).split(",") if s.strip()]
    if not screens:
        screens = [""]

    for screen in screens:
        result = asyncio.run(
            capture_screener(
                url=args.url,
                screen=screen or None,
                profile_dir=Path(args.profile_dir),
                headless=bool(args.headless),
                max_rows=int(args.max_rows),
                wait_for_manual_login=bool(args.wait_login),
                output_dir=Path(args.output_dir),
                screenshot=bool(args.screenshot),
            ),
        )
        name = result.screen or "(current)"
        print(f"\nScreen: {name}")
        print(f"Captured rows: {len(result.rows)}")
        if args.print:
            print("Headers:", ", ".join(result.headers))
            n = max(0, int(args.print_rows))
            sample = result.rows[:n]
            print(f"Rows (first {len(sample)}):")
            print(json.dumps(sample, ensure_ascii=False, indent=2))
        if result.rows:
            first = result.rows[0]
            print("Sample row keys:", ", ".join(list(first.keys())[:8]))
            print("Sample row:", {k: first[k] for k in list(first.keys())[:8]})


if __name__ == "__main__":
    main()


