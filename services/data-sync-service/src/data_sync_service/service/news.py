"""RSS feed fetching service."""

from __future__ import annotations

import hashlib
import re
import time
from datetime import UTC, datetime

from data_sync_service.db.news import (
    delete_old_items,
    ensure_tables,
    fetch_sources,
    update_source_last_fetch,
    upsert_item,
)

try:
    import feedparser  # type: ignore[import-not-found]
except ImportError:
    feedparser = None

# HTML tag stripper — keeps text content, drops tags
_HTML_TAG_RE = re.compile(r"<[^>]+>")
# Fallback: handle unclosed tags (RSS summaries truncated to 500 chars
# before HTML close). Drops the orphan `<` and everything until EOS.
_HTML_ORPHAN_RE = re.compile(r"<[^>]*$")
# Multiple whitespace collapsed to single space
_MULTI_SPACE_RE = re.compile(r"\s+")
# Common HTML entities
_HTML_ENTITIES = {"&amp;": "&", "&lt;": "<", "&gt;": ">", "&nbsp;": " ", "&quot;": '"'}


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities from RSS summary."""
    text = _HTML_TAG_RE.sub(" ", text)
    text = _HTML_ORPHAN_RE.sub(" ", text)
    for entity, char in _HTML_ENTITIES.items():
        text = text.replace(entity, char)
    text = _MULTI_SPACE_RE.sub(" ", text).strip()
    return text


def _normalize_title(title: str) -> str:
    """Normalize title for dedup: strip whitespace, punctuation, lowercased."""
    t = title.strip().lower()
    # Remove common prefixes like 【xxx】
    t = re.sub(r"【[^】]*】", "", t)
    # Remove parenthetical content
    t = re.sub(r"[（(][^)）]*[)）]", "", t)
    # Strip punctuation
    t = re.sub(r"[，。！？、；：""''「」\[\]【】(),.!?;:\"]", "", t)
    t = _MULTI_SPACE_RE.sub("", t)
    return t


def fetch_rss_feed(url: str) -> list[dict]:
    if feedparser is None:
        raise RuntimeError("feedparser is not installed. Run: uv add feedparser")

    parsed = feedparser.parse(url)
    items = []
    for entry in parsed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        if not title or not link:
            continue

        item_id = hashlib.md5(link.encode()).hexdigest()[:16]

        summary = entry.get("summary") or entry.get("description") or None
        if summary:
            summary = _strip_html(summary)[:500]

        published_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                ts = time.mktime(entry.published_parsed)
                published_at = datetime.fromtimestamp(ts, tz=UTC).isoformat()
            except Exception:
                pass
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            try:
                ts = time.mktime(entry.updated_parsed)
                published_at = datetime.fromtimestamp(ts, tz=UTC).isoformat()
            except Exception:
                pass

        items.append(
            {
                "id": item_id,
                "title": title.strip(),
                "link": link,
                "summary": summary,
                "published_at": published_at,
            }
        )
    return items


def fetch_all_sources() -> dict[str, int]:
    """Fetch all enabled sources with cross-source dedup.

    Deduplication: normalized titles that are identical or very similar
    (>80% character overlap) are collapsed — only the first occurrence is kept.
    This prevents the same news story from multiple sources (e.g. 财联社 + 华尔街见闻
    both covering the same event) from inflating item counts.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ensure_tables()
    sources = fetch_sources(enabled_only=True)

    # --- Cross-source dedup state ---
    seen_titles: dict[str, str] = {}  # normalized_title -> item_id of first occurrence

    def _is_duplicate(title: str) -> str | None:
        """Return the existing item_id if title is a duplicate, else None."""
        norm = _normalize_title(title)
        if not norm:
            return None
        # Exact match
        if norm in seen_titles:
            return seen_titles[norm]
        # Fuzzy: >80% character overlap (cheap Jaccard on character sets)
        norm_set = set(norm)
        for existing_norm, existing_id in seen_titles.items():
            existing_set = set(existing_norm)
            if not norm_set or not existing_set:
                continue
            intersection = len(norm_set & existing_set)
            union = len(norm_set | existing_set)
            if union > 0 and intersection / union > 0.8:
                return existing_id
        return None

    def _register_title(title: str, item_id: str) -> None:
        norm = _normalize_title(title)
        if norm and norm not in seen_titles:
            seen_titles[norm] = item_id

    def _fetch_one(source: dict) -> tuple[str, int]:
        source_id = source["id"]
        url = source["url"]
        try:
            items = fetch_rss_feed(url)
            fetched_at = datetime.now(UTC).isoformat()
            count = 0
            deduped = 0
            for item in items:
                existing_id = _is_duplicate(item["title"])
                if existing_id:
                    deduped += 1
                    continue
                _register_title(item["title"], item["id"])
                upsert_item(
                    item_id=item["id"],
                    source_id=source_id,
                    title=item["title"],
                    link=item["link"],
                    summary=item["summary"],
                    published_at=item["published_at"],
                    fetched_at=fetched_at,
                )
                count += 1
            update_source_last_fetch(source_id, fetched_at)
            if deduped:
                print(f"[news] {source_id}: {deduped} duplicates skipped")
            return source_id, count
        except Exception as e:
            print(f"[news] Failed to fetch {url}: {e}")
            return source_id, -1

    results: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_fetch_one, s): s["id"] for s in sources}
        for future in as_completed(futures):
            source_id, count = future.result()
            results[source_id] = count

    delete_old_items(hours=72)
    return results


def _rsshub_url(path: str) -> str:
    """Build an RSSHub URL using the same env var the alpha-radar pipeline uses.

    Falls back to http://127.0.0.1:1200 if ALPHA_RADAR_RSSHUB_BASE_URL is unset.
    """
    import os

    base = os.getenv("ALPHA_RADAR_RSSHUB_BASE_URL", "http://127.0.0.1:1200").rstrip("/")
    return f"{base}{path}"


# News Substrate 2.0 · Track 1 (2026-08-02) — trimmed to5 core sources 2026-08-02
# Only sources that directly serve A-share investment decisions.
# Tier A = must-read real-time telegraph · Tier C = policy first-hand.
DEFAULT_NEWS_SOURCES: list[tuple[str, str, str, str, str | None]] = [
    # Tier A — real-time telegraph, must-read for A-share
    ("cls-telegraph", "财联社·电报", _rsshub_url("/cls/telegraph"), "A", "telegraph"),
    (
        "wallstreetcn-global",
        "华尔街见闻·全球快讯",
        _rsshub_url("/wallstreetcn/live/global"),
        "A",
        "telegraph",
    ),
    ("jin10-flash", "金十数据·快讯", _rsshub_url("/jin10/flash"), "A", "telegraph"),
    # Depth / research
    ("cls-depth", "财联社·深度研判", _rsshub_url("/cls/depth"), "A", "depth"),
    # Policy first-hand
    ("csrc-news", "证监会·要闻", _rsshub_url("/gov/csrc/news"), "C", "policy"),
]


# Sources to disable (kept around for back-compat, not deleted).
# Tier D = not in DEFAULT_NEWS_SOURCES, kept disabled.
# - legacy 4: bbc/nyt/hn/reddit — generic world/HN/Reddit, not investment-grade
# - playwright_required 6: routes that 503 without Playwright in our RSSHub env
# - trimmed 2026-08-02: removed 8 sources to reduce noise (36kr, huxiu, yicai,
#   gelonghui, caixin, stats-gov, jin10-data, wallstreetcn-us)
LEGACY_DISABLED_SOURCES: list[str] = [
    "bbc-world",
    "nyt-world",
    "hn-front",
    "reddit-finance",
    "xueqiu-hots",
    "10jqka-major",
    "eastmoney-yaowen",
    "stcn-ecom",
    "36kr-flash",
    "reuters-business",
    "gelonghui-home",
    "caixin-headline",
    "wallstreetcn-us",
    "yicai-news",
    "36kr-news",
    "huxiu-finance",
    "7e2ce389",
    "jin10-data",
]


def add_default_sources() -> None:
    """Idempotent seed: upsert 13 investment-grade sources + disable 4 legacy ones.

    Safe to run on every startup. New source tiers / categories are propagated via
    ON CONFLICT (url) DO UPDATE. Existing rows are not overwritten for name/enabled
    if the row already exists (so user toggles persist).
    """
    from data_sync_service.db.news import create_source, update_source

    for sid, name, url, tier, category in DEFAULT_NEWS_SOURCES:
        try:
            create_source(
                source_id=sid,
                name=name,
                url=url,
                enabled=True,
                tier=tier,
                category=category,
            )
        except Exception as e:
            print(f"[news] failed to seed source {sid}: {e}")

    for sid in LEGACY_DISABLED_SOURCES:
        try:
            update_source(source_id=sid, enabled=False)
        except Exception:
            pass