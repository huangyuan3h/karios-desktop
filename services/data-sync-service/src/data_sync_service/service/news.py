"""RSS feed fetching service."""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone

from data_sync_service.db.news import (
    ensure_tables,
    fetch_sources,
    upsert_item,
    update_source_last_fetch,
    delete_old_items,
)

try:
    import feedparser  # type: ignore[import-not-found]
except ImportError:
    feedparser = None


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
            summary = summary[:500]

        published_at = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                ts = time.mktime(entry.published_parsed)
                published_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                pass
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            try:
                ts = time.mktime(entry.updated_parsed)
                published_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except Exception:
                pass

        items.append(
            {
                "id": item_id,
                "title": title,
                "link": link,
                "summary": summary,
                "published_at": published_at,
            }
        )
    return items


def fetch_all_sources() -> dict[str, int]:
    ensure_tables()
    sources = fetch_sources(enabled_only=True)
    results = {}

    for source in sources:
        source_id = source["id"]
        url = source["url"]
        try:
            items = fetch_rss_feed(url)
            fetched_at = datetime.now(timezone.utc).isoformat()
            count = 0
            for item in items:
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
            results[source_id] = count
        except Exception as e:
            results[source_id] = -1
            print(f"[news] Failed to fetch {url}: {e}")

    delete_old_items(hours=72)
    return results


def add_default_sources() -> None:
    default_sources = [
        ("bbc-world", "BBC World News", "https://feeds.bbci.co.uk/news/world/rss.xml"),
        ("nyt-world", "NYT World", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
        ("hn-front", "Hacker News", "https://hnrss.org/frontpage"),
        ("reddit-finance", "Reddit Finance", "https://www.reddit.com/r/finance/.rss"),
    ]
    from data_sync_service.db.news import create_source
    import uuid

    for sid, name, url in default_sources:
        try:
            create_source(source_id=sid, name=name, url=url, enabled=True)
        except Exception:
            pass