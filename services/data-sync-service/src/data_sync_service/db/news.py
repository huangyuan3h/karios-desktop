"""News RSS feed storage (Postgres)."""

from __future__ import annotations

import re
from datetime import UTC
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

# HTML cleanup utilities (mirror of service/news.py _strip_html)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_SPACE_RE = re.compile(r"\s+")
_HTML_ENTITIES = {"&amp;": "&", "&lt;": "<", "&gt;": ">", "&nbsp;": " ", "&quot;": '"'}


def _strip_html(text: str) -> str:
    text = _HTML_TAG_RE.sub(" ", text)
    for entity, char in _HTML_ENTITIES.items():
        text = text.replace(entity, char)
    return _MULTI_SPACE_RE.sub(" ", text).strip()

SOURCES_TABLE = "news_sources"
ITEMS_TABLE = "news_items"

CREATE_SOURCES_SQL = f"""
CREATE TABLE IF NOT EXISTS {SOURCES_TABLE} (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL UNIQUE,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_fetch  TEXT,
    created_at  TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'D',
    category    TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_sources_enabled ON {SOURCES_TABLE}(enabled);
CREATE INDEX IF NOT EXISTS idx_news_sources_tier ON {SOURCES_TABLE}(tier);
"""

CREATE_ITEMS_SQL = f"""
CREATE TABLE IF NOT EXISTS {ITEMS_TABLE} (
    id          TEXT PRIMARY KEY,
    source_id   TEXT NOT NULL REFERENCES {SOURCES_TABLE}(id) ON DELETE CASCADE,
    title       TEXT NOT NULL,
    link        TEXT NOT NULL,
    summary     TEXT,
    published_at TEXT,
    fetched_at  TEXT NOT NULL,
    is_read     BOOLEAN NOT NULL DEFAULT FALSE,
    is_important BOOLEAN NOT NULL DEFAULT FALSE,
    -- Track 2: LLM enrichment columns
    tickers         TEXT[],
    sectors         TEXT[],
    event_type      TEXT,
    importance      SMALLINT,
    relevance_score SMALLINT,
    ai_summary      TEXT,
    enrichment_status TEXT,
    enriched_at     TIMESTAMPTZ,
    enrichment_model TEXT,
    actionability   TEXT
);

CREATE INDEX IF NOT EXISTS idx_news_items_published ON {ITEMS_TABLE}(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_items_source ON {ITEMS_TABLE}(source_id);
CREATE INDEX IF NOT EXISTS idx_news_items_fetched ON {ITEMS_TABLE}(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_items_enrichment_status ON {ITEMS_TABLE}(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_news_items_importance ON {ITEMS_TABLE}(importance);
CREATE INDEX IF NOT EXISTS idx_news_items_tickers ON {ITEMS_TABLE} USING gin(tickers);
"""


def _ensure_tables_impl() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SOURCES_SQL)
            cur.execute(CREATE_ITEMS_SQL)
        conn.commit()


def ensure_tables() -> None:
    ensure_once("news", _ensure_tables_impl)


def fetch_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if enabled_only:
                cur.execute(
                    f"SELECT id, name, url, enabled, last_fetch, created_at, tier, category FROM {SOURCES_TABLE} WHERE enabled = TRUE ORDER BY tier, name"
                )
            else:
                cur.execute(
                    f"SELECT id, name, url, enabled, last_fetch, created_at, tier, category FROM {SOURCES_TABLE} ORDER BY tier, name"
                )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "name": str(r[1]),
            "url": str(r[2]),
            "enabled": bool(r[3]),
            "lastFetch": str(r[4]) if r[4] else None,
            "createdAt": str(r[5]),
            "tier": str(r[6]) if r[6] is not None else "D",
            "category": str(r[7]) if r[7] is not None else None,
        }
        for r in rows
    ]


def create_source(
    *,
    source_id: str,
    name: str,
    url: str,
    enabled: bool = True,
    tier: str = "D",
    category: str | None = None,
) -> dict[str, Any]:
    ensure_tables()
    from datetime import datetime

    created_at = datetime.now(UTC).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SOURCES_TABLE}(id, name, url, enabled, created_at, tier, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET name = EXCLUDED.name, enabled = EXCLUDED.enabled, tier = EXCLUDED.tier, category = EXCLUDED.category
                RETURNING id, name, url, enabled, last_fetch, created_at, tier, category
                """,
                (source_id, name, url, enabled, created_at, tier, category),
            )
            row = cur.fetchone()
        conn.commit()
    return {
        "id": str(row[0]),
        "name": str(row[1]),
        "url": str(row[2]),
        "enabled": bool(row[3]),
        "lastFetch": str(row[4]) if row[4] else None,
        "createdAt": str(row[5]),
        "tier": str(row[6]) if row[6] is not None else "D",
        "category": str(row[7]) if row[7] is not None else None,
    }


def update_source(
    *,
    source_id: str,
    name: str | None = None,
    enabled: bool | None = None,
    tier: str | None = None,
    category: str | None = None,
) -> dict[str, Any] | None:
    ensure_tables()
    updates = []
    params = []
    if name is not None:
        updates.append("name = %s")
        params.append(name)
    if enabled is not None:
        updates.append("enabled = %s")
        params.append(enabled)
    if tier is not None:
        updates.append("tier = %s")
        params.append(tier)
    if category is not None:
        updates.append("category = %s")
        params.append(category)
    if not updates:
        return None
    params.append(source_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SOURCES_TABLE} SET {', '.join(updates)} WHERE id = %s
                RETURNING id, name, url, enabled, last_fetch, created_at, tier, category
                """,
                params,
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    return {
        "id": str(row[0]),
        "name": str(row[1]),
        "url": str(row[2]),
        "enabled": bool(row[3]),
        "lastFetch": str(row[4]) if row[4] else None,
        "createdAt": str(row[5]),
        "tier": str(row[6]) if row[6] is not None else "D",
        "category": str(row[7]) if row[7] is not None else None,
    }


def delete_source(source_id: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SOURCES_TABLE} WHERE id = %s", (source_id,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def fetch_items(
    limit: int = 100,
    offset: int = 0,
    source_id: str | None = None,
    is_read: bool | None = None,
    hours: int | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    ensure_tables()
    lim = max(1, min(int(limit), 500))
    off = max(0, int(offset))

    conditions = []
    params = []

    if source_id:
        conditions.append("source_id = %s")
        params.append(source_id)
    if is_read is not None:
        conditions.append("is_read = %s")
        params.append(is_read)
    if hours is not None:
        from datetime import datetime, timedelta

        cutoff = (datetime.now(UTC) - timedelta(hours=hours)).isoformat()
        conditions.append("fetched_at >= %s")
        params.append(cutoff)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {ITEMS_TABLE} {where_clause}", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT id, source_id, title, link, summary, published_at, fetched_at,
                       is_read, is_important,
                       tickers, sectors, event_type, importance, relevance_score,
                       ai_summary, enrichment_status, enriched_at, enrichment_model,
                       actionability
                FROM {ITEMS_TABLE}
                {where_clause}
                ORDER BY COALESCE(published_at, fetched_at) DESC
                LIMIT %s OFFSET %s
                """,
                params + [lim, off],
            )
            rows = cur.fetchall()

    items = [
        {
            "id": str(r[0]),
            "sourceId": str(r[1]),
            "title": str(r[2]),
            "link": str(r[3]),
            "summary": _strip_html(str(r[4])) if r[4] else None,
            "publishedAt": str(r[5]) if r[5] else None,
            "fetchedAt": str(r[6]),
            "isRead": bool(r[7]),
            "isImportant": bool(r[8]),
            "tickers": list(r[9]) if r[9] else None,
            "sectors": list(r[10]) if r[10] else None,
            "eventType": str(r[11]) if r[11] else None,
            "importance": int(r[12]) if r[12] is not None else None,
            "relevanceScore": int(r[13]) if r[13] is not None else None,
            "aiSummary": str(r[14]) if r[14] else None,
            "enrichmentStatus": str(r[15]) if r[15] else None,
            "enrichedAt": str(r[16]) if r[16] else None,
            "enrichmentModel": str(r[17]) if r[17] else None,
            "actionability": str(r[18]) if r[18] else None,
        }
        for r in rows
    ]
    return total, items


def upsert_item(
    *,
    item_id: str,
    source_id: str,
    title: str,
    link: str,
    summary: str | None = None,
    published_at: str | None = None,
    fetched_at: str,
) -> dict[str, Any]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {ITEMS_TABLE}(id, source_id, title, link, summary, published_at, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    summary = COALESCE(EXCLUDED.summary, {ITEMS_TABLE}.summary)
                RETURNING id, source_id, title, link, summary, published_at, fetched_at,
                          is_read, is_important,
                          tickers, sectors, event_type, importance, relevance_score,
                          ai_summary, enrichment_status, enriched_at, enrichment_model
                """,
                (item_id, source_id, title, link, summary, published_at, fetched_at),
            )
            row = cur.fetchone()
        conn.commit()
    return {
        "id": str(row[0]),
        "sourceId": str(row[1]),
        "title": str(row[2]),
        "link": str(row[3]),
        "summary": str(row[4]) if row[4] else None,
        "publishedAt": str(row[5]) if row[5] else None,
        "fetchedAt": str(row[6]),
        "isRead": bool(row[7]),
        "isImportant": bool(row[8]),
        "tickers": list(row[9]) if row[9] else None,
        "sectors": list(row[10]) if row[10] else None,
        "eventType": str(row[11]) if row[11] else None,
        "importance": int(row[12]) if row[12] is not None else None,
        "relevanceScore": int(row[13]) if row[13] is not None else None,
        "aiSummary": str(row[14]) if row[14] else None,
        "enrichmentStatus": str(row[15]) if row[15] else None,
        "enrichedAt": str(row[16]) if row[16] else None,
        "enrichmentModel": str(row[17]) if row[17] else None,
    }


def mark_item_read(item_id: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {ITEMS_TABLE} SET is_read = TRUE WHERE id = %s", (item_id,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def mark_item_important(item_id: str, is_important: bool) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {ITEMS_TABLE} SET is_important = %s WHERE id = %s", (is_important, item_id))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def update_source_last_fetch(source_id: str, fetched_at: str) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {SOURCES_TABLE} SET last_fetch = %s WHERE id = %s", (fetched_at, source_id))
        conn.commit()


def delete_old_items(hours: int = 72) -> int:
    ensure_tables()
    from datetime import datetime, timedelta

    cutoff = (datetime.now(UTC) - timedelta(hours=hours)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ITEMS_TABLE} WHERE fetched_at < %s", (cutoff,))
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted


# ---------------------------------------------------------------------------
# Track 2: LLM enrichment helpers
# ---------------------------------------------------------------------------

def fetch_pending_enrichment(limit: int = 50) -> list[dict[str, Any]]:
    """Return items not yet enriched (enrichment_status IS NULL), newest first."""
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, source_id, title, link, summary, published_at, fetched_at,
                       is_read, is_important,
                       tickers, sectors, event_type, importance, relevance_score,
                       ai_summary, enrichment_status, enriched_at, enrichment_model
                FROM {ITEMS_TABLE}
                WHERE enrichment_status IS NULL
                ORDER BY COALESCE(published_at, fetched_at) DESC
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "sourceId": str(r[1]),
            "title": str(r[2]),
            "link": str(r[3]),
            "summary": str(r[4]) if r[4] else None,
            "publishedAt": str(r[5]) if r[5] else None,
            "fetchedAt": str(r[6]),
            "isRead": bool(r[7]),
            "isImportant": bool(r[8]),
            "tickers": list(r[9]) if r[9] else None,
            "sectors": list(r[10]) if r[10] else None,
            "eventType": str(r[11]) if r[11] else None,
            "importance": int(r[12]) if r[12] is not None else None,
            "relevanceScore": int(r[13]) if r[13] is not None else None,
            "aiSummary": str(r[14]) if r[14] else None,
            "enrichmentStatus": str(r[15]) if r[15] else None,
            "enrichedAt": str(r[16]) if r[16] else None,
            "enrichmentModel": str(r[17]) if r[17] else None,
        }
        for r in rows
    ]


def update_item_enrichment(
    *,
    item_id: str,
    tickers: list[str] | None = None,
    sectors: list[str] | None = None,
    event_type: str | None = None,
    importance: int | None = None,
    relevance_score: int | None = None,
    ai_summary: str | None = None,
    actionability: str | None = None,
    enrichment_status: str = "done",
    enrichment_model: str | None = None,
) -> bool:
    """Write LLM enrichment results back to a news item."""
    ensure_tables()
    from datetime import datetime, timezone

    enriched_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ITEMS_TABLE} SET
                    tickers          = COALESCE(%s, tickers),
                    sectors          = COALESCE(%s, sectors),
                    event_type       = COALESCE(%s, event_type),
                    importance       = COALESCE(%s, importance),
                    relevance_score  = COALESCE(%s, relevance_score),
                    ai_summary       = COALESCE(%s, ai_summary),
                    actionability    = COALESCE(%s, actionability),
                    enrichment_status = %s,
                    enriched_at      = %s,
                    enrichment_model = COALESCE(%s, enrichment_model)
                WHERE id = %s
                """,
                (
                    tickers,
                    sectors,
                    event_type,
                    importance,
                    relevance_score,
                    ai_summary,
                    actionability,
                    enrichment_status,
                    enriched_at,
                    enrichment_model,
                    item_id,
                ),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def count_by_enrichment_status() -> dict[str, int]:
    """Return {status: count} for monitoring / health check."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COALESCE(enrichment_status, 'pending') AS st, COUNT(*)
                FROM {ITEMS_TABLE}
                GROUP BY st
                """
            )
            return {str(r[0]): int(r[1]) for r in cur.fetchall()}


def strip_html_from_existing_items() -> int:
    """One-time cleanup: strip HTML tags from summary of existing items.

    Returns count of updated rows. Safe to run multiple times (idempotent).
    """
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, summary FROM {ITEMS_TABLE} WHERE summary ~ '<[a-z]'")
            rows = cur.fetchall()
            updated = 0
            for row in rows:
                item_id = row[0]
                raw = row[1] or ""
                cleaned = _strip_html(raw)
                if cleaned != raw:
                    cur.execute(
                        f"UPDATE {ITEMS_TABLE} SET summary = %s WHERE id = %s",
                        (cleaned, item_id),
                    )
                    updated += 1
        conn.commit()
    return updated