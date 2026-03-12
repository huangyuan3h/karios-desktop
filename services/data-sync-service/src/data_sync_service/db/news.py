"""News RSS feed storage (Postgres)."""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection

SOURCES_TABLE = "news_sources"
ITEMS_TABLE = "news_items"

CREATE_SOURCES_SQL = f"""
CREATE TABLE IF NOT EXISTS {SOURCES_TABLE} (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL UNIQUE,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    last_fetch  TEXT,
    created_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_news_sources_enabled ON {SOURCES_TABLE}(enabled);
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
    is_important BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_news_items_published ON {ITEMS_TABLE}(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_items_source ON {ITEMS_TABLE}(source_id);
CREATE INDEX IF NOT EXISTS idx_news_items_fetched ON {ITEMS_TABLE}(fetched_at DESC);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SOURCES_SQL)
            cur.execute(CREATE_ITEMS_SQL)
        conn.commit()


def fetch_sources(enabled_only: bool = True) -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if enabled_only:
                cur.execute(
                    f"SELECT id, name, url, enabled, last_fetch, created_at FROM {SOURCES_TABLE} WHERE enabled = TRUE ORDER BY name"
                )
            else:
                cur.execute(
                    f"SELECT id, name, url, enabled, last_fetch, created_at FROM {SOURCES_TABLE} ORDER BY name"
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
        }
        for r in rows
    ]


def create_source(*, source_id: str, name: str, url: str, enabled: bool = True) -> dict[str, Any]:
    ensure_tables()
    from datetime import datetime, timezone

    created_at = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SOURCES_TABLE}(id, name, url, enabled, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (url) DO UPDATE SET name = EXCLUDED.name, enabled = EXCLUDED.enabled
                RETURNING id, name, url, enabled, last_fetch, created_at
                """,
                (source_id, name, url, enabled, created_at),
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
    }


def update_source(*, source_id: str, name: str | None = None, enabled: bool | None = None) -> dict[str, Any] | None:
    ensure_tables()
    updates = []
    params = []
    if name is not None:
        updates.append("name = %s")
        params.append(name)
    if enabled is not None:
        updates.append("enabled = %s")
        params.append(enabled)
    if not updates:
        return None
    params.append(source_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SOURCES_TABLE} SET {', '.join(updates)} WHERE id = %s
                RETURNING id, name, url, enabled, last_fetch, created_at
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
        from datetime import datetime, timezone, timedelta

        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        conditions.append("fetched_at >= %s")
        params.append(cutoff)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {ITEMS_TABLE} {where_clause}", params)
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT id, source_id, title, link, summary, published_at, fetched_at, is_read, is_important
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
            "summary": str(r[4]) if r[4] else None,
            "publishedAt": str(r[5]) if r[5] else None,
            "fetchedAt": str(r[6]),
            "isRead": bool(r[7]),
            "isImportant": bool(r[8]),
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
                RETURNING id, source_id, title, link, summary, published_at, fetched_at, is_read, is_important
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
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ITEMS_TABLE} WHERE fetched_at < %s", (cutoff,))
            deleted = cur.rowcount or 0
        conn.commit()
    return deleted