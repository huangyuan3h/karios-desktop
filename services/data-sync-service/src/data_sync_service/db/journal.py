"""Trade journal table (Postgres)."""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "trade_journals"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    content_md  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trade_journals_updated_at ON {TABLE_NAME}(updated_at DESC);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def fetch_all(limit: int = 200, offset: int = 0) -> tuple[int, list[dict[str, Any]]]:
    """
    Return total count and paginated list of journals.
    Returns: (total, items) where items are ordered by updated_at DESC.
    """
    ensure_table()
    lim = max(1, min(int(limit), 200))
    off = max(0, int(offset))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT id, title, content_md, created_at, updated_at
                FROM {TABLE_NAME}
                ORDER BY updated_at DESC
                LIMIT %s OFFSET %s
                """,
                (lim, off),
            )
            rows = cur.fetchall()
    items = [
        {
            "id": str(r[0]),
            "title": str(r[1]),
            "contentMd": str(r[2]),
            "createdAt": str(r[3]),
            "updatedAt": str(r[4]),
        }
        for r in rows
    ]
    return total, items


def fetch_by_id(journal_id: str) -> dict[str, Any] | None:
    """Return a single journal by id, or None if not found."""
    ensure_table()
    jid = (journal_id or "").strip()
    if not jid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, title, content_md, created_at, updated_at
                FROM {TABLE_NAME}
                WHERE id = %s
                """,
                (jid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "id": str(row[0]),
        "title": str(row[1]),
        "contentMd": str(row[2]),
        "createdAt": str(row[3]),
        "updatedAt": str(row[4]),
    }


def create_journal(*, journal_id: str, title: str, content_md: str, created_at: str, updated_at: str) -> dict[str, Any]:
    """Create a new journal entry."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}(id, title, content_md, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (journal_id, title, content_md, created_at, updated_at),
            )
        conn.commit()
    return fetch_by_id(journal_id) or {}


def update_journal(*, journal_id: str, title: str | None = None, content_md: str | None = None, updated_at: str) -> dict[str, Any] | None:
    """Update journal title and/or content. Returns updated journal or None if not found."""
    ensure_table()
    existing = fetch_by_id(journal_id)
    if not existing:
        return None
    next_title = title if title is not None else existing["title"]
    next_content = content_md if content_md is not None else existing["contentMd"]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET title = %s, content_md = %s, updated_at = %s
                WHERE id = %s
                """,
                (next_title, next_content, updated_at, journal_id),
            )
        conn.commit()
    return fetch_by_id(journal_id)


def delete_journal(journal_id: str) -> bool:
    """Delete a journal entry. Returns True if deleted, False if not found."""
    ensure_table()
    jid = (journal_id or "").strip()
    if not jid:
        return False
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (jid,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok
