"""Morning Brief storage (Postgres).

Track 3 of News Substrate 2.0 — selected top 5–7 enriched news items
for morning (08:30) and midday (12:30) briefings.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

BRIEFS_TABLE = "morning_briefs"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {BRIEFS_TABLE} (
    id              TEXT PRIMARY KEY,
    brief_date      TEXT NOT NULL,
    brief_type      TEXT NOT NULL,
    items           JSONB NOT NULL DEFAULT '[]'::jsonb,
    macro_overview  TEXT,
    model_version   TEXT,
    source_item_ids TEXT[],
    markdown        TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(brief_date, brief_type)
);

CREATE INDEX IF NOT EXISTS idx_morning_briefs_date ON {BRIEFS_TABLE}(brief_date DESC);
CREATE INDEX IF NOT EXISTS idx_morning_briefs_type ON {BRIEFS_TABLE}(brief_type);
"""


def _ensure_tables_impl() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def ensure_tables() -> None:
    ensure_once("morning_briefs", _ensure_tables_impl)


def upsert_brief(
    *,
    brief_date: str,
    brief_type: str,
    items: list[dict[str, Any]],
    macro_overview: str | None = None,
    model_version: str | None = None,
    source_item_ids: list[str] | None = None,
    markdown: str | None = None,
) -> dict[str, Any]:
    """Insert or update a brief (news or trading-session) for a given date."""
    ensure_tables()
    import json

    now = datetime.now(UTC).isoformat()
    brief_id = f"{brief_date}-{brief_type}"
    items_json = json.dumps(items, ensure_ascii=False)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {BRIEFS_TABLE}(id, brief_date, brief_type, items, macro_overview, model_version, source_item_ids, markdown, created_at)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
                ON CONFLICT (brief_date, brief_type) DO UPDATE SET
                    items = EXCLUDED.items,
                    macro_overview = EXCLUDED.macro_overview,
                    model_version = EXCLUDED.model_version,
                    source_item_ids = EXCLUDED.source_item_ids,
                    markdown = EXCLUDED.markdown,
                    created_at = EXCLUDED.created_at
                RETURNING id, brief_date, brief_type, items, macro_overview, model_version, source_item_ids, markdown, created_at
                """,
                (brief_id, brief_date, brief_type, items_json, macro_overview, model_version, source_item_ids, markdown, now),
            )
            row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row)


def fetch_brief(brief_date: str, brief_type: str) -> dict[str, Any] | None:
    """Fetch a single brief by date + type."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, brief_date, brief_type, items, macro_overview, model_version, source_item_ids, markdown, created_at
                FROM {BRIEFS_TABLE}
                WHERE brief_date = %s AND brief_type = %s
                """,
                (brief_date, brief_type),
            )
            row = cur.fetchone()
    return _row_to_dict(row) if row else None


def fetch_latest_brief(brief_type: str | None = None) -> dict[str, Any] | None:
    """Fetch the most recent brief, optionally filtered by type."""
    ensure_tables()
    condition = "WHERE brief_type = %s" if brief_type else ""
    params: tuple[str, ...] = (brief_type,) if brief_type else ()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, brief_date, brief_type, items, macro_overview, model_version, source_item_ids, markdown, created_at
                FROM {BRIEFS_TABLE}
                {condition}
                ORDER BY brief_date DESC, created_at DESC
                LIMIT 1
                """,
                params,
            )
            row = cur.fetchone()
    return _row_to_dict(row) if row else None


def fetch_recent_briefs(limit: int = 7) -> list[dict[str, Any]]:
    """Fetch the most recent N briefs, newest first."""
    ensure_tables()
    lim = max(1, min(int(limit), 30))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, brief_date, brief_type, items, macro_overview, model_version, source_item_ids, markdown, created_at
                FROM {BRIEFS_TABLE}
                ORDER BY brief_date DESC, created_at DESC
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(row: tuple) -> dict[str, Any]:
    import json

    items_raw = row[3]
    if isinstance(items_raw, str):
        try:
            items_raw = json.loads(items_raw)
        except (json.JSONDecodeError, TypeError):
            items_raw = []
    elif items_raw is None:
        items_raw = []

    return {
        "id": str(row[0]),
        "briefDate": str(row[1]),
        "briefType": str(row[2]),
        "items": items_raw,
        "macroOverview": str(row[4]) if row[4] else None,
        "modelVersion": str(row[5]) if row[5] else None,
        "sourceItemIds": list(row[6]) if row[6] else None,
        "markdown": str(row[7]) if len(row) > 7 and row[7] else None,
        "createdAt": str(row[8] if len(row) > 8 else row[7]),
    }
