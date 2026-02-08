"""TradingView integration tables: screeners and snapshots (Postgres)."""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection

SCREENERS_TABLE = "tv_screeners"
SNAPSHOTS_TABLE = "tv_screener_snapshots"

# Notes:
# - Keep `id` as TEXT to support default ids like "falcon".
# - Keep `captured_at` as TEXT (ISO-8601) for compatibility with existing data and stable ordering.
# - Store snapshot payload as JSONB to avoid re-encoding/decoding at the DB boundary.

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCREENERS_TABLE} (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {SNAPSHOTS_TABLE} (
    id          TEXT PRIMARY KEY,
    screener_id TEXT NOT NULL REFERENCES {SCREENERS_TABLE}(id) ON DELETE CASCADE,
    captured_at TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    payload     JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tv_screeners_updated_at ON {SCREENERS_TABLE}(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_tv_snapshots_screener_captured ON {SNAPSHOTS_TABLE}(screener_id, captured_at DESC);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def fetch_screeners() -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, name, url, enabled, updated_at
                FROM {SCREENERS_TABLE}
                ORDER BY updated_at DESC
                """,
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "name": str(r[1]),
            "url": str(r[2]),
            "enabled": bool(r[3]),
            "updatedAt": str(r[4]),
        }
        for r in rows
    ]


def fetch_screener_by_id(screener_id: str) -> dict[str, Any] | None:
    ensure_tables()
    sid = (screener_id or "").strip()
    if not sid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, name, url, enabled, updated_at FROM {SCREENERS_TABLE} WHERE id = %s",
                (sid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "id": str(row[0]),
        "name": str(row[1]),
        "url": str(row[2]),
        "enabled": bool(row[3]),
        "updatedAt": str(row[4]),
    }


def upsert_screener(
    *,
    screener_id: str,
    name: str,
    url: str,
    enabled: bool,
    created_at: str,
    updated_at: str,
) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SCREENERS_TABLE}(id, name, url, enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    url = EXCLUDED.url,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                """,
                (screener_id, name, url, bool(enabled), created_at, updated_at),
            )
        conn.commit()


def update_screener(
    *,
    screener_id: str,
    name: str,
    url: str,
    enabled: bool,
    updated_at: str,
) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SCREENERS_TABLE}
                SET name = %s, url = %s, enabled = %s, updated_at = %s
                WHERE id = %s
                """,
                (name, url, bool(enabled), updated_at, screener_id),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def delete_screener(screener_id: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCREENERS_TABLE} WHERE id = %s", (screener_id,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def count_screeners() -> int:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(1) FROM {SCREENERS_TABLE}")
            row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def upsert_snapshot(
    *,
    snapshot_id: str,
    screener_id: str,
    captured_at: str,
    row_count: int,
    payload: dict[str, Any],
) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SNAPSHOTS_TABLE}(id, screener_id, captured_at, row_count, payload)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO UPDATE SET
                    screener_id = EXCLUDED.screener_id,
                    captured_at = EXCLUDED.captured_at,
                    row_count = EXCLUDED.row_count,
                    payload = EXCLUDED.payload
                """,
                (snapshot_id, screener_id, captured_at, int(row_count), payload),
            )
        conn.commit()


def list_snapshots_for_screener(screener_id: str, *, limit: int = 10) -> list[dict[str, Any]]:
    ensure_tables()
    lim = max(1, min(int(limit), 50))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, screener_id, captured_at, row_count
                FROM {SNAPSHOTS_TABLE}
                WHERE screener_id = %s
                ORDER BY captured_at DESC
                LIMIT %s
                """,
                (screener_id, lim),
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "screenerId": str(r[1]),
            "capturedAt": str(r[2]),
            "rowCount": int(r[3]),
        }
        for r in rows
    ]


def list_snapshots_for_screener_full(screener_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
    """
    Return snapshot rows with minimal payload extraction (screenTitle/filters) for history view.
    """
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, screener_id, captured_at, row_count, payload
                FROM {SNAPSHOTS_TABLE}
                WHERE screener_id = %s
                ORDER BY captured_at DESC
                LIMIT %s
                """,
                (screener_id, lim),
            )
            rows = cur.fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        payload = r[4] if isinstance(r[4], dict) else (r[4] or {})
        screen_title = str(payload.get("screenTitle") or "") or None
        filters = payload.get("filters") or []
        filters2 = [str(x) for x in filters if str(x).strip()] if isinstance(filters, list) else []
        out.append(
            {
                "snapshotId": str(r[0]),
                "screenerId": str(r[1]),
                "capturedAt": str(r[2]),
                "rowCount": int(r[3]),
                "screenTitle": screen_title,
                "filters": filters2,
            },
        )
    return out


def fetch_snapshot_detail(snapshot_id: str) -> dict[str, Any] | None:
    ensure_tables()
    sid = (snapshot_id or "").strip()
    if not sid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, screener_id, captured_at, row_count, payload
                FROM {SNAPSHOTS_TABLE}
                WHERE id = %s
                """,
                (sid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    payload = row[4] if isinstance(row[4], dict) else (row[4] or {})
    screen_title = str(payload.get("screenTitle") or "") or None
    filters = payload.get("filters") or []
    filters2 = [str(x) for x in filters if str(x).strip()] if isinstance(filters, list) else []
    headers = payload.get("headers") or []
    headers2 = [str(x) for x in headers] if isinstance(headers, list) else []
    rows0 = payload.get("rows") or []
    rows2 = [{str(k): str(v) for k, v in (r or {}).items()} for r in rows0] if isinstance(rows0, list) else []
    return {
        "id": str(row[0]),
        "screenerId": str(row[1]),
        "capturedAt": str(row[2]),
        "rowCount": int(row[3]),
        "screenTitle": screen_title,
        "filters": filters2,
        "url": str(payload.get("url") or ""),
        "headers": headers2,
        "rows": rows2,
    }

