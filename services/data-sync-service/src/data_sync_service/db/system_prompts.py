from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from data_sync_service.db import get_connection

PRESETS_TABLE = "system_prompts"
STATE_TABLE = "system_prompt_state"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PRESETS_TABLE} (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    content     TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_system_prompts_updated_at ON {PRESETS_TABLE}(updated_at DESC);

CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    key                 TEXT PRIMARY KEY,
    active_preset_id    TEXT,
    legacy_content      TEXT NOT NULL DEFAULT '',
    updated_at          TEXT NOT NULL
);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            # Ensure singleton row exists
            cur.execute(
                f"""
                INSERT INTO {STATE_TABLE}(key, active_preset_id, legacy_content, updated_at)
                VALUES ('singleton', NULL, '', %s)
                ON CONFLICT (key) DO NOTHING
                """,
                (_now_iso(),),
            )
        conn.commit()


def list_presets() -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, title, updated_at FROM {PRESETS_TABLE} ORDER BY updated_at DESC")
            rows = cur.fetchall()
    return [{"id": str(r[0]), "title": str(r[1]), "updatedAt": str(r[2])} for r in rows]


def get_preset(preset_id: str) -> dict[str, str] | None:
    ensure_tables()
    pid = (preset_id or "").strip()
    if not pid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT id, title, content FROM {PRESETS_TABLE} WHERE id = %s", (pid,))
            row = cur.fetchone()
    if not row:
        return None
    return {"id": str(row[0]), "title": str(row[1]), "content": str(row[2])}


def create_preset(*, preset_id: str, title: str, content: str) -> None:
    ensure_tables()
    ts = _now_iso()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {PRESETS_TABLE}(id, title, content, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (preset_id, title, content, ts, ts),
            )
        conn.commit()


def update_preset(*, preset_id: str, title: str | None, content: str | None) -> bool:
    ensure_tables()
    pid = (preset_id or "").strip()
    if not pid:
        return False
    existing = get_preset(pid)
    if existing is None:
        return False
    new_title = (title if title is not None else existing["title"]).strip() or "Untitled"
    new_content = content if content is not None else existing["content"]
    ts = _now_iso()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {PRESETS_TABLE}
                SET title = %s, content = %s, updated_at = %s
                WHERE id = %s
                """,
                (new_title, new_content, ts, pid),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def delete_preset(preset_id: str) -> bool:
    ensure_tables()
    pid = (preset_id or "").strip()
    if not pid:
        return False
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {PRESETS_TABLE} WHERE id = %s", (pid,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def get_state() -> dict[str, str | None]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT active_preset_id, legacy_content FROM {STATE_TABLE} WHERE key = 'singleton'"
            )
            row = cur.fetchone()
    if not row:
        return {"activePresetId": None, "legacyContent": ""}
    active = str(row[0]) if row[0] else None
    legacy = str(row[1] or "")
    return {"activePresetId": active, "legacyContent": legacy}


def set_active_preset_id(active_preset_id: str | None) -> None:
    ensure_tables()
    pid = (active_preset_id or "").strip() if active_preset_id is not None else None
    ts = _now_iso()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {STATE_TABLE}
                SET active_preset_id = %s, updated_at = %s
                WHERE key = 'singleton'
                """,
                (pid or None, ts),
            )
        conn.commit()


def set_legacy_content(value: str) -> None:
    ensure_tables()
    ts = _now_iso()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {STATE_TABLE}
                SET legacy_content = %s, updated_at = %s
                WHERE key = 'singleton'
                """,
                (value, ts),
            )
        conn.commit()

