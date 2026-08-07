"""Decision agent loop storage (TIP-015).

Persists decision-agent sessions/messages server-side (unlike the generic
chat which lives in localStorage), plus daily decision snapshots for the
10-day archive layer and outcome feedback.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from data_sync_service.db import get_connection

SESSIONS_TABLE = "decision_sessions"
MESSAGES_TABLE = "decision_messages"
SNAPSHOTS_TABLE = "decision_snapshots"
ACTIONS_TABLE = "decision_actions"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SESSIONS_TABLE} (
    id             BIGSERIAL PRIMARY KEY,
    title          TEXT,
    model_profile  TEXT,
    system_prompt  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
    id               BIGSERIAL PRIMARY KEY,
    session_id       BIGINT NOT NULL REFERENCES {SESSIONS_TABLE}(id) ON DELETE CASCADE,
    role             TEXT NOT NULL,
    content          TEXT NOT NULL DEFAULT '',
    context_snapshot JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_decision_messages_session
    ON {MESSAGES_TABLE} (session_id, created_at);
CREATE TABLE IF NOT EXISTS {SNAPSHOTS_TABLE} (
    id               BIGSERIAL PRIMARY KEY,
    snapshot_date    DATE NOT NULL UNIQUE,
    active_layer_ref JSONB,
    agent_exchanges  JSONB,
    outcome          JSONB,
    status           TEXT NOT NULL DEFAULT 'open',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {ACTIONS_TABLE} (
    id                 BIGSERIAL PRIMARY KEY,
    session_id         BIGINT,
    message_id         BIGINT,
    symbol             TEXT NOT NULL,
    action             TEXT NOT NULL,
    rationale          TEXT,
    confidence         DOUBLE PRECISION,
    status             TEXT NOT NULL DEFAULT 'proposed',
    source             TEXT NOT NULL DEFAULT 'decision_agent',
    snapshot_date      DATE,
    matched_change_id  TEXT,
    outcome            JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_decision_actions_created
    ON {ACTIONS_TABLE} (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_decision_actions_symbol
    ON {ACTIONS_TABLE} (symbol);
CREATE INDEX IF NOT EXISTS ix_decision_actions_message
    ON {ACTIONS_TABLE} (message_id);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_session(
    *,
    title: str | None = None,
    model_profile: str | None = None,
    system_prompt: str | None = None,
) -> dict[str, Any]:
    ensure_table()
    now = _now_iso()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SESSIONS_TABLE} (title, model_profile, system_prompt, last_active_at)
                VALUES (%s, %s, %s, %s)
                RETURNING id, title, model_profile, system_prompt, created_at, last_active_at
                """,
                (title, model_profile, system_prompt, now),
            )
            row = cur.fetchone()
        conn.commit()
    cols = ("id", "title", "model_profile", "system_prompt", "created_at", "last_active_at")
    rec: dict[str, Any] = dict(zip(cols, row, strict=False))
    for key in ("created_at", "last_active_at"):
        if rec.get(key) and hasattr(rec[key], "isoformat"):
            rec[key] = rec[key].isoformat()
    return rec


def list_sessions(limit: int = 50) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, title, model_profile, system_prompt, created_at, last_active_at,
                       (SELECT COUNT(*) FROM {MESSAGES_TABLE} m WHERE m.session_id = s.id) AS message_count
                FROM {SESSIONS_TABLE} s
                ORDER BY last_active_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    cols = ("id", "title", "model_profile", "system_prompt", "created_at", "last_active_at", "message_count")
    return [_row_dict(cols, r) for r in rows]


def get_session(session_id: int) -> dict[str, Any] | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, title, model_profile, system_prompt, created_at, last_active_at FROM {SESSIONS_TABLE} WHERE id = %s",
                (session_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    cols = ("id", "title", "model_profile", "system_prompt", "created_at", "last_active_at")
    return _row_dict(cols, row)


def update_session_title(session_id: int, title: str) -> dict[str, Any] | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SESSIONS_TABLE} SET title = %s, last_active_at = now() WHERE id = %s RETURNING id, title",
                (title, session_id),
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    return {"id": row[0], "title": row[1]}


def update_session_settings(
    session_id: int,
    *,
    title: str | None = None,
    system_prompt: str | None = None,
) -> dict[str, Any] | None:
    ensure_table()
    fields: list[str] = ["last_active_at = now()"]
    params: list[Any] = []
    if title is not None:
        fields.append("title = %s")
        params.append(title)
    if system_prompt is not None:
        fields.append("system_prompt = %s")
        params.append(system_prompt)
    params.append(session_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SESSIONS_TABLE} SET {', '.join(fields)} WHERE id = %s RETURNING id, title",
                tuple(params),
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    return {"id": row[0], "title": row[1]}


def touch_session(session_id: int) -> None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {SESSIONS_TABLE} SET last_active_at = now() WHERE id = %s",
                (session_id,),
            )
        conn.commit()


def list_messages(session_id: int, limit: int = 200) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, session_id, role, content, context_snapshot, created_at
                FROM {MESSAGES_TABLE}
                WHERE session_id = %s
                ORDER BY created_at ASC, id ASC
                LIMIT %s
                """,
                (session_id, limit),
            )
            rows = cur.fetchall()
    cols = ("id", "session_id", "role", "content", "context_snapshot", "created_at")
    return [_row_dict(cols, r) for r in rows]


def append_message(
    session_id: int,
    *,
    role: str,
    content: str,
    context_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {MESSAGES_TABLE} (session_id, role, content, context_snapshot)
                VALUES (%s, %s, %s, %s)
                RETURNING id, session_id, role, content, context_snapshot, created_at
                """,
                (session_id, role, content, None if context_snapshot is None else json.dumps(context_snapshot)),
            )
            row = cur.fetchone()
        conn.commit()
    cols = ("id", "session_id", "role", "content", "context_snapshot", "created_at")
    return _row_dict(cols, row)


def upsert_snapshot(
    *,
    snapshot_date: date,
    active_layer_ref: dict[str, Any] | None = None,
    agent_exchanges: list[dict[str, Any]] | None = None,
    outcome: dict[str, Any] | None = None,
    status: str = "open",
) -> dict[str, Any]:
    ensure_table()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SNAPSHOTS_TABLE} (snapshot_date, active_layer_ref, agent_exchanges, outcome, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date) DO UPDATE SET
                    active_layer_ref = COALESCE(EXCLUDED.active_layer_ref, {SNAPSHOTS_TABLE}.active_layer_ref),
                    agent_exchanges = COALESCE(EXCLUDED.agent_exchanges, {SNAPSHOTS_TABLE}.agent_exchanges),
                    outcome = COALESCE(EXCLUDED.outcome, {SNAPSHOTS_TABLE}.outcome),
                    status = EXCLUDED.status
                RETURNING id, snapshot_date, status, created_at
                """,
                (
                    snapshot_date,
                    None if active_layer_ref is None else json.dumps(active_layer_ref),
                    None if agent_exchanges is None else json.dumps(agent_exchanges),
                    None if outcome is None else json.dumps(outcome),
                    status,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    cols = ("id", "snapshot_date", "status", "created_at")
    return _row_dict(cols, row)


def upsert_actions(actions: list[dict[str, Any]]) -> int:
    """Insert extracted actions; replaces any existing actions for the same
    message_id (idempotent re-extraction)."""
    ensure_table()
    if not actions:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {ACTIONS_TABLE} WHERE message_id = %s",
                (actions[0].get("message_id"),),
            )
            for a in actions:
                cur.execute(
                    f"""
                    INSERT INTO {ACTIONS_TABLE}
                        (session_id, message_id, symbol, action, rationale, confidence,
                         status, source, snapshot_date)
                    VALUES (%s, %s, %s, %s, %s, %s, 'proposed', %s, %s)
                    """,
                    (
                        a.get("session_id"),
                        a.get("message_id"),
                        a.get("symbol"),
                        a.get("action"),
                        a.get("rationale"),
                        a.get("confidence"),
                        a.get("source", "decision_agent"),
                        a.get("snapshot_date"),
                    ),
                )
        conn.commit()
    return len(actions)


def list_actions(
    *,
    status: str | None = None,
    days: int = 30,
    limit: int = 200,
) -> list[dict[str, Any]]:
    ensure_table()
    clauses: list[str] = [f"created_at >= now() - INTERVAL '%s days'"]
    params: list[Any] = [days]
    if status:
        clauses.append("status = %s")
        params.append(status)
    params.append(limit)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, session_id, message_id, symbol, action, rationale, confidence,
                       status, source, snapshot_date, matched_change_id, outcome, created_at
                FROM {ACTIONS_TABLE}
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    cols = (
        "id", "session_id", "message_id", "symbol", "action", "rationale", "confidence",
        "status", "source", "snapshot_date", "matchedChangeId", "outcome", "createdAt",
    )
    out: list[dict[str, Any]] = []
    for r in rows:
        rec: dict[str, Any] = {}
        for key, value in zip(cols, r, strict=False):
            if key == "createdAt" and value is not None and hasattr(value, "isoformat"):
                value = value.isoformat()
            if key == "snapshot_date" and value is not None and hasattr(value, "isoformat"):
                value = value.isoformat()
            rec[key] = value
        out.append(rec)
    return out


def update_action_status(
    action_id: int,
    *,
    status: str,
    matched_change_id: str | None = None,
    outcome: dict[str, Any] | None = None,
) -> bool:
    ensure_table()
    fields: list[str] = ["status = %s"]
    params: list[Any] = [status]
    if matched_change_id is not None:
        fields.append("matched_change_id = %s")
        params.append(matched_change_id)
    if outcome is not None:
        fields.append("outcome = %s")
        params.append(json.dumps(outcome))
    params.append(action_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {ACTIONS_TABLE} SET {', '.join(fields)} WHERE id = %s RETURNING id",
                tuple(params),
            )
            row = cur.fetchone()
        conn.commit()
    return row is not None


def delete_message(session_id: int, message_id: int) -> bool:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {MESSAGES_TABLE} WHERE id = %s AND session_id = %s",
                (message_id, session_id),
            )
            deleted = cur.rowcount
        conn.commit()
    return deleted > 0


def list_snapshots(limit: int = 30) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, snapshot_date, active_layer_ref, agent_exchanges, outcome, status, created_at
                FROM {SNAPSHOTS_TABLE}
                ORDER BY snapshot_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    cols = ("id", "snapshot_date", "active_layer_ref", "agent_exchanges", "outcome", "status", "created_at")
    return [_row_dict(cols, r) for r in rows]


def _row_dict(cols: tuple[str, ...], row: tuple[Any, ...]) -> dict[str, Any]:
    """Map DB row to camelCase dict (API convention)."""
    camel_map = {
        "created_at": "createdAt",
        "last_active_at": "lastActiveAt",
        "session_id": "sessionId",
        "context_snapshot": "contextSnapshot",
        "snapshot_date": "snapshotDate",
        "active_layer_ref": "activeLayerRef",
        "agent_exchanges": "agentExchanges",
        "message_count": "messageCount",
    }
    rec: dict[str, Any] = {}
    for key, value in zip(cols, row, strict=False):
        out_key = camel_map.get(key, key)
        if out_key in ("createdAt", "lastActiveAt") and value is not None and hasattr(value, "isoformat"):
            value = value.isoformat()
        rec[out_key] = value
    return rec
