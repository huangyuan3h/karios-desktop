"""System events inbox — unified error/unstable collection (2026-08-28).

Low severity stays in DB only (weekly fix), high severity also emits Bark webhook.
Reuses TRADING_JOB_TYPES from notifications.py as high threshold.
"""

from __future__ import annotations

import json
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "system_events"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,
    severity    TEXT NOT NULL DEFAULT 'low',
    title       TEXT NOT NULL,
    detail      TEXT NOT NULL DEFAULT '',
    payload     JSONB NOT NULL DEFAULT '{{}}',
    dedupe_key  TEXT NOT NULL UNIQUE,
    resolved    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_system_events_sev_created ON {TABLE_NAME} (severity, created_at);
CREATE INDEX IF NOT EXISTS ix_system_events_type ON {TABLE_NAME} (event_type);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


HIGH_JOB_TYPES = {
    "close_sync",
    "stock_close_sync",
    "watchlist_automation",
    "paper_s3_intake_CN",
    "paper_s3_intake_HK",
    "paper_trading_update",
    "paper_chain_watchdog",
    "cn_industry_post_close_sync",
    "index_basic_sync",
}


def severity_for_job(job_type: str) -> str:
    return "high" if job_type in HIGH_JOB_TYPES else "low"


def insert_event(
    *,
    event_type: str,
    severity: str,
    title: str,
    detail: str = "",
    payload: dict[str, Any] | None = None,
    dedupe_key: str,
) -> bool:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (event_type, severity, title, detail, payload, dedupe_key)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (event_type, severity, title, detail, json.dumps(payload or {}), dedupe_key),
                )
            except Exception:
                conn.rollback()
                return False
        conn.commit()
    return True


def list_events(limit: int = 100, include_resolved: bool = False) -> list[dict[str, Any]]:
    ensure_table()
    lim = max(1, min(int(limit), 500))
    with get_connection() as conn:
        with conn.cursor() as cur:
            if include_resolved:
                cur.execute(
                    f"""
                    SELECT id, event_type, severity, title, detail, payload, dedupe_key, resolved, created_at
                    FROM {TABLE_NAME}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (lim,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, event_type, severity, title, detail, payload, dedupe_key, resolved, created_at
                    FROM {TABLE_NAME}
                    WHERE NOT resolved
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (lim,),
                )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "id": r[0],
            "eventType": r[1],
            "severity": r[2],
            "title": r[3],
            "detail": r[4],
            "payload": r[5],
            "dedupeKey": r[6],
            "resolved": bool(r[7]),
            "createdAt": r[8].isoformat() if hasattr(r[8], "isoformat") else str(r[8]),
        })
    return out


def resolve_event(event_id: int) -> bool:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE {TABLE_NAME} SET resolved = TRUE WHERE id = %s", (event_id,))
            ok = cur.rowcount > 0
        conn.commit()
    return ok
