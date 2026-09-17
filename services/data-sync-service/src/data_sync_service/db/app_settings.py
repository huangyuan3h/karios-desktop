"""Generic key/value app settings (OPT-223, 2026-09-17).

Small store for cross-layer preferences the backend needs while composing
background pushes. Today the only key is ``strategy_mode``: the UI keeps the
selected strategy in localStorage (source of truth for the UI) and mirrors it
here so Bark/notification copy can be written for the strategy the user is
actually watching.
"""

from __future__ import annotations

import logging
from typing import Any

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "app_settings"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    key        TEXT PRIMARY KEY,
    value      JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)


def get_setting(key: str, default: Any = None) -> Any:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {TABLE_NAME} WHERE key = %s", (key,))
            row = cur.fetchone()
    if not row or row[0] is None:
        return default
    return row[0]


def set_setting(key: str, value: Any) -> None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}(key, value, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = now()
                """,
                (key, Json(value)),
            )
        conn.commit()
