"""TradingView Chrome settings (KV store) in Postgres.

This replaces quant-service's SQLite `settings` table for TradingView Chrome management.
"""

from __future__ import annotations

from data_sync_service.db import get_connection

TABLE_NAME = "tv_chrome_settings"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def get_value(key: str) -> str | None:
    ensure_table()
    k = (key or "").strip()
    if not k:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT value FROM {TABLE_NAME} WHERE key = %s", (k,))
            row = cur.fetchone()
    return None if not row else (str(row[0]) if row[0] is not None else None)


def set_value(*, key: str, value: str, updated_at: str) -> None:
    ensure_table()
    k = (key or "").strip()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}(key, value, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                (k, str(value), updated_at),
            )
        conn.commit()

