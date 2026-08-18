from __future__ import annotations

import psycopg

from data_sync_service.config import get_settings


def get_connection() -> psycopg.Connection:
    settings = get_settings()
    if not settings.database_url:
        raise ValueError("DATABASE_URL is not configured.")
    # statement_timeout guards against a hung SQL statement (lock waits,
    # deadlocks) permanently blocking a scheduler thread. 120s is long enough
    # for the largest batch upserts but bounds pathological statements.
    options = "-c statement_timeout=120000"
    return psycopg.connect(settings.database_url, connect_timeout=5, options=options)


def check_db() -> tuple[bool, str | None]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
