from __future__ import annotations

from typing import Tuple

import psycopg

from data_sync_service.config import get_settings


def get_connection() -> psycopg.Connection:
    settings = get_settings()
    if not settings.database_url:
        raise ValueError("DATABASE_URL is not configured.")
    return psycopg.connect(settings.database_url, connect_timeout=5)


def check_db() -> Tuple[bool, str | None]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
