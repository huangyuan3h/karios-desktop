"""Postgres availability helpers for integration tests."""

from __future__ import annotations

import os


def postgres_available() -> bool:
    """Return True when Postgres is reachable and DB tests are not skipped."""
    if os.getenv("SKIP_DB_TESTS", "").lower() in {"1", "true", "yes"}:
        return False
    try:
        from data_sync_service.db import check_db  # type: ignore[import-not-found]
    except Exception:
        return False
    ok, _ = check_db()
    return bool(ok)
