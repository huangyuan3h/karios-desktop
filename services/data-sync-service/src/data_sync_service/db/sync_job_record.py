"""Sync job run records: success/fail, last_ts_code on failure, job_type. Used to skip if today ok, resume from failure."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "sync_job_record"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id           SERIAL PRIMARY KEY,
    job_type     TEXT NOT NULL,
    sync_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    success      BOOLEAN NOT NULL,
    last_ts_code TEXT,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS ix_sync_job_record_job_date ON {TABLE_NAME} (job_type, (sync_at::date));
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def get_today_run(job_type: str) -> dict[str, Any] | None:
    """Return today's latest run for job_type, or None. Used to skip if success, or resume from last_ts_code if failed."""
    ensure_table()
    today = _utc_today()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, job_type, sync_at, success, last_ts_code, error_message
                FROM {TABLE_NAME}
                WHERE job_type = %s AND (sync_at AT TIME ZONE 'UTC')::date = %s
                ORDER BY sync_at DESC
                LIMIT 1
                """,
                (job_type, today),
            )
            row = cur.fetchone()
    if not row:
        return None
    cols = ("id", "job_type", "sync_at", "success", "last_ts_code", "error_message")
    rec = dict(zip(cols, row))
    if rec.get("sync_at") and hasattr(rec["sync_at"], "isoformat"):
        rec["sync_at"] = rec["sync_at"].isoformat()
    return rec


def insert_record(
    job_type: str,
    success: bool,
    last_ts_code: str | None = None,
    error_message: str | None = None,
) -> None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (job_type, success, last_ts_code, error_message)
                VALUES (%s, %s, %s, %s)
                """,
                (job_type, success, last_ts_code, error_message),
            )
        conn.commit()
