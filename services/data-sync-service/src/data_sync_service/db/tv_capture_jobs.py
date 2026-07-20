"""TradingView screener capture job queue (Postgres)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "tv_capture_jobs"

TERMINAL_STATUSES = frozenset({"done", "failed", "cancelled"})
ACTIVE_STATUSES = frozenset({"queued", "running"})

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id              TEXT PRIMARY KEY,
    screener_id     TEXT NOT NULL,
    status          TEXT NOT NULL,
    trigger_source  TEXT NOT NULL DEFAULT 'api',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    snapshot_id     TEXT,
    row_count       INTEGER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_tv_capture_jobs_screener_created
    ON {TABLE_NAME}(screener_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_tv_capture_jobs_status_created
    ON {TABLE_NAME}(status, created_at ASC);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _row_to_dict(row: tuple[Any, ...], columns: list[str]) -> dict[str, Any]:
    out = dict(zip(columns, row, strict=True))
    for key in ("created_at", "started_at", "finished_at"):
        val = out.get(key)
        if isinstance(val, datetime):
            out[key] = val.isoformat()
    return out


_JOB_COLUMNS = [
    "id",
    "screener_id",
    "status",
    "trigger_source",
    "created_at",
    "started_at",
    "finished_at",
    "snapshot_id",
    "row_count",
    "error_message",
]


def _select_job(cur, job_id: str) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT id, screener_id, status, trigger_source, created_at, started_at,
               finished_at, snapshot_id, row_count, error_message
        FROM {TABLE_NAME}
        WHERE id = %s
        """,
        (job_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return _row_to_dict(row, _JOB_COLUMNS)


def reset_stale_running_jobs(*, older_than_minutes: int = 30) -> int:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET status = 'queued',
                    started_at = NULL,
                    error_message = COALESCE(error_message, 'requeued after worker restart')
                WHERE status = 'running'
                  AND started_at < now() - (%s * interval '1 minute')
                """,
                (int(older_than_minutes),),
            )
            count = cur.rowcount
        conn.commit()
    return int(count or 0)


def find_active_job_for_screener(screener_id: str) -> dict[str, Any] | None:
    ensure_table()
    sid = (screener_id or "").strip()
    if not sid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, screener_id, status, trigger_source, created_at, started_at,
                       finished_at, snapshot_id, row_count, error_message
                FROM {TABLE_NAME}
                WHERE screener_id = %s
                  AND status IN ('queued', 'running')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (sid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _row_to_dict(row, _JOB_COLUMNS)


def insert_job(*, screener_id: str, trigger_source: str = "api") -> dict[str, Any]:
    ensure_table()
    job_id = str(uuid.uuid4())
    sid = (screener_id or "").strip()
    trigger = (trigger_source or "api").strip() or "api"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (
                    id, screener_id, status, trigger_source, created_at
                ) VALUES (%s, %s, 'queued', %s, %s)
                """,
                (job_id, sid, trigger, _now_utc()),
            )
        conn.commit()
    job = get_job(job_id)
    assert job is not None
    return job


def enqueue_or_get_active(*, screener_id: str, trigger_source: str = "api") -> dict[str, Any]:
    existing = find_active_job_for_screener(screener_id)
    if existing is not None:
        return existing
    return insert_job(screener_id=screener_id, trigger_source=trigger_source)


def claim_next_jobs(*, limit: int = 1) -> list[dict[str, Any]]:
    ensure_table()
    lim = max(1, min(int(limit), 10))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH picked AS (
                    SELECT id
                    FROM {TABLE_NAME}
                    WHERE status = 'queued'
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE {TABLE_NAME} AS j
                SET status = 'running',
                    started_at = COALESCE(j.started_at, now())
                FROM picked
                WHERE j.id = picked.id
                RETURNING j.id, j.screener_id, j.status, j.trigger_source, j.created_at,
                          j.started_at, j.finished_at, j.snapshot_id, j.row_count, j.error_message
                """,
                (lim,),
            )
            rows = cur.fetchall()
        conn.commit()
    return [_row_to_dict(r, _JOB_COLUMNS) for r in rows]


def mark_done(*, job_id: str, snapshot_id: str, row_count: int) -> None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET status = 'done',
                    finished_at = %s,
                    snapshot_id = %s,
                    row_count = %s,
                    error_message = NULL
                WHERE id = %s
                """,
                (_now_utc(), snapshot_id, int(row_count), job_id),
            )
        conn.commit()


def mark_failed(*, job_id: str, error_message: str) -> None:
    ensure_table()
    msg = (error_message or "unknown error").strip()[:2000]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET status = 'failed',
                    finished_at = %s,
                    error_message = %s
                WHERE id = %s
                """,
                (_now_utc(), msg, job_id),
            )
        conn.commit()


def get_job(job_id: str) -> dict[str, Any] | None:
    ensure_table()
    jid = (job_id or "").strip()
    if not jid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            return _select_job(cur, jid)


def list_jobs(*, screener_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    ensure_table()
    lim = max(1, min(int(limit), 100))
    sid = (screener_id or "").strip() if screener_id else ""
    with get_connection() as conn:
        with conn.cursor() as cur:
            if sid:
                cur.execute(
                    f"""
                    SELECT id, screener_id, status, trigger_source, created_at, started_at,
                           finished_at, snapshot_id, row_count, error_message
                    FROM {TABLE_NAME}
                    WHERE screener_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (sid, lim),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, screener_id, status, trigger_source, created_at, started_at,
                           finished_at, snapshot_id, row_count, error_message
                    FROM {TABLE_NAME}
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (lim,),
                )
            rows = cur.fetchall()
    return [_row_to_dict(r, _JOB_COLUMNS) for r in rows]
