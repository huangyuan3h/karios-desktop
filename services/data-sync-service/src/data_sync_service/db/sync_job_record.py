"""Sync job run records: success/fail, last_ts_code on failure, job_type. Used to skip if today ok, resume from failure."""

from __future__ import annotations

from datetime import UTC, date, datetime
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
CREATE INDEX IF NOT EXISTS ix_sync_job_record_job_sync ON {TABLE_NAME} (job_type, sync_at);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _utc_today() -> date:
    return datetime.now(UTC).date()


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
    rec: dict[str, Any] = dict(zip(cols, row, strict=False))
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
    if not success:
        from data_sync_service.db.system_events import insert_event, severity_for_job
        from data_sync_service.db.webhook import emit_event

        sev = severity_for_job(job_type)
        dedupe = f"job_failed:{job_type}:{datetime.now(UTC).date().isoformat()}"
        insert_event(
            event_type="job_failed",
            severity=sev,
            title=f"任务失败 · {job_type}",
            detail=(error_message or "unknown error")[:500],
            payload={"job_type": job_type, "error": error_message or "unknown error", "last_ts_code": last_ts_code},
            dedupe_key=dedupe,
        )
        # OPT-144: peripheral (low-severity) jobs don't page the phone on a
        # single failure — every failure is still in system_events + the hub
        # digest. A 3-streak (or any high-severity failure) emits.
        streak = consec_failures(job_type) if sev != "high" else 1
        if sev == "high" or streak >= _PERIPHERAL_STREAK_EMIT:
            emit_event(
                "job_failed",
                {
                    "job_type": job_type,
                    "error": error_message or "unknown error",
                    "last_ts_code": last_ts_code,
                    "streak": streak,
                },
                dedupe_key=dedupe,
            )


_PERIPHERAL_STREAK_EMIT = 3


def consec_failures(job_type: str, *, limit: int = 10) -> int:
    """Trailing consecutive-failure count for job_type (newest first).

    Counts failure rows back to (excluding) the latest success, capped at
    ``limit``. Used by OPT-144 to page peripheral jobs only on a streak.
    Never raises (0 on error).
    """
    try:
        ensure_table()
        lim = max(1, min(int(limit), 30))
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT success FROM {TABLE_NAME}
                    WHERE job_type = %s
                    ORDER BY sync_at DESC, id DESC
                    LIMIT %s
                    """,
                    (job_type, lim),
                )
                rows = cur.fetchall()
    except Exception:  # noqa: BLE001
        return 0
    streak = 0
    for r in rows:
        ok = r[0] if not isinstance(r, dict) else r.get("success")
        if ok:
            break
        streak += 1
    return streak


def get_last_success(job_type: str) -> dict[str, Any] | None:
    """Return the latest successful run for job_type, or None."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, job_type, sync_at, success, last_ts_code, error_message
                FROM {TABLE_NAME}
                WHERE job_type = %s AND success = TRUE
                ORDER BY sync_at DESC
                LIMIT 1
                """,
                (job_type,),
            )
            row = cur.fetchone()
    if not row:
        return None
    cols = ("id", "job_type", "sync_at", "success", "last_ts_code", "error_message")
    rec: dict[str, Any] = dict(zip(cols, row, strict=False))
    if rec.get("sync_at") and hasattr(rec["sync_at"], "isoformat"):
        rec["sync_at"] = rec["sync_at"].isoformat()
    return rec


def get_last_successful_run(job_type: str) -> dict[str, Any] | None:
    """Alias for get_last_success for compatibility."""
    return get_last_success(job_type)


def _record_from_row(row: Any) -> dict[str, Any]:
    cols = ("id", "job_type", "sync_at", "success", "last_ts_code", "error_message")
    rec: dict[str, Any] = dict(zip(cols, row, strict=False))
    if rec.get("sync_at") and hasattr(rec["sync_at"], "isoformat"):
        rec["sync_at"] = rec["sync_at"].isoformat()
    return rec


def list_recent_failures(hours: int = 24) -> list[dict[str, Any]]:
    """Return failed sync job records from the last `hours` (newest first)."""
    ensure_table()
    hours = max(1, min(int(hours), 24 * 7))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, job_type, sync_at, success, last_ts_code, error_message
                FROM {TABLE_NAME}
                WHERE success = FALSE AND sync_at >= now() - make_interval(hours => %s)
                ORDER BY sync_at DESC
                """,
                (hours,),
            )
            rows = cur.fetchall()
    return [_record_from_row(r) for r in rows]


def list_recent_runs(job_type: str, limit: int = 7) -> list[dict[str, Any]]:
    """Return latest run records for a job_type (newest first).

    Used by the funnel health monitor to detect consecutive-anomaly streaks
    across trading days.
    """
    ensure_table()
    lim = max(1, min(int(limit), 30))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, job_type, sync_at, success, last_ts_code, error_message
                FROM {TABLE_NAME}
                WHERE job_type = %s
                ORDER BY sync_at DESC
                LIMIT %s
                """,
                (job_type, lim),
            )
            rows = cur.fetchall()
    return [_record_from_row(r) for r in rows]
