"""Add tv_capture_jobs queue table.

Revision ID: 0002_tv_capture_jobs
Revises: 0001_baseline
Create Date: 2026-06-18

2026-08-12: the TV module was fully retired (data kept, code removed) — the
CREATE_SQL was inlined here so this historical migration still runs without
importing the deleted ``data_sync_service.db.tv_capture_jobs`` module.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_tv_capture_jobs"
down_revision: str | Sequence[str] | None = "0001_baseline"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS tv_capture_jobs (
    id          SERIAL PRIMARY KEY,
    job_type    TEXT NOT NULL DEFAULT 'screener_capture',
    screener_id TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'queued',
    row_count   INTEGER,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tv_capture_jobs;")
