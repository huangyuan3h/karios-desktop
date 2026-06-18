"""Add tv_capture_jobs queue table.

Revision ID: 0002_tv_capture_jobs
Revises: 0001_baseline
Create Date: 2026-06-18

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from data_sync_service.db.tv_capture_jobs import CREATE_SQL

revision: str = "0002_tv_capture_jobs"
down_revision: Union[str, Sequence[str], None] = "0001_baseline"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tv_capture_jobs;")
