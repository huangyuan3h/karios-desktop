"""Add sleeve_execution_log for ETF/择强 fill outcomes.

Revision ID: 0037_sleeve_execution_log
Revises: 0036_factor_signals
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from data_sync_service.db.sleeve_execution_log import CREATE_SQL

revision: str = "0037_sleeve_execution_log"
down_revision: str | Sequence[str] | None = "0036_factor_signals"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_sleeve_exec_date;")
    op.execute("DROP TABLE IF EXISTS sleeve_execution_log;")
