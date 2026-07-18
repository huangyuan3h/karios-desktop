"""Add execution decision journal tables.

Revision ID: 0010_execution_decision_journal
Revises: 0009_drop_unused_daily_indexes
Create Date: 2026-07-18

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from data_sync_service.db.execution_journal import CREATE_SQL

revision: str = "0010_execution_decision_journal"
down_revision: Union[str, Sequence[str], None] = "0009_drop_unused_daily_indexes"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS execution_decision_changes;")
    op.execute("DROP TABLE IF EXISTS execution_snapshots;")
