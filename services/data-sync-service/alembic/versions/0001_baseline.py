"""Baseline schema snapshot from db/*.py CREATE_SQL constants.

Revision ID: 0001_baseline
Revises:
Create Date: 2026-06-18

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from data_sync_service.db.schema_baseline import baseline_ddl_statements

revision: str = "0001_baseline"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for stmt in baseline_ddl_statements():
        op.execute(stmt)


def downgrade() -> None:
    # Irreversible baseline: existing deployments use `alembic stamp head`.
    pass
