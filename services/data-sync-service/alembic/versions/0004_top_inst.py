"""Add market_top_inst_daily and market_top_inst_summary tables.

Revision ID: 0004_top_inst
Revises: 0003_etf_fund_flow
Create Date: 2026-06-19

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from data_sync_service.db.top_inst import CREATE_SQL

revision: str = "0004_top_inst"
down_revision: Union[str, Sequence[str], None] = "0003_etf_fund_flow"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS market_top_inst_summary;")
    op.execute("DROP TABLE IF EXISTS market_top_inst_daily;")
