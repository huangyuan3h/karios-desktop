"""Add market_etf_fund_flow_daily table.

Revision ID: 0003_etf_fund_flow
Revises: 0002_tv_capture_jobs
Create Date: 2026-06-19

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from data_sync_service.db.etf_fund_flow import CREATE_SQL

revision: str = "0003_etf_fund_flow"
down_revision: Union[str, Sequence[str], None] = "0002_tv_capture_jobs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS market_etf_fund_flow_daily;")
