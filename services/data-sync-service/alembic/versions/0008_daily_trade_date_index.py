"""Add trade_date indexes on daily and index_daily.

Revision ID: 0008_daily_trade_date_index
Revises: 0007_industry_fund_flow_taxonomy
Create Date: 2026-06-27

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0008_daily_trade_date_index"
down_revision: Union[str, Sequence[str], None] = "0007_industry_fund_flow_taxonomy"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily (trade_date DESC)")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_index_daily_trade_date ON index_daily (trade_date DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_index_daily_trade_date")
    op.execute("DROP INDEX IF EXISTS idx_daily_trade_date")
