"""Remove industry/sentiment rows stored on non-trading calendar days (ghost holiday data).

Revision ID: 0005_purge_non_trading_daily
Revises: 0004_top_inst
Create Date: 2026-06-22

Requires trade_calendar to be seeded (sync_trade_calendar) before upgrade deletes rows.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005_purge_non_trading_daily"
down_revision: str | Sequence[str] | None = "0004_top_inst"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        DELETE FROM market_cn_industry_fund_flow_daily d
        WHERE EXISTS (
            SELECT 1
            FROM trade_calendar c
            WHERE c.exchange = 'SSE'
              AND c.cal_date = d.date::date
              AND c.is_open = 0
        );
        """
    )
    op.execute(
        """
        DELETE FROM market_cn_sentiment_daily d
        WHERE EXISTS (
            SELECT 1
            FROM trade_calendar c
            WHERE c.exchange = 'SSE'
              AND c.cal_date = d.date::date
              AND c.is_open = 0
        );
        """
    )


def downgrade() -> None:
    pass
