"""Stock daily valuation snapshot (total_mv / circ_mv / turnover).

Revision ID: 0024_stock_dailybasic
Revises: 0023_user_trades
Create Date: 2026-08-09

Market-cap layering data for the S-3 universe (docs/todo.md §19.2 step 12):
``total_mv``/``circ_mv`` in 10k CNY, ``turnover_rate`` in percent. Fed from
tushare ``daily_basic`` by trade date; used to split the candidate pool by
market cap (large/mid/small) and to enforce a liquidity floor.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0024_stock_dailybasic"
down_revision: str | Sequence[str] | None = "0023_user_trades"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_dailybasic (
            ts_code       TEXT NOT NULL,
            trade_date    TEXT NOT NULL,
            total_mv      DOUBLE PRECISION,
            circ_mv       DOUBLE PRECISION,
            turnover_rate DOUBLE PRECISION,
            PRIMARY KEY (ts_code, trade_date)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_dailybasic_date
        ON stock_dailybasic (trade_date)
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stock_dailybasic")
