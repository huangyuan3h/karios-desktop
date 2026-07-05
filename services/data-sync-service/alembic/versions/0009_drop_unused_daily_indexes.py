"""Replace rarely-used B-tree indexes on daily / index_daily.trade_date with BRIN.

Revision ID: 0009_drop_unused_daily_indexes
Revises: 0008_daily_trade_date_index
Create Date: 2026-07-05

Background:
- idx_daily_trade_date (B-tree on daily.trade_date DESC) was 120 MB and served
  only count_rows_for_trade_date (used ~2 times since stats reset).
- idx_index_daily_trade_date (B-tree on index_daily.trade_date DESC) had
  no production hot path at all (only 7 historical lookups).
- Hot paths all carry WHERE ts_code = ? and naturally use the composite PK
  (ts_code, trade_date); the single-column trade_date B-tree is redundant.
- BRIN retains "scan by date range" capability at <1 MB cost (vs 120 MB).
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0009_drop_unused_daily_indexes"
down_revision: Union[str, Sequence[str], None] = "0008_daily_trade_date_index"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop the heavy single-column B-tree indexes.
    op.execute("DROP INDEX IF EXISTS idx_daily_trade_date")
    op.execute("DROP INDEX IF EXISTS idx_index_daily_trade_date")
    # BRIN replacement keeps "scan by trade_date range" cheap for ETL checks.
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_trade_date_brin "
        "ON daily USING BRIN (trade_date) WITH (pages_per_range = 32)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_index_daily_trade_date_brin "
        "ON index_daily USING BRIN (trade_date) WITH (pages_per_range = 32)"
    )


def downgrade() -> None:
    # Reverse: drop BRIN, restore original B-tree.
    op.execute("DROP INDEX IF EXISTS idx_daily_trade_date_brin")
    op.execute("DROP INDEX IF EXISTS idx_index_daily_trade_date_brin")
    op.execute("CREATE INDEX IF NOT EXISTS idx_daily_trade_date ON daily (trade_date DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_index_daily_trade_date ON index_daily (trade_date DESC)")