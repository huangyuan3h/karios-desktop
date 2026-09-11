"""0045 cn_hot_rank — Eastmoney popularity rank panel (D1).

One row per (ts_code, trade_date): full-market hot rank (smaller = hotter)
+ follower mix. ~366d history per stock, weekend rows included.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0045_cn_hot_rank"
down_revision: str | Sequence[str] | None = "0044_cn_fin_statements"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_hot_rank (
        ts_code     TEXT NOT NULL,
        trade_date  DATE NOT NULL,
        rank        DOUBLE PRECISION,
        new_fans    DOUBLE PRECISION,
        iron_fans   DOUBLE PRECISION,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (ts_code, trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_hot_rank_date ON cn_hot_rank(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cn_hot_rank_ts_date ON cn_hot_rank(ts_code, trade_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cn_hot_rank")
