"""0049 cn_xq_follow — Snowball follow-count daily snapshot (P0-13 attention).

One row per (ts_code, trade_date): cumulative 雪球 follow count + latest price.
No history API — grows forward one snapshot/day. Eastmoney rank panel is
cn_hot_rank (0045), a separate source.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0049_cn_xq_follow"
down_revision: str | Sequence[str] | None = "0048_system_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_xq_follow (
        ts_code     TEXT NOT NULL,
        trade_date  DATE NOT NULL,
        follow      DOUBLE PRECISION,
        price       DOUBLE PRECISION,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (ts_code, trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_xq_follow_date ON cn_xq_follow(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cn_xq_follow_ts_date ON cn_xq_follow(ts_code, trade_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cn_xq_follow")
