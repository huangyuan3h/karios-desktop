"""0046 sat_push_log — queryable twin of the 14:30 intraday screen cache.

One row per (trade_date, slot, ts_code): which names were pushed
(candidates), shown as alternates, blocked (limit-up) or skipped (C1),
with gate/breadth context. Joins user_trades on (ts_code, trade_date)
to audit push -> fill.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0046_sat_push_log"
down_revision: str | Sequence[str] | None = "0045_cn_hot_rank"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS sat_push_log (
        trade_date  DATE NOT NULL,
        slot        TEXT NOT NULL,
        ts_code     TEXT NOT NULL,
        amp         DOUBLE PRECISION,
        gap_pct     DOUBLE PRECISION,
        gate_open   BOOLEAN,
        breadth     DOUBLE PRECISION,
        snapshot_at TIMESTAMPTZ,
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, slot, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_sat_push_log_date ON sat_push_log(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_sat_push_log_ts_date ON sat_push_log(ts_code, trade_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sat_push_log")
