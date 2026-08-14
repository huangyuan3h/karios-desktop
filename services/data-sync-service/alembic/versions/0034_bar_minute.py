"""0034 bar_minute (2026-08-14).

New table bar_minute: 1-minute OHLCV bars for CN/HK symbols, captured daily
from Tencent minute-line endpoints (web.ifzq.gtimg.cn hkMinute/minute —
the only intraday source that works from this network; Eastmoney push2his
is IP-rate-limited). Purpose (TIP-014 Phase 3 / D7): validate intraday
entry fills (尾盘执行) and re-sample 5m bars for entry-price research.

Data starts accumulating from the deployment date — no backfill (history
not available from Tencent minute endpoints; Eastmoney blocked).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0034_bar_minute"
down_revision: str | Sequence[str] | None = "0033_webhook_provider"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "bar_minute",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("ts_code", sa.Text(), nullable=False),
        sa.Column("trade_date", sa.Text(), nullable=False),
        sa.Column("trade_time", sa.Text(), nullable=False),
        sa.Column("open", sa.Double(), nullable=True),
        sa.Column("high", sa.Double(), nullable=True),
        sa.Column("low", sa.Double(), nullable=True),
        sa.Column("close", sa.Double(), nullable=False),
        sa.Column("vol", sa.Double(), nullable=True),
        sa.Column("amount", sa.Double(), nullable=True),
        sa.UniqueConstraint("ts_code", "trade_date", "trade_time", name="uq_bar_minute_ts_time"),
    )
    op.create_index("ix_bar_minute_ts_date", "bar_minute", ["ts_code", "trade_date"])


def downgrade() -> None:
    op.drop_table("bar_minute")
