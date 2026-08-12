"""0025 allocation weights — T4 cross-market capital pool.

R5c weekly decisions (Monday, Asia/Shanghai): CN tradable -> 100% CN, only HK
tradable -> 100% HK, both weak -> 0/0. The paper intake reads this table for
the week's sleeve scale so the real book follows the same allocation the
backtest replays (service/allocation.py — same decision code both sides).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0025_allocation_weights"
down_revision: str | Sequence[str] | None = "0024_stock_dailybasic"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "allocation_weights",
        sa.Column("week_start", sa.Text, primary_key=True),
        sa.Column("cn_regime", sa.Text, nullable=False),
        sa.Column("hk_regime", sa.Text, nullable=False),
        sa.Column("w_cn", sa.Double, nullable=False),
        sa.Column("w_hk", sa.Double, nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("allocation_weights")
