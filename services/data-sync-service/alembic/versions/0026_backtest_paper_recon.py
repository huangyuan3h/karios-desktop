"""0026 backtest-paper reconciliation snapshots (2026-08-11).

Weekly auto-reconcile (Monday 07:30 Asia/Shanghai): what the backtest says
we SHOULD hold vs what the paper book ACTUALLY holds on last Friday, per
market. The weekly report is the anchor for "make the real book follow the
backtest" — any drift (missing/extra positions, entry-date skew) is
surfaced here instead of silently diverging.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0026_backtest_paper_recon"
down_revision: str | Sequence[str] | None = "0025_allocation_weights"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backtest_paper_recon",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("recon_date", sa.Text, nullable=False),
        sa.Column("market", sa.Text, nullable=False),
        sa.Column("window", sa.Text, nullable=False),
        sa.Column("expected", sa.Integer, nullable=False),
        sa.Column("actual", sa.Integer, nullable=False),
        sa.Column("aligned", sa.Integer, nullable=False),
        sa.Column("missing", sa.Integer, nullable=False),
        sa.Column("extra", sa.Integer, nullable=False),
        sa.Column("detail", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("recon_date", "market", name="uq_recon_date_market"),
    )


def downgrade() -> None:
    op.drop_table("backtest_paper_recon")
