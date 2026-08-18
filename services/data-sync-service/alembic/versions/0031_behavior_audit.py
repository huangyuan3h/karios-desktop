"""0031 behavior audit table (2026-08-13 · watchlist 行为对账 OPT-106).

Real holdings (watchlist registry) vs the S-3 backtest "should hold" set for
one trading day — extra (held but backtest does not: 买了不该买 / 该卖没卖)
and missing (backtest holds, user does not). Kept in sync with
db/behavior_audit.py CREATE_SQL.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0031_behavior_audit"
down_revision: str | Sequence[str] | None = "0030_webhook_subscription"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "behavior_audit",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("audit_date", sa.Text(), nullable=False),
        sa.Column("market", sa.Text(), nullable=False),
        sa.Column("expected", sa.Integer(), nullable=False),
        sa.Column("actual", sa.Integer(), nullable=False),
        sa.Column("extra", sa.Integer(), nullable=False),
        sa.Column("missing", sa.Integer(), nullable=False),
        sa.Column("extra_list", sa.Text()),
        sa.Column("missing_list", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("audit_date", "market", name="uq_behavior_audit_date_market"),
    )


def downgrade() -> None:
    op.drop_table("behavior_audit")
