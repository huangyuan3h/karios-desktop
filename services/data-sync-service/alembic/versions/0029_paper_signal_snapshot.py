"""0029 paper signal snapshot (2026-08-12).

paper_trades gains a JSONB snapshot of the orthogonal info layers at entry
time (SW L1 industry 5-day net-inflow rank / total / amount, alpha-event
count). Written by the S-3 paper intake (17:42 cron) so the C4 paper-vs-
backtest comparison can later test whether industry-flow-leading or
event-tagged entries outperform — display/validation data only, never a
gate.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0029_paper_signal_snapshot"
down_revision: str | Sequence[str] | None = "0028_trading_brief_markdown"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "paper_trades",
        sa.Column("signal_snapshot", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("paper_trades", "signal_snapshot")
