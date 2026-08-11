"""0027 recon return-diff metrics (C4 half-way, 2026-08-11).

Reconciliation snapshots gain the C4 half-way metric: on aligned names,
the median return gap between the backtest replay (entry close → recon-day
close) and the paper book (entry price → recon-day close). Positive diff =
paper running ahead of the backtest. These columns feed the weekly review /
decision agent so drift in EXECUTION QUALITY (not just position count) is
measurable while we wait for >= 20 closed trades (full C4).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027_recon_return_diff"
down_revision: str | Sequence[str] | None = "0026_backtest_paper_recon"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "backtest_paper_recon",
        sa.Column("aligned_return_diff_pct", sa.Numeric(), nullable=True),
    )
    op.add_column(
        "backtest_paper_recon",
        sa.Column("bt_return_median_pct", sa.Numeric(), nullable=True),
    )
    op.add_column(
        "backtest_paper_recon",
        sa.Column("paper_return_median_pct", sa.Numeric(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_paper_recon", "aligned_return_diff_pct")
    op.drop_column("backtest_paper_recon", "bt_return_median_pct")
    op.drop_column("backtest_paper_recon", "paper_return_median_pct")
