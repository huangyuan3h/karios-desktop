"""Add `source` column to execution_decision_changes + paper_trades (TIP-011).

Revision ID: 0018_source_attribution
Revises: 0017_drop_backtest_tables
Create Date: 2026-08-04

Source attribution lets the Dashboard Copy markdown and the new
/v1/execution/source-stats endpoint break out BUY/ADD win-rate by
provenance (TV / ALPHA / MANUAL). Existing rows keep `source=NULL`
and are treated as unknown.

- paper_trades.source — set by the intake job from
  execution_decision_changes.source.
- execution_decision_changes.source — set by execution_journal.diff_snapshots
  (via the card.source that deriveActionCard attaches).

`source` is a closed enum: 'TV' | 'ALPHA' | 'MANUAL'. NULL means
"ingested before TIP-011 shipped" — backfill script optional.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0018_source_attribution"
down_revision: str | Sequence[str] | None = "0017_drop_backtest_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS source TEXT;")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_trades_source "
        "ON paper_trades(source, entry_date DESC) WHERE source IS NOT NULL;"
    )
    op.execute(
        "ALTER TABLE execution_decision_changes "
        "ADD COLUMN IF NOT EXISTS source TEXT;"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_changes_source "
        "ON execution_decision_changes(source, changed_at DESC) "
        "WHERE source IS NOT NULL;"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_execution_changes_source;")
    op.execute("DROP INDEX IF EXISTS idx_paper_trades_source;")
    op.execute("ALTER TABLE execution_decision_changes DROP COLUMN IF EXISTS source;")
    op.execute("ALTER TABLE paper_trades DROP COLUMN IF EXISTS source;")