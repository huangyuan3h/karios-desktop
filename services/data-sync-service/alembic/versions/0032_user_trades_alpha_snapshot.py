"""0032 user trades alpha snapshot (2026-08-13).

user_trades gains a JSONB as-of alpha-radar snapshot captured at trade time
(§19.3 forward data collection): only events visible ON the trade date count
(no lookahead). Written by POST /trades for every BUY/ADD/SELL leg so the
future journal-vs-alpha backtest can test whether alpha-endorsed entries
outperform and whether stale/risk-flipped alpha justifies an early exit —
display/validation data only, never a gate.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0032_user_trades_alpha_snapshot"
down_revision: str | Sequence[str] | None = "0031_behavior_audit"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades ADD COLUMN IF NOT EXISTS alpha_snapshot JSONB;"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE user_trades DROP COLUMN IF EXISTS alpha_snapshot;")
