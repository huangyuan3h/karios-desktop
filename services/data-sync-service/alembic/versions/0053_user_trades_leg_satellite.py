"""0053 user_trades leg: allow 'satellite' (2026-09-21).

The watchlist satellite card can now record its 14:30 buys directly (manual
execution aid). Those legs must be auditable against the satellite book, not
S-3 / parking, so the leg CHECK gains a third value.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0053_user_trades_leg_satellite"
down_revision: str | Sequence[str] | None = "0052_app_settings"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check "
        "CHECK (leg IN ('s3', 'parking', 'satellite'));"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check "
        "CHECK (leg IN ('s3', 'parking'));"
    )
