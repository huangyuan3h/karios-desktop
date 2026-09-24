"""0054 isolate user journal strategy and H2/B3 legs.

The manual journal now records the selected strategy explicitly and distinguishes
H2/B3 cash sleeves from Harbor parking. Existing rows remain ``legacy_unknown``
rather than being guessed into a strategy.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0054_user_trades_strategy_legs"
down_revision: str | Sequence[str] | None = "0053_user_trades_leg_satellite"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE user_trades ADD COLUMN IF NOT EXISTS strategy_mode TEXT NOT NULL DEFAULT 'legacy_unknown';"
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check "
        "CHECK (leg IN ('s3', 'parking', 'satellite', 'h2', 'b3'));"
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_strategy_mode_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_strategy_mode_check "
        "CHECK (strategy_mode IN ('harbor', 'homeport', 'starport', 'starship', "
        "'starship_robust', 'twin_star', 'legacy_unknown'));"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE user_trades SET leg = 'parking' WHERE leg IN ('h2', 'b3');"
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check "
        "CHECK (leg IN ('s3', 'parking', 'satellite'));"
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_strategy_mode_check;"
        "ALTER TABLE user_trades DROP COLUMN IF EXISTS strategy_mode;"
    )
