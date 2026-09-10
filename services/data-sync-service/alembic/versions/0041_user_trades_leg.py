"""0041 user_trades leg split (2026-09-09).

OPT-149: satellite manual legs (twin-star 12.5% slots) must be auditable
against the satellite book, not the S-3 rulebook. Adds ``leg`` with a
safe default so all pre-existing rows stay S-3.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0041_user_trades_leg"
down_revision: str | Sequence[str] | None = "0040_behavior_audit_sat_leg"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE user_trades ADD COLUMN IF NOT EXISTS leg TEXT NOT NULL DEFAULT 's3';")
    op.execute(
        "ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;"
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check CHECK (leg IN ('s3', 'sat'));"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;")
    op.execute("ALTER TABLE user_trades DROP COLUMN IF EXISTS leg;")
