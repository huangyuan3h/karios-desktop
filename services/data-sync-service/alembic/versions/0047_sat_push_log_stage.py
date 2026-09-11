"""0047 sat_push_log.stage — record the H-SAT-RANK stage tag per pushed name."""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0047_sat_push_log_stage"
down_revision: str | Sequence[str] | None = "0046_sat_push_log"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE sat_push_log ADD COLUMN IF NOT EXISTS stage TEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE sat_push_log DROP COLUMN IF EXISTS stage")
