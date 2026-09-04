"""0040 behavior_audit satellite-leg columns (2026-09-04).

OPT-140: the registry audit splits CORE (S-3) vs SATELLITE (twin-star engine
book) legs. Satellite holdings must never count as S-3 ``extra`` again, but
their own should-hold state still needs persisting for the watchlist banner.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0040_behavior_audit_sat_leg"
down_revision: str | Sequence[str] | None = "0039_signal_snapshot_jsonb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_expected INTEGER NOT NULL DEFAULT 0;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_actual INTEGER NOT NULL DEFAULT 0;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_extra INTEGER NOT NULL DEFAULT 0;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_missing INTEGER NOT NULL DEFAULT 0;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_extra_list TEXT;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_missing_list TEXT;")


def downgrade() -> None:
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_missing_list;")
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_extra_list;")
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_missing;")
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_extra;")
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_actual;")
    op.execute("ALTER TABLE behavior_audit DROP COLUMN IF EXISTS sat_expected;")
