"""0039 paper_trades.signal_snapshot json -> jsonb (2026-09-04).

OPT-138 root cause: the live/dev ``paper_trades.signal_snapshot`` column was
created as ``json`` (pre-0029 ``ensure_table``), while 0029 only ran
``ADD COLUMN IF NOT EXISTS ... JSONB`` — a no-op on existing DBs. Any
``close_paper_trade(..., signal_snapshot_extra=...)`` then failed in Postgres
with ``COALESCE could not convert type jsonb to json`` (CREATE_SQL already
declares JSONB, so fresh DBs were fine).

This revision converts the column in place; ``USING ...::jsonb`` is safe
(every JSON value is valid JSONB).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0039_signal_snapshot_jsonb"
down_revision: str | Sequence[str] | None = "0038_bar_5min"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE paper_trades "
        "ALTER COLUMN signal_snapshot TYPE JSONB "
        "USING signal_snapshot::jsonb;"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE paper_trades "
        "ALTER COLUMN signal_snapshot TYPE JSON "
        "USING signal_snapshot::json;"
    )
