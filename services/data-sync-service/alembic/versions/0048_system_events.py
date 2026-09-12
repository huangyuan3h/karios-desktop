"""0048 system_events — error/unstable inbox (was ensure_table-only).

OPT-165 caught the drift: ``db/system_events.py`` had a ``CREATE_SQL`` and an
``ensure_table()`` but no revision, so a fresh ``alembic upgrade head`` never
created the table.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from data_sync_service.db.system_events import CREATE_SQL

revision: str = "0048_system_events"
down_revision: str | Sequence[str] | None = "0047_sat_push_log_stage"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_system_events_sev_created;")
    op.execute("DROP INDEX IF EXISTS ix_system_events_type;")
    op.execute("DROP TABLE IF EXISTS system_events;")
