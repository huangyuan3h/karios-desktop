"""0038 bar_5min (2026-09-03).

Last-hour (14:30–15:00) 5-minute OHLCV for CN A-shares. Separate from
bar_minute (1-minute Tencent current-session tape) so 14:30 5m and 1m
bars cannot collide.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from data_sync_service.db.bar_5min import CREATE_SQL

revision: str = "0038_bar_5min"
down_revision: str | Sequence[str] | None = "0037_sleeve_execution_log"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for stmt in CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_bar_5min_date_time;")
    op.execute("DROP INDEX IF EXISTS ix_bar_5min_ts_date;")
    op.execute("DROP TABLE IF EXISTS bar_5min;")
