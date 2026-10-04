"""0055 amp_1430 — dense <=14:30 session high/low for the satellite amp key.

The frozen S-gap satellite ranks by `rank_key="amp_1430"`. Historically that
input came from the sparse `bar_5min` (decision slots only) proxy, which made
the live realtime panel and the frozen replay select different names
(docs/backtests/sat/sat-live-replay-amp-drift-2026-09-28.md). This table is the
single dense source (vendor full-day 5-min <=14:30 historically; the 14:30 live
snapshot forward). See docs/backtests/stable/dense-amp-1430-2026-09-28.md.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0055_amp_1430"
down_revision: str | Sequence[str] | None = "0054_user_trades_strategy_legs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE TABLE IF NOT EXISTS amp_1430 ("
        " ts_code TEXT NOT NULL,"
        " trade_date TEXT NOT NULL,"
        " high DOUBLE PRECISION NOT NULL,"
        " low DOUBLE PRECISION NOT NULL,"
        " source TEXT NOT NULL DEFAULT 'vendor',"
        " updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
        " PRIMARY KEY (ts_code, trade_date)"
        ");"
        "CREATE INDEX IF NOT EXISTS ix_amp_1430_date ON amp_1430 (trade_date);"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS amp_1430;")
