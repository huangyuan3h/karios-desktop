"""0050 remove twin-star (2026-09-13).

Removes the falsified 机会双子星 (twin-star / satellite) strategy surface:

- drops ``sat_push_log`` (14:30 intraday satellite push log);
- drops the ``behavior_audit`` sat_* leg columns (OPT-140 split removed);
- migrates ``user_trades.leg`` 'sat' -> 'parking' and narrows the CHECK to
  ('s3', 'parking').

Historical ``paper_trades`` rows (source='twin_star', close_reason='body_exit')
are kept as evidence — only the enum constants were retired.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0050_remove_twin_star"
down_revision: str | Sequence[str] | None = "0049_cn_xq_follow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_SAT_COLUMNS: tuple[str, ...] = (
    "sat_expected",
    "sat_actual",
    "sat_extra",
    "sat_missing",
    "sat_extra_list",
    "sat_missing_list",
)

_SAT_PUSH_LOG_DDL = """
CREATE TABLE IF NOT EXISTS sat_push_log (
    trade_date  DATE NOT NULL,
    slot        TEXT NOT NULL,
    ts_code     TEXT NOT NULL,
    amp         DOUBLE PRECISION,
    gap_pct     DOUBLE PRECISION,
    gate_open   BOOLEAN,
    breadth     DOUBLE PRECISION,
    snapshot_at TIMESTAMPTZ,
    stage       TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, slot, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_sat_push_log_date ON sat_push_log(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_sat_push_log_ts_date ON sat_push_log(ts_code, trade_date DESC);
"""


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sat_push_log;")
    for column in _SAT_COLUMNS:
        op.execute(f"ALTER TABLE behavior_audit DROP COLUMN IF EXISTS {column};")
    op.execute("ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;")
    op.execute("UPDATE user_trades SET leg = 'parking' WHERE leg = 'sat';")
    op.execute(
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check "
        "CHECK (leg IN ('s3', 'parking'));"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE user_trades DROP CONSTRAINT IF EXISTS user_trades_leg_check;")
    op.execute("UPDATE user_trades SET leg = 'sat' WHERE leg = 'parking';")
    op.execute(
        "ALTER TABLE user_trades ADD CONSTRAINT user_trades_leg_check CHECK (leg IN ('s3', 'sat'));"
    )
    op.execute(
        "ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_expected INTEGER NOT NULL DEFAULT 0;"
    )
    op.execute(
        "ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_actual INTEGER NOT NULL DEFAULT 0;"
    )
    op.execute(
        "ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_extra INTEGER NOT NULL DEFAULT 0;"
    )
    op.execute(
        "ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_missing INTEGER NOT NULL DEFAULT 0;"
    )
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_extra_list TEXT;")
    op.execute("ALTER TABLE behavior_audit ADD COLUMN IF NOT EXISTS sat_missing_list TEXT;")
    op.execute(_SAT_PUSH_LOG_DDL)
