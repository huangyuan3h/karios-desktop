"""0034 bar_minute (2026-08-14).

New table bar_minute: 1-minute OHLCV bars for CN/HK symbols, captured daily
from Tencent minute-line endpoints (web.ifzq.gtimg.cn hkMinute/minute —
the only intraday source that works from this network; Eastmoney push2his
is IP-rate-limited). Purpose (TIP-014 Phase 3 / D7): validate intraday
entry fills (尾盘执行) and re-sample 5m bars for entry-price research.

Data starts accumulating from the deployment date — no backfill (history
not available from Tencent minute endpoints; Eastmoney blocked).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0034_bar_minute"
down_revision: str | Sequence[str] | None = "0033_webhook_provider"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # bar_minute is already part of the current baseline DDL. Keep this
    # historical migration safe for fresh databases and stamped deployments.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS bar_minute (
            id BIGSERIAL PRIMARY KEY,
            ts_code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            trade_time TEXT NOT NULL,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION NOT NULL,
            vol DOUBLE PRECISION,
            amount DOUBLE PRECISION,
            CONSTRAINT uq_bar_minute_ts_time UNIQUE (ts_code, trade_date, trade_time)
        );
        CREATE INDEX IF NOT EXISTS ix_bar_minute_ts_date
            ON bar_minute (ts_code, trade_date);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS bar_minute;")
