"""0035 stock_forecast (2026-08-15).

New table stock_forecast: A-share earnings forecasts (业绩预告) per
(ts_code, ann_date) from tushare ``forecast``. Purpose (signal pool P14):
post-earnings drift (PEAD) gate — a candidate whose name announced a
POSITIVE surprise within the last N sessions is favored/allowed at entry.

Data discipline: event date = ann_date (announcement), NOT end_date (report
period end) — a report is only tradeable from its announcement onward.
Backfill via scripts/backfill_forecast.py.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0035_stock_forecast"
down_revision: str | Sequence[str] | None = "0034_bar_minute"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # stock_forecast is already included in the current baseline DDL.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_forecast (
            ts_code TEXT NOT NULL,
            ann_date TEXT NOT NULL,
            end_date TEXT,
            forecast_type TEXT,
            net_profit_min DOUBLE PRECISION,
            net_profit_max DOUBLE PRECISION,
            change_pct DOUBLE PRECISION,
            CONSTRAINT pk_stock_forecast PRIMARY KEY (ts_code, ann_date)
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS stock_forecast;")
