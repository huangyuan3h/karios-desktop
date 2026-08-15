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

import sqlalchemy as sa

from alembic import op

revision: str = "0035_stock_forecast"
down_revision: str | Sequence[str] | None = "0034_bar_minute"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "stock_forecast",
        sa.Column("ts_code", sa.Text(), nullable=False),
        sa.Column("ann_date", sa.Text(), nullable=False),
        sa.Column("end_date", sa.Text(), nullable=True),
        sa.Column("forecast_type", sa.Text(), nullable=True),
        sa.Column("net_profit_min", sa.Double(), nullable=True),
        sa.Column("net_profit_max", sa.Double(), nullable=True),
        sa.Column("change_pct", sa.Double(), nullable=True),
        sa.PrimaryKeyConstraint("ts_code", "ann_date", name="pk_stock_forecast"),
    )


def downgrade() -> None:
    op.drop_table("stock_forecast")
