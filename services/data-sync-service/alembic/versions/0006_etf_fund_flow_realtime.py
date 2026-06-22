"""Add realtime ETF fund-flow source fields.

Revision ID: 0006_etf_fund_flow_realtime
Revises: 0005_purge_non_trading_daily
Create Date: 2026-06-22

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0006_etf_fund_flow_realtime"
down_revision: Union[str, Sequence[str], None] = "0005_purge_non_trading_daily"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS source TEXT;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS trade_time TEXT;")
    op.execute(
        "ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS main_net_inflow DOUBLE PRECISION;"
    )
    op.execute(
        "ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS super_large_net_inflow DOUBLE PRECISION;"
    )
    op.execute(
        "ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS large_net_inflow DOUBLE PRECISION;"
    )
    op.execute(
        "ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS medium_net_inflow DOUBLE PRECISION;"
    )
    op.execute(
        "ALTER TABLE market_etf_fund_flow_daily ADD COLUMN IF NOT EXISTS small_net_inflow DOUBLE PRECISION;"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_etf_fund_flow_source ON market_etf_fund_flow_daily(source);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_etf_fund_flow_source;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS small_net_inflow;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS medium_net_inflow;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS large_net_inflow;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS super_large_net_inflow;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS main_net_inflow;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS trade_time;")
    op.execute("ALTER TABLE market_etf_fund_flow_daily DROP COLUMN IF EXISTS source;")
