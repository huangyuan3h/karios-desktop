"""Add taxonomy fields to industry fund-flow rows.

Revision ID: 0007_industry_fund_flow_taxonomy
Revises: 0006_etf_fund_flow_realtime
Create Date: 2026-06-22

"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "0007_industry_fund_flow_taxonomy"
down_revision: Union[str, Sequence[str], None] = "0006_etf_fund_flow_realtime"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily ADD COLUMN IF NOT EXISTS taxonomy TEXT NOT NULL DEFAULT 'UNKNOWN';")
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily ADD COLUMN IF NOT EXISTS industry_level INTEGER;")
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'eastmoney_bkzj';")
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cn_industry_fund_flow_taxonomy_level_date
        ON market_cn_industry_fund_flow_daily(taxonomy, industry_level, date DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_cn_industry_fund_flow_taxonomy_level_date;")
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily DROP COLUMN IF EXISTS source;")
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily DROP COLUMN IF EXISTS industry_level;")
    op.execute("ALTER TABLE market_cn_industry_fund_flow_daily DROP COLUMN IF EXISTS taxonomy;")
