"""Add research_reports table (研报 → Alpha channel).

Revision ID: 0019_research_reports
Revises: 0018_source_attribution
Create Date: 2026-08-05

Sell-side research reports pulled from East Money's report center. Each
report carries rating / target price / EPS forecasts; per-report alpha
scores are computed by service/research.py and stored on alpha_score.
Keep in sync with db/research.py CREATE_TABLE_SQL.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0019_research_reports"
down_revision: str | Sequence[str] | None = "0018_source_attribution"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS research_reports (
            id            BIGSERIAL PRIMARY KEY,
            info_code     TEXT NOT NULL UNIQUE,
            stock_code    TEXT NOT NULL,
            stock_name    TEXT NOT NULL,
            title         TEXT NOT NULL,
            org_name      TEXT NOT NULL,
            rating        TEXT,
            target_price  DOUBLE PRECISION,
            eps_this_year DOUBLE PRECISION,
            pe_this_year  DOUBLE PRECISION,
            industry_name TEXT,
            market        TEXT NOT NULL DEFAULT 'CN',
            publish_date  DATE NOT NULL,
            encode_url    TEXT,
            source        TEXT NOT NULL DEFAULT 'eastmoney',
            alpha_score   DOUBLE PRECISION,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (stock_code, publish_date, title)
        );
        CREATE INDEX IF NOT EXISTS idx_research_reports_publish_date
            ON research_reports (publish_date DESC);
        CREATE INDEX IF NOT EXISTS idx_research_reports_stock_code
            ON research_reports (stock_code);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS research_reports;")
