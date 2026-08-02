"""Add tier + category columns to news_sources (News Substrate 2.0 · Track 1).

Revision ID: 0013_news_sources_tier
Revises: 0012_tv_screeners_api_mode
Create Date: 2026-08-02

Adds:
- news_sources.tier TEXT ('A' | 'B' | 'C' | 'D') — importance weight, default 'D'
- news_sources.category TEXT ('telegraph' | 'depth' | 'macro' | 'policy' | 'tech'
  | 'international' | 'company' | 'other') — editorial bucket

Existing rows default to tier='D' (unclassified). Backfill script in
scripts/seed_news_sources.py assigns explicit tiers.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0013_news_sources_tier"
down_revision: str | Sequence[str] | None = "0012_tv_screeners_api_mode"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NOTE: baseline 0001 pulls live CREATE_SQL from db/news.py, which already
    # contains these columns, so all ALTERs must be idempotent (IF NOT EXISTS).
    op.execute("ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'D';")
    op.execute("ALTER TABLE news_sources ADD COLUMN IF NOT EXISTS category TEXT;")
    op.execute("CREATE INDEX IF NOT EXISTS idx_news_sources_tier ON news_sources (tier);")


def downgrade() -> None:
    op.drop_index("idx_news_sources_tier", table_name="news_sources")
    op.drop_column("news_sources", "category")
    op.drop_column("news_sources", "tier")
