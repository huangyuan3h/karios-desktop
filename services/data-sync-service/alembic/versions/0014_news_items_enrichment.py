"""Add LLM enrichment columns to news_items (News Substrate 2.0 · Track 2).

Revision ID: 0014_news_items_enrichment
Revises: 0013_news_sources_tier
Create Date: 2026-08-02

Adds:
- tickers TEXT[]           — extracted A-share / HK ticker symbols (e.g. {600519, 00700})
- sectors TEXT[]           — related sector tags (e.g. {'白酒', '消费'})
- event_type TEXT          — event classification (earnings/macro/policy/m&a/ipo/dividend/other)
- importance SMALLINT      — 0–5 importance scale (0=ignore, 5=critical market-moving)
- relevance_score SMALLINT — 0–100 watchlist-aware relevance (higher = closer to user holdings)
- ai_summary TEXT          — LLM-generated one-paragraph summary
- enrichment_status TEXT   — pending | done | failed (default NULL = not yet attempted)
- enriched_at TIMESTAMPTZ  — when enrichment completed
- enrichment_model TEXT    — which model produced the enrichment (e.g. 'gpt-4o-mini')
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0014_news_items_enrichment"
down_revision: str | Sequence[str] | None = "0013_news_sources_tier"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NOTE: baseline 0001 pulls live CREATE_SQL from db/news.py, which already
    # contains these columns/indexes, so all statements must be idempotent
    # (IF NOT EXISTS) to work on fresh DBs and legacy DBs alike.
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS tickers TEXT[];")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS sectors TEXT[];")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS event_type TEXT;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS importance SMALLINT;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS relevance_score SMALLINT;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS ai_summary TEXT;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS enrichment_status TEXT;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;")
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS enrichment_model TEXT;")

    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_items_enrichment_status "
        "ON news_items (enrichment_status);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_items_importance "
        "ON news_items (importance);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_items_tickers "
        "ON news_items USING GIN (tickers);"
    )


def downgrade() -> None:
    op.drop_index("idx_news_items_tickers", table_name="news_items")
    op.drop_index("idx_news_items_importance", table_name="news_items")
    op.drop_index("idx_news_items_enrichment_status", table_name="news_items")
    op.drop_column("news_items", "enrichment_model")
    op.drop_column("news_items", "enriched_at")
    op.drop_column("news_items", "enrichment_status")
    op.drop_column("news_items", "ai_summary")
    op.drop_column("news_items", "relevance_score")
    op.drop_column("news_items", "importance")
    op.drop_column("news_items", "event_type")
    op.drop_column("news_items", "sectors")
    op.drop_column("news_items", "tickers")
