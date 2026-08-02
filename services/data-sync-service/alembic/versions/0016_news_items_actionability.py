"""Add actionability column to news_items (News Substrate 2.0 · Brief optimization).

Revision ID: 0016_news_items_actionability
Revises: 0015_morning_briefs
Create Date: 2026-08-02

Adds:
- actionability TEXT — actionable | informational | historical (LLM classification for brief selection)
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0016_news_items_actionability"
down_revision: str | Sequence[str] | None = "0015_morning_briefs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NOTE: baseline 0001 pulls live CREATE_SQL from db/news.py, which already
    # contains this column, so the statements must be idempotent (IF NOT EXISTS).
    op.execute("ALTER TABLE news_items ADD COLUMN IF NOT EXISTS actionability TEXT;")
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_news_items_actionability "
        "ON news_items (actionability);"
    )


def downgrade() -> None:
    op.drop_index("idx_news_items_actionability", table_name="news_items")
    op.drop_column("news_items", "actionability")
