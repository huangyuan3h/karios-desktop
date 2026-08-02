"""Create morning_briefs table (News Substrate 2.0 · Track 3).

Revision ID: 0015_morning_briefs
Revises: 0014_news_items_enrichment
Create Date: 2026-08-02

Creates:
- morning_briefs table — stores curated morning/midday briefings
  (top 5–7 enriched news items selected by importance × relevance × freshness).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "0015_morning_briefs"
down_revision: str | Sequence[str] | None = "0014_news_items_enrichment"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "morning_briefs",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("brief_date", sa.Text(), nullable=False),
        sa.Column("brief_type", sa.Text(), nullable=False),
        sa.Column("items", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("macro_overview", sa.Text(), nullable=True),
        sa.Column("model_version", sa.Text(), nullable=True),
        sa.Column("source_item_ids", sa.ARRAY(sa.Text()), nullable=True),
        sa.Column("created_at", sa.Text(), nullable=False),
        sa.UniqueConstraint("brief_date", "brief_type", name="uq_morning_briefs_date_type"),
    )
    op.create_index(
        "idx_morning_briefs_date",
        "morning_briefs",
        ["brief_date"],
    )
    op.create_index(
        "idx_morning_briefs_type",
        "morning_briefs",
        ["brief_type"],
    )


def downgrade() -> None:
    op.drop_index("idx_morning_briefs_type", table_name="morning_briefs")
    op.drop_index("idx_morning_briefs_date", table_name="morning_briefs")
    op.drop_table("morning_briefs")
