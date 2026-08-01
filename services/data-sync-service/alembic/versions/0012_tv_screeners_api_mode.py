"""Add API-mode + filter_json columns to tv_screeners (OPT-057).

Revision ID: 0012_tv_screeners_api_mode
Revises: 0011_paper_trades
Create Date: 2026-08-01

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0012_tv_screeners_api_mode"
down_revision: str | Sequence[str] | None = "0011_paper_trades"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # mode: 'api' (TV Scanner API) | 'chrome' (legacy CDP).
    # Default 'chrome' to preserve backward-compat for all existing rows.
    op.add_column(
        "tv_screeners",
        sa.Column(
            "mode",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'chrome'"),
        ),
    )
    # market: 'cn' | 'hk' | 'us' | NULL (any). Only relevant when mode='api'.
    op.add_column(
        "tv_screeners",
        sa.Column("market", sa.Text(), nullable=True),
    )
    # filter_json: TV Scanner API filter payload (only when mode='api').
    op.add_column(
        "tv_screeners",
        sa.Column(
            "filter_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    # api_columns: TV Scanner API columns list (only when mode='api').
    op.add_column(
        "tv_screeners",
        sa.Column(
            "api_columns",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    # url: was NOT NULL; now nullable so mode='api' screeners can omit it.
    op.alter_column("tv_screeners", "url", existing_type=sa.Text(), nullable=True)
    # Add a CHECK constraint to enforce mode enum at DB level.
    op.create_check_constraint(
        "ck_tv_screeners_mode",
        "tv_screeners",
        "mode IN ('api', 'chrome')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_tv_screeners_mode", "tv_screeners", type_="check")
    op.alter_column("tv_screeners", "url", existing_type=sa.Text(), nullable=False)
    op.drop_column("tv_screeners", "api_columns")
    op.drop_column("tv_screeners", "filter_json")
    op.drop_column("tv_screeners", "market")
    op.drop_column("tv_screeners", "mode")