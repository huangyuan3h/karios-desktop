"""Add API-mode + filter_json columns to tv_screeners (OPT-057).

Revision ID: 0012_tv_screeners_api_mode
Revises: 0011_paper_trades
Create Date: 2026-08-01

"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0012_tv_screeners_api_mode"
down_revision: str | Sequence[str] | None = "0011_paper_trades"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NOTE: baseline 0001 pulls live CREATE_SQL from db/tv.py, which already
    # contains these columns, so all ALTERs must be idempotent (IF NOT EXISTS)
    # to work both on fresh DBs and on legacy DBs that predate OPT-057.
    # mode: 'api' (TV Scanner API) | 'chrome' (legacy CDP).
    # Default 'chrome' to preserve backward-compat for all existing rows.
    op.execute(
        "ALTER TABLE tv_screeners ADD COLUMN IF NOT EXISTS "
        "mode TEXT NOT NULL DEFAULT 'chrome';"
    )
    # market: 'cn' | 'hk' | 'us' | NULL (any). Only relevant when mode='api'.
    op.execute("ALTER TABLE tv_screeners ADD COLUMN IF NOT EXISTS market TEXT;")
    # filter_json: TV Scanner API filter payload (only when mode='api').
    op.execute("ALTER TABLE tv_screeners ADD COLUMN IF NOT EXISTS filter_json JSONB;")
    # api_columns: TV Scanner API columns list (only when mode='api').
    op.execute("ALTER TABLE tv_screeners ADD COLUMN IF NOT EXISTS api_columns JSONB;")
    # url: was NOT NULL; now nullable so mode='api' screeners can omit it.
    op.alter_column("tv_screeners", "url", existing_type=sa.Text(), nullable=True)
    # Enforce mode enum at DB level; recreate so both paths converge.
    op.execute("ALTER TABLE tv_screeners DROP CONSTRAINT IF EXISTS ck_tv_screeners_mode;")
    op.execute(
        "ALTER TABLE tv_screeners ADD CONSTRAINT ck_tv_screeners_mode "
        "CHECK (mode IN ('api', 'chrome'));"
    )


def downgrade() -> None:
    op.drop_constraint("ck_tv_screeners_mode", "tv_screeners", type_="check")
    op.alter_column("tv_screeners", "url", existing_type=sa.Text(), nullable=False)
    op.drop_column("tv_screeners", "api_columns")
    op.drop_column("tv_screeners", "filter_json")
    op.drop_column("tv_screeners", "market")
    op.drop_column("tv_screeners", "mode")