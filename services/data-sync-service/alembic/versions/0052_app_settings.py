"""0052 app_settings (OPT-223, 2026-09-17).

Generic key/value store for cross-layer preferences the backend needs while
composing background pushes. First key: ``strategy_mode`` — the UI mirrors the
selected strategy here so Bark/notifications can be written for it.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0052_app_settings"
down_revision: str | Sequence[str] | None = "0051_daily_trade_date_btree"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS app_settings (
        key        TEXT PRIMARY KEY,
        value      JSONB NOT NULL DEFAULT '{}'::jsonb,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app_settings")
