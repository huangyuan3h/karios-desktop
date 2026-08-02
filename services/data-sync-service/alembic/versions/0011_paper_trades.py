"""Add paper_trades table (OPT-049).

Revision ID: 0011_paper_trades
Revises: 0010_execution_decision_journal
Create Date: 2026-08-01

"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from data_sync_service.db.paper_trading import CREATE_SQL as PAPER_TRADES_CREATE_SQL

revision: str = "0011_paper_trades"
down_revision: str | Sequence[str] | None = "0010_execution_decision_journal"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for stmt in PAPER_TRADES_CREATE_SQL.split(";"):
        part = stmt.strip()
        if part:
            op.execute(part + ";")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS paper_trades;")
