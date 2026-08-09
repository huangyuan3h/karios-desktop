"""User trade journal table (real buys / adds / sells entered in the UI).

Revision ID: 0023_user_trades
Revises: 0022_paper_trades_v02
Create Date: 2026-08-08

Append-only journal of the user's ACTUAL trades, separate from
``paper_trades`` (simulated signal log). Columns:

- ``side`` — 'BUY' | 'ADD' | 'SELL'
- ``trade_date`` — Shanghai calendar date of the leg
- ``price`` — entry price (BUY/ADD) or exit price (SELL)
- ``position_pct`` — position size involved in this leg
- ``cost_basis`` — blended cost at close (SELL only)
- ``entry_date`` — original open date (SELL only, holding-days source)
- ``pnl_pct`` / ``holding_days`` — computed at record time (SELL only, gross)
- ``source`` / ``market`` / ``note`` — attribution + free text
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0023_user_trades"
down_revision: str | Sequence[str] | None = "0022_paper_trades_v02"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS user_trades (
            id            TEXT PRIMARY KEY,
            symbol        TEXT NOT NULL,
            side          TEXT NOT NULL,
            trade_date    TEXT NOT NULL,
            price         DOUBLE PRECISION NOT NULL,
            position_pct  DOUBLE PRECISION NOT NULL,
            cost_basis    DOUBLE PRECISION,
            entry_date    TEXT,
            pnl_pct       DOUBLE PRECISION,
            holding_days  INTEGER,
            source        TEXT,
            market        TEXT NOT NULL DEFAULT 'CN',
            note          TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_trades_symbol_date "
        "ON user_trades(symbol, trade_date DESC);"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_trades_date "
        "ON user_trades(trade_date DESC);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_user_trades_date;")
    op.execute("DROP INDEX IF EXISTS idx_user_trades_symbol_date;")
    op.execute("DROP TABLE IF EXISTS user_trades;")
