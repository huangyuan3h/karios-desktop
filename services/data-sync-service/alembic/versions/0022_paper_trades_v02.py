"""Paper-trading v0.2 columns: market + gross/net cost split (OPT-062 / L3-P1).

Revision ID: 0022_paper_trades_v02
Revises: 0021_decision_actions
Create Date: 2026-08-07

Paper v0.2 adds a per-market cost model (slippage / commission / stamp tax)
and HK support. Three columns:

- ``market`` — 'CN' | 'HK'. Legacy rows are backfilled to 'CN' (v0 was
  CN-only). NOT NULL with a default so future inserts can't forget it.
- ``gross_pnl_pct`` — close-time pnl BEFORE round-trip costs. Legacy rows
  backfilled to ``pnl_pct`` (they have no cost model; costs = 0).
- ``costs_pct`` — round-trip cost as % of position, deducted from gross to
  produce the net ``pnl_pct``. Legacy rows backfilled to 0.

Semantics change (documented in db/paper_trading.py + docs/designs/
l3-l4-evolution-roadmap.md): from v0.2 onward ``pnl_pct`` on CLOSED rows is
the NET pnl; open rows keep showing the current gross pnl until the trade
closes (costs land once, at close time).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0022_paper_trades_v02"
down_revision: str | Sequence[str] | None = "0021_decision_actions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS market TEXT;")
    op.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS gross_pnl_pct DOUBLE PRECISION;")
    op.execute("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS costs_pct DOUBLE PRECISION;")
    # Legacy rows: v0 was CN-only with no cost model → market='CN',
    # gross = the pnl we recorded, costs = 0.
    op.execute("UPDATE paper_trades SET market = 'CN' WHERE market IS NULL;")
    op.execute("UPDATE paper_trades SET gross_pnl_pct = pnl_pct WHERE gross_pnl_pct IS NULL AND pnl_pct IS NOT NULL;")
    op.execute("UPDATE paper_trades SET costs_pct = 0 WHERE costs_pct IS NULL AND pnl_pct IS NOT NULL;")
    op.execute(
        "ALTER TABLE paper_trades ALTER COLUMN market SET NOT NULL;"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_paper_trades_market "
        "ON paper_trades(market, entry_date DESC);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_paper_trades_market;")
    op.execute("ALTER TABLE paper_trades ALTER COLUMN market DROP NOT NULL;")
    op.execute("ALTER TABLE paper_trades DROP COLUMN IF EXISTS costs_pct;")
    op.execute("ALTER TABLE paper_trades DROP COLUMN IF EXISTS gross_pnl_pct;")
    op.execute("ALTER TABLE paper_trades DROP COLUMN IF EXISTS market;")
