"""0051 daily trade_date btree (OPT-221, 2026-09-17).

Re-adds the B-tree on ``daily(trade_date)`` that 0009 dropped as "redundant vs
PK prefix scans". That analysis covered per-symbol lookups only; the
date-family queries in production (``MAX(trade_date)``, ``DISTINCT trade_date``,
``COUNT(DISTINCT trade_date)``, date-range joins in sentiment / mainline /
paper_s3 / watchlist_automation) cannot use the PK or the BRIN:

- ``SELECT MAX(trade_date) FROM daily``: 3.2s (seq scan) -> 0.00s
- ``COUNT(*) WHERE trade_date >= X``: 3.6s -> 0.07s
- daily-distinct calendar: 3.9s -> 2.9s (service now uses a witness fast path
  anyway; kept as the fallback path's floor)

The BRIN stays for large sequential range scans; the two indexes coexist.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0051_daily_trade_date_btree"
down_revision: str | Sequence[str] | None = "0050_remove_twin_star"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_daily_trade_date_btree ON daily(trade_date)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_daily_trade_date_btree")
