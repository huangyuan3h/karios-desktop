"""Drop legacy backtest tables (OPT-059 · legacy cleanup).

Revision ID: 0017_drop_backtest_tables
Revises: 0016_news_items_actionability
Create Date: 2026-08-03

Drops:
- backtest_run — old testback/ framework run metadata (2 rows)
- backtest_trade — old testback/ framework trade log (132 rows)

The hidden BacktestPage + /backtest/* API + testback/ package were removed
(OPT-059). Fresh databases no longer create these tables (baseline DDL
updated in db/schema_baseline.py); this migration removes them from
existing databases.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0017_drop_backtest_tables"
down_revision: str | Sequence[str] | None = "0016_news_items_actionability"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS backtest_trade;")
    op.execute("DROP TABLE IF EXISTS backtest_run;")


def downgrade() -> None:
    # Recreating exact legacy DDL here is not worth it: the tables are only
    # used by the removed testback/ framework. Fresh installs never had them
    # in baseline (see db/schema_baseline.py).
    pass
