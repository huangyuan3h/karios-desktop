"""0043 cn_flow_daily — daily size-class flow rollup of cn_moneyflow.

TIP-017 C (散户共振) display layer: one row per day with sm/md/lg/elg net
amounts + turnover proxy, so the twin-star timeline (and future C-factor
IC screens) never re-aggregate the 3M-row per-stock table per request.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0043_cn_flow_daily"
down_revision: str | Sequence[str] | None = "0042_cn_risk_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_flow_daily (
        trade_date   DATE NOT NULL,
        sm_net       DOUBLE PRECISION,
        md_net       DOUBLE PRECISION,
        lg_net       DOUBLE PRECISION,
        elg_net      DOUBLE PRECISION,
        turnover     DOUBLE PRECISION,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_flow_daily_date ON cn_flow_daily(trade_date DESC);
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS cn_flow_daily")
