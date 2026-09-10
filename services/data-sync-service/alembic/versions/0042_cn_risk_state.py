"""0042 TIP-017 risk-state tables (2026-09-09).

Four daily series backing the risk-state sensor family:
cn_etf_share (国家队代理, fund_share 2018+), cn_margin_total (margin 2021+),
cn_moneyflow_hsgt (north-bound 2021+), global_index_daily (HSI/HSTECH via
index_global 2018+ — deliberately separate from macro_daily so the frozen
HK baseline's fail-closed regime behavior is untouched).
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0042_cn_risk_state"
down_revision: str | Sequence[str] | None = "0041_user_trades_leg"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_etf_share (
        trade_date   DATE NOT NULL,
        ts_code      TEXT NOT NULL,
        fd_share     DOUBLE PRECISION,
        fund_type    TEXT,
        market       TEXT,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_etf_share_date ON cn_etf_share(trade_date DESC);
    """)
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_margin_total (
        trade_date   DATE NOT NULL,
        exchange_id  TEXT NOT NULL,
        rzye         DOUBLE PRECISION,
        rzmre        DOUBLE PRECISION,
        rzche        DOUBLE PRECISION,
        rqye         DOUBLE PRECISION,
        rqmcl        DOUBLE PRECISION,
        rzrqye       DOUBLE PRECISION,
        rqyl         DOUBLE PRECISION,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, exchange_id)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_margin_total_date ON cn_margin_total(trade_date DESC);
    """)
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_moneyflow_hsgt (
        trade_date   DATE NOT NULL,
        ggt_ss       DOUBLE PRECISION,
        ggt_sz       DOUBLE PRECISION,
        hgt          DOUBLE PRECISION,
        sgt          DOUBLE PRECISION,
        north_money  DOUBLE PRECISION,
        south_money  DOUBLE PRECISION,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_hsgt_date ON cn_moneyflow_hsgt(trade_date DESC);
    """)
    op.execute("""
    CREATE TABLE IF NOT EXISTS global_index_daily (
        trade_date   DATE NOT NULL,
        ts_code      TEXT NOT NULL,
        open         DOUBLE PRECISION,
        high         DOUBLE PRECISION,
        low          DOUBLE PRECISION,
        close        DOUBLE PRECISION,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (trade_date, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_global_index_date ON global_index_daily(trade_date DESC);
    """)


def downgrade() -> None:
    for t in ("global_index_daily", "cn_moneyflow_hsgt", "cn_margin_total", "cn_etf_share"):
        op.execute(f"DROP TABLE IF EXISTS {t};")
