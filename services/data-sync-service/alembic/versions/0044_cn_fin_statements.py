"""0044 cn_fin_statements — raw three financial statements (4y window).

TIP/P15 quality-factor input: tushare balancesheet / income / cashflow per
ts_code, stored per (ts_code, ann_date, end_date, report_type) for PiT
forward-fill. Hot subjects typed; full source row kept in extra JSONB.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0044_cn_fin_statements"
down_revision: str | Sequence[str] | None = "0043_cn_flow_daily"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS cn_balance_sheet (
        ts_code            TEXT NOT NULL,
        ann_date           DATE NOT NULL,
        end_date           DATE NOT NULL,
        report_type        TEXT NOT NULL DEFAULT '1',
        f_ann_date         DATE,
        comp_type          TEXT,
        end_type           TEXT,
        total_assets       DOUBLE PRECISION,
        total_cur_assets   DOUBLE PRECISION,
        money_cap          DOUBLE PRECISION,
        accounts_receiv    DOUBLE PRECISION,
        oth_receiv         DOUBLE PRECISION,
        inventories        DOUBLE PRECISION,
        goodwill           DOUBLE PRECISION,
        intan_assets       DOUBLE PRECISION,
        total_liab         DOUBLE PRECISION,
        total_cur_liab     DOUBLE PRECISION,
        st_borr            DOUBLE PRECISION,
        lt_borr            DOUBLE PRECISION,
        bond_payable       DOUBLE PRECISION,
        total_hldr_eqy_inc_min_int DOUBLE PRECISION,
        total_hldr_eqy_exc_min_int DOUBLE PRECISION,
        update_flag        TEXT,
        extra              JSONB,
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (ts_code, ann_date, end_date, report_type)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_balance_ann_date ON cn_balance_sheet(ann_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cn_balance_ts_end ON cn_balance_sheet(ts_code, end_date DESC);

    CREATE TABLE IF NOT EXISTS cn_income_stmt (
        ts_code            TEXT NOT NULL,
        ann_date           DATE NOT NULL,
        end_date           DATE NOT NULL,
        report_type        TEXT NOT NULL DEFAULT '1',
        f_ann_date         DATE,
        comp_type          TEXT,
        end_type           TEXT,
        basic_eps          DOUBLE PRECISION,
        diluted_eps        DOUBLE PRECISION,
        total_revenue      DOUBLE PRECISION,
        revenue            DOUBLE PRECISION,
        total_cogs         DOUBLE PRECISION,
        oper_cost          DOUBLE PRECISION,
        sell_exp           DOUBLE PRECISION,
        admin_exp          DOUBLE PRECISION,
        fin_exp            DOUBLE PRECISION,
        rd_exp             DOUBLE PRECISION,
        operate_profit     DOUBLE PRECISION,
        total_profit       DOUBLE PRECISION,
        income_tax         DOUBLE PRECISION,
        n_income           DOUBLE PRECISION,
        n_income_attr_p    DOUBLE PRECISION,
        ebit               DOUBLE PRECISION,
        ebitda             DOUBLE PRECISION,
        update_flag        TEXT,
        extra              JSONB,
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (ts_code, ann_date, end_date, report_type)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_income_ann_date ON cn_income_stmt(ann_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cn_income_ts_end ON cn_income_stmt(ts_code, end_date DESC);

    CREATE TABLE IF NOT EXISTS cn_cashflow_stmt (
        ts_code            TEXT NOT NULL,
        ann_date           DATE NOT NULL,
        end_date           DATE NOT NULL,
        report_type        TEXT NOT NULL DEFAULT '1',
        f_ann_date         DATE,
        comp_type          TEXT,
        end_type           TEXT,
        n_cashflow_act     DOUBLE PRECISION,
        n_cashflow_inv_act DOUBLE PRECISION,
        n_cash_flows_fnc_act DOUBLE PRECISION,
        free_cashflow      DOUBLE PRECISION,
        c_cash_equ_end_period DOUBLE PRECISION,
        c_cash_equ_beg_period DOUBLE PRECISION,
        update_flag        TEXT,
        extra              JSONB,
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (ts_code, ann_date, end_date, report_type)
    );
    CREATE INDEX IF NOT EXISTS idx_cn_cashflow_ann_date ON cn_cashflow_stmt(ann_date DESC);
    CREATE INDEX IF NOT EXISTS idx_cn_cashflow_ts_end ON cn_cashflow_stmt(ts_code, end_date DESC);
    """)


def downgrade() -> None:
    op.execute("""
    DROP TABLE IF EXISTS cn_cashflow_stmt;
    DROP TABLE IF EXISTS cn_income_stmt;
    DROP TABLE IF EXISTS cn_balance_sheet;
    """)
