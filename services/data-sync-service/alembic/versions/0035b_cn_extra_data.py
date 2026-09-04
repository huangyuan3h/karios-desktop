"""Add CN extra data tables: financial/holder/margin/moneyflow/hk_hold.

Revision ID: 0020_cn_extra_data (kept stable — stamped DBs key on this, NOT
the filename; the file was renamed from 0020_cn_extra_data.py to 0035b_ on
2026-09-04 (OPT-142) to fix the duplicate 0020 file number).
Revises: 0035_stock_forecast
Create Date: 2026-08-23

Quarterly: cn_financial (fina_indicator), cn_holder_number
Daily: cn_margin_detail, cn_moneyflow, cn_hk_hold
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0020_cn_extra_data"
down_revision: str | Sequence[str] | None = "0035_stock_forecast"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS cn_financial (
            ts_code            TEXT NOT NULL,
            ann_date           DATE NOT NULL,
            end_date           DATE NOT NULL,
            eps                DOUBLE PRECISION,
            dt_eps             DOUBLE PRECISION,
            bps                DOUBLE PRECISION,
            roe                DOUBLE PRECISION,
            roa                DOUBLE PRECISION,
            gross_margin       DOUBLE PRECISION,
            netprofit_margin   DOUBLE PRECISION,
            profit_dedt        DOUBLE PRECISION,
            op_income          DOUBLE PRECISION,
            debt_to_assets     DOUBLE PRECISION,
            ocf_to_or          DOUBLE PRECISION,
            netprofit_yoy      DOUBLE PRECISION,
            tr_yoy             DOUBLE PRECISION,
            or_yoy             DOUBLE PRECISION,
            basic_eps_yoy      DOUBLE PRECISION,
            q_netprofit_yoy    DOUBLE PRECISION,
            q_sales_yoy        DOUBLE PRECISION,
            q_profit_yoy       DOUBLE PRECISION,
            q_roe              DOUBLE PRECISION,
            update_flag        TEXT,
            extra              JSONB,
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ts_code, ann_date, end_date)
        );
        CREATE INDEX IF NOT EXISTS idx_cn_financial_ann_date ON cn_financial(ann_date DESC);
        CREATE INDEX IF NOT EXISTS idx_cn_financial_ts_end ON cn_financial(ts_code, end_date DESC);

        CREATE TABLE IF NOT EXISTS cn_holder_number (
            ts_code     TEXT NOT NULL,
            ann_date    DATE NOT NULL,
            end_date    DATE NOT NULL,
            holder_num  DOUBLE PRECISION,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (ts_code, ann_date, end_date)
        );
        CREATE INDEX IF NOT EXISTS idx_cn_holder_ann ON cn_holder_number(ann_date DESC);

        CREATE TABLE IF NOT EXISTS cn_margin_detail (
            trade_date DATE NOT NULL,
            ts_code    TEXT NOT NULL,
            rzye       DOUBLE PRECISION,
            rqye       DOUBLE PRECISION,
            rzmre      DOUBLE PRECISION,
            rqyl       DOUBLE PRECISION,
            rzche      DOUBLE PRECISION,
            rqchl      DOUBLE PRECISION,
            rqmcl      DOUBLE PRECISION,
            rzrqye     DOUBLE PRECISION,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (trade_date, ts_code)
        );
        CREATE INDEX IF NOT EXISTS idx_cn_margin_date ON cn_margin_detail(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_cn_margin_ts_date ON cn_margin_detail(ts_code, trade_date DESC);

        CREATE TABLE IF NOT EXISTS cn_moneyflow (
            trade_date       DATE NOT NULL,
            ts_code          TEXT NOT NULL,
            buy_sm_amount    DOUBLE PRECISION,
            sell_sm_amount   DOUBLE PRECISION,
            buy_md_amount    DOUBLE PRECISION,
            sell_md_amount   DOUBLE PRECISION,
            buy_lg_amount    DOUBLE PRECISION,
            sell_lg_amount   DOUBLE PRECISION,
            buy_elg_amount   DOUBLE PRECISION,
            sell_elg_amount  DOUBLE PRECISION,
            net_mf_amount    DOUBLE PRECISION,
            net_mf_vol       DOUBLE PRECISION,
            updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (trade_date, ts_code)
        );
        CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_date ON cn_moneyflow(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_ts_date ON cn_moneyflow(ts_code, trade_date DESC);

        CREATE TABLE IF NOT EXISTS cn_hk_hold (
            trade_date DATE NOT NULL,
            ts_code    TEXT NOT NULL,
            vol        DOUBLE PRECISION,
            ratio      DOUBLE PRECISION,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (trade_date, ts_code)
        );
        CREATE INDEX IF NOT EXISTS idx_cn_hk_hold_date ON cn_hk_hold(trade_date DESC);
        CREATE INDEX IF NOT EXISTS idx_cn_hk_hold_ts_date ON cn_hk_hold(ts_code, trade_date DESC);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS cn_hk_hold;
        DROP TABLE IF EXISTS cn_moneyflow;
        DROP TABLE IF EXISTS cn_margin_detail;
        DROP TABLE IF EXISTS cn_holder_number;
        DROP TABLE IF EXISTS cn_financial;
        """
    )
