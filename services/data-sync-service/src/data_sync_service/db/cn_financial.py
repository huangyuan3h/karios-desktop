"""CN financials (fina_indicator) — PiT ann_date key.

Source: tushare fina_indicator (per ts_code, period). Stored per (ts_code, ann_date, end_date)
so ML can forward-fill by ann_date without lookahead. Only key predictors kept;
extra JSON kept for future factors. Quarterly, ~320k rows for 2015-2025.
"""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_financial"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

CREATE INDEX IF NOT EXISTS idx_cn_financial_ann_date ON {TABLE_NAME}(ann_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_financial_ts_end ON {TABLE_NAME}(ts_code, end_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def _date(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def upsert_rows(rows: list[dict]) -> int:
    ensure_table()
    if not rows:
        return 0
    import json

    vals = []
    for r in rows:
        ts = str(r.get("ts_code") or "").strip()
        ann = _date(r.get("ann_date"))
        end = _date(r.get("end_date"))
        if not ts or not ann or not end:
            continue
        vals.append(
            (
                ts,
                ann,
                end,
                r.get("eps"),
                r.get("dt_eps"),
                r.get("bps"),
                r.get("roe"),
                r.get("roa"),
                r.get("gross_margin"),
                r.get("netprofit_margin"),
                r.get("profit_dedt"),
                r.get("op_income"),
                r.get("debt_to_assets"),
                r.get("ocf_to_or"),
                r.get("netprofit_yoy"),
                r.get("tr_yoy"),
                r.get("or_yoy"),
                r.get("basic_eps_yoy"),
                r.get("q_netprofit_yoy"),
                r.get("q_sales_yoy"),
                r.get("q_profit_yoy"),
                r.get("q_roe"),
                str(r.get("update_flag") or "") or None,
                json.dumps(r.get("extra") or {}, ensure_ascii=False),
            )
        )
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (
                    ts_code, ann_date, end_date, eps, dt_eps, bps, roe, roa,
                    gross_margin, netprofit_margin, profit_dedt, op_income,
                    debt_to_assets, ocf_to_or, netprofit_yoy, tr_yoy, or_yoy,
                    basic_eps_yoy, q_netprofit_yoy, q_sales_yoy, q_profit_yoy, q_roe,
                    update_flag, extra, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now())
                ON CONFLICT (ts_code, ann_date, end_date) DO UPDATE SET
                    eps=excluded.eps, dt_eps=excluded.dt_eps, bps=excluded.bps,
                    roe=excluded.roe, roa=excluded.roa, gross_margin=excluded.gross_margin,
                    netprofit_margin=excluded.netprofit_margin, profit_dedt=excluded.profit_dedt,
                    op_income=excluded.op_income, debt_to_assets=excluded.debt_to_assets,
                    ocf_to_or=excluded.ocf_to_or, netprofit_yoy=excluded.netprofit_yoy,
                    tr_yoy=excluded.tr_yoy, or_yoy=excluded.or_yoy, basic_eps_yoy=excluded.basic_eps_yoy,
                    q_netprofit_yoy=excluded.q_netprofit_yoy, q_sales_yoy=excluded.q_sales_yoy,
                    q_profit_yoy=excluded.q_profit_yoy, q_roe=excluded.q_roe,
                    update_flag=excluded.update_flag, extra=excluded.extra, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
