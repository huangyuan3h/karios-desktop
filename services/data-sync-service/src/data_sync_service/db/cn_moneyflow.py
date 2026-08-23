"""CN moneyflow — per-stock daily buy/sell by size."""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_moneyflow"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_moneyflow_ts_date ON {TABLE_NAME}(ts_code, trade_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def _date(s):
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
    vals = []
    for r in rows:
        td = _date(r.get("trade_date"))
        ts = str(r.get("ts_code") or "").strip()
        if not td or not ts:
            continue
        vals.append((td, ts, r.get("buy_sm_amount"), r.get("sell_sm_amount"), r.get("buy_md_amount"), r.get("sell_md_amount"), r.get("buy_lg_amount"), r.get("sell_lg_amount"), r.get("buy_elg_amount"), r.get("sell_elg_amount"), r.get("net_mf_amount"), r.get("net_mf_vol")))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(trade_date, ts_code, buy_sm_amount, sell_sm_amount, buy_md_amount, sell_md_amount, buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount, net_mf_amount, net_mf_vol, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET buy_sm_amount=excluded.buy_sm_amount, sell_sm_amount=excluded.sell_sm_amount, buy_md_amount=excluded.buy_md_amount, sell_md_amount=excluded.sell_md_amount, buy_lg_amount=excluded.buy_lg_amount, sell_lg_amount=excluded.sell_lg_amount, buy_elg_amount=excluded.buy_elg_amount, sell_elg_amount=excluded.sell_elg_amount, net_mf_amount=excluded.net_mf_amount, net_mf_vol=excluded.net_mf_vol, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
