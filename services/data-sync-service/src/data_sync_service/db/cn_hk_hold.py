"""CN hk_hold — northbound holding per stock daily."""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_hk_hold"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    trade_date DATE NOT NULL,
    ts_code    TEXT NOT NULL,
    vol        DOUBLE PRECISION,
    ratio      DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_cn_hk_hold_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_hk_hold_ts_date ON {TABLE_NAME}(ts_code, trade_date DESC);
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
        vals.append((td, ts, r.get("vol"), r.get("ratio")))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(trade_date, ts_code, vol, ratio, updated_at)
                VALUES (%s,%s,%s,%s, now())
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET vol=excluded.vol, ratio=excluded.ratio, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)
