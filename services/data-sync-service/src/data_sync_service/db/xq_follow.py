"""Snowball (雪球) stock-follow snapshot — daily retail-attention panel.

Source: akshare stock_hot_follow_xq("最热门") — full A-share cross-section of
cumulative follow counts. No history API: the panel only grows forward, one
snapshot per calendar day (weekend/holiday rows included; consumers join
A-share sessions). New table for the P0-13 retail-attention line; the Eastmoney
rank panel (cn_hot_rank, D1) is a separate source.
"""

from __future__ import annotations

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "cn_xq_follow"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code     TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    follow      DOUBLE PRECISION,
    price       DOUBLE PRECISION,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_cn_xq_follow_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_xq_follow_ts_date ON {TABLE_NAME}(ts_code, trade_date DESC);
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        try:
            f = float(str(v).replace(",", "").strip())
        except (TypeError, ValueError):
            return None
    return f if f == f else None


def upsert_rows(rows: list[dict], trade_date: str) -> int:
    ensure_table()
    if not rows:
        return 0
    vals = []
    for r in rows:
        ts = str(r.get("ts_code") or "").strip()
        if not ts:
            continue
        vals.append((ts, trade_date, _num(r.get("follow")), _num(r.get("price"))))
    if not vals:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(ts_code, trade_date, follow, price, updated_at)
                VALUES (%s,%s,%s,%s, now())
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    follow=excluded.follow, price=excluded.price, updated_at=now()
                """,
                vals,
            )
        conn.commit()
    return len(vals)


def last_snapshot_date() -> str | None:
    ensure_table()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT max(trade_date) FROM {TABLE_NAME}")
                row = cur.fetchone()
                return str(row[0]) if row and row[0] else None
    except Exception:
        return None
