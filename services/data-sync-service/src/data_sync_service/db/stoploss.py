"""StopLoss storage: persist stoploss prices per stock.

Rule: stoploss can only increase, never decrease.
When a new stoploss is computed, we compare with the stored value and keep the higher one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "stock_stoploss"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code         TEXT PRIMARY KEY,
    stop_loss_price NUMERIC(12, 6) NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    as_of_date      DATE
);
"""

CREATE_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS idx_stock_stoploss_updated_at ON {TABLE_NAME} (updated_at);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            cur.execute(CREATE_INDEX_SQL)
        conn.commit()


def get_stoploss(ts_code: str) -> dict[str, Any] | None:
    """Get stored stoploss for a single stock."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, stop_loss_price, updated_at, as_of_date FROM {TABLE_NAME} WHERE ts_code = %s",
                (ts_code,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "ts_code": row[0],
        "stop_loss_price": float(row[1]) if row[1] is not None else None,
        "updated_at": row[2].isoformat() if row[2] else None,
        "as_of_date": str(row[3]) if row[3] else None,
    }


def get_stoploss_batch(ts_codes: list[str]) -> dict[str, dict[str, Any]]:
    """Get stored stoploss for multiple stocks. Returns {ts_code: record}."""
    if not ts_codes:
        return {}
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, stop_loss_price, updated_at, as_of_date FROM {TABLE_NAME} WHERE ts_code = ANY(%s)",
                (ts_codes,),
            )
            rows = cur.fetchall()
    return {
        row[0]: {
            "ts_code": row[0],
            "stop_loss_price": float(row[1]) if row[1] is not None else None,
            "updated_at": row[2].isoformat() if row[2] else None,
            "as_of_date": str(row[3]) if row[3] else None,
        }
        for row in rows
        if row and row[0]
    }


def upsert_stoploss(ts_code: str, stop_loss_price: float, as_of_date: str | None = None) -> None:
    """Upsert stoploss for a single stock."""
    ensure_table()
    now = datetime.utcnow()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (ts_code, stop_loss_price, updated_at, as_of_date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ts_code) DO UPDATE SET
                    stop_loss_price = EXCLUDED.stop_loss_price,
                    updated_at = EXCLUDED.updated_at,
                    as_of_date = EXCLUDED.as_of_date
                """,
                (ts_code, stop_loss_price, now, as_of_date),
            )
        conn.commit()


def compute_effective_stoploss(
    ts_code: str,
    newly_computed: float,
    as_of_date: str | None = None,
) -> tuple[float, bool]:
    """
    Compute effective stoploss: max(stored, newly_computed).
    Returns (effective_stoploss, was_upgraded) where was_upgraded is True if stored value was used.
    If newly_computed > stored, updates the stored value.
    """
    stored = get_stoploss(ts_code)
    stored_price = stored.get("stop_loss_price") if stored else None

    if stored_price is None:
        upsert_stoploss(ts_code, newly_computed, as_of_date)
        return newly_computed, False

    if newly_computed > stored_price:
        upsert_stoploss(ts_code, newly_computed, as_of_date)
        return newly_computed, False

    return stored_price, True