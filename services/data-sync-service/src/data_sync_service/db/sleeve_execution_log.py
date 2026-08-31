"""sleeve_execution_log — manual ETF/择强 fill outcomes (credibility phase).

Records whether the user could actually trade the day's pick (filled /
partial / failed / skipped), optional premium bps, and a free-text reason.
Not a gate — logging only so <20-trade windows still accumulate execution
evidence before C4 statistics.
"""

from __future__ import annotations

import uuid
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Json

from data_sync_service.db import get_connection

TABLE = "sleeve_execution_log"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id              TEXT PRIMARY KEY,
    trade_date      TEXT NOT NULL,
    pick_key        TEXT NOT NULL,
    symbol          TEXT,
    status          TEXT NOT NULL,
    premium_bps     DOUBLE PRECISION,
    signal_price    DOUBLE PRECISION,
    fill_price      DOUBLE PRECISION,
    note            TEXT,
    meta            JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sleeve_exec_date
    ON {TABLE}(trade_date DESC);
"""

STATUSES = frozenset({"filled", "partial", "failed", "skipped"})


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in CREATE_SQL.split(";"):
                part = stmt.strip()
                if part:
                    cur.execute(part)
        conn.commit()


def insert_event(
    *,
    trade_date: str,
    pick_key: str,
    status: str,
    symbol: str | None = None,
    premium_bps: float | None = None,
    signal_price: float | None = None,
    fill_price: float | None = None,
    note: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if status not in STATUSES:
        raise ValueError(f"status must be one of {sorted(STATUSES)} (got {status!r})")
    ensure_table()
    new_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE}
                    (id, trade_date, pick_key, symbol, status, premium_bps,
                     signal_price, fill_price, note, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    new_id,
                    trade_date[:10],
                    pick_key.upper(),
                    symbol,
                    status,
                    premium_bps,
                    signal_price,
                    fill_price,
                    note,
                    Json(meta) if meta else None,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _row(row) if row else {"id": new_id}


def list_events(*, limit: int = 30) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT * FROM {TABLE}
                ORDER BY trade_date DESC, created_at DESC
                LIMIT %s
                """,
                (max(1, min(int(limit), 200)),),
            )
            rows = cur.fetchall()
    return [_row(r) for r in rows]


def _row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {
        "id": row.get("id"),
        "tradeDate": row.get("trade_date"),
        "pickKey": row.get("pick_key"),
        "symbol": row.get("symbol"),
        "status": row.get("status"),
        "premiumBps": float(row["premium_bps"]) if row.get("premium_bps") is not None else None,
        "signalPrice": float(row["signal_price"]) if row.get("signal_price") is not None else None,
        "fillPrice": float(row["fill_price"]) if row.get("fill_price") is not None else None,
        "note": row.get("note"),
        "meta": row.get("meta"),
        "createdAt": str(row.get("created_at") or ""),
    }
