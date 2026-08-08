"""user_trades — real (user-entered) trade journal.

Records the user's ACTUAL buys / adds / sells from the watchlist UI,
separate from ``paper_trades`` (which logs the system's simulated signals).

Each row is one leg:
- ``BUY``  — opening entry (user enters cost price + position pct on a
  watchlist row that had no position).
- ``ADD``  — adding to an existing open position. The UI detects this when
  cost price is set on a row that already has positionPct > 0 + entryDate,
  blends the weighted average cost client-side and records the leg.
- ``SELL`` — closing (full or partial). Records the exit price + pct sold;
  ``pnl_pct`` / ``holding_days`` are computed against the cost basis the
  client sends (the blended cost + original entry date).

Design rules:

- The watchlist registry stays the source of truth for *current* positions;
  this table is an append-only journal (delete-by-id only for corrections).
- ``pnl_pct`` on SELL rows is GROSS (no cost model). The 0.3% round-trip
  cost is applied at display time in the expectancy board, so the user sees
  net expectancy = win_rate*avg_win − loss_rate*avg_loss − costs.
- Stats are computed in ``service/user_trades_stats`` from this table.
"""

from __future__ import annotations

import uuid
from typing import Any

from psycopg.rows import dict_row

from data_sync_service.db import get_connection

USER_TRADES_TABLE = "user_trades"

SIDE_BUY = "BUY"
SIDE_ADD = "ADD"
SIDE_SELL = "SELL"
SIDES = (SIDE_BUY, SIDE_ADD, SIDE_SELL)

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {USER_TRADES_TABLE} (
    id            TEXT PRIMARY KEY,
    symbol        TEXT NOT NULL,
    side          TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    position_pct  DOUBLE PRECISION NOT NULL,
    cost_basis    DOUBLE PRECISION,
    entry_date    TEXT,
    pnl_pct       DOUBLE PRECISION,
    holding_days  INTEGER,
    source        TEXT,
    market        TEXT NOT NULL DEFAULT 'CN',
    note          TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_trades_symbol_date
    ON {USER_TRADES_TABLE}(symbol, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_user_trades_date
    ON {USER_TRADES_TABLE}(trade_date DESC);
"""


def ensure_tables() -> None:
    from data_sync_service.db._ensure_guard import ensure_once

    ensure_once(USER_TRADES_TABLE, _ensure_table_impl)


def _ensure_table_impl() -> None:
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(CREATE_SQL)


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "side": row["side"],
        "tradeDate": row["trade_date"],
        "price": row["price"],
        "positionPct": row["position_pct"],
        "costBasis": row["cost_basis"],
        "entryDate": row["entry_date"],
        "pnlPct": row["pnl_pct"],
        "holdingDays": row["holding_days"],
        "source": row["source"],
        "market": row["market"],
        "note": row["note"],
        "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
    }


def insert_trade(
    *,
    symbol: str,
    side: str,
    trade_date: str,
    price: float,
    position_pct: float,
    cost_basis: float | None = None,
    entry_date: str | None = None,
    pnl_pct: float | None = None,
    holding_days: int | None = None,
    source: str | None = None,
    market: str = "CN",
    note: str | None = None,
) -> dict[str, Any]:
    """Insert one trade leg and return the normalized row."""
    if side not in SIDES:
        raise ValueError(f"invalid side: {side}")
    trade_id = str(uuid.uuid4())
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            INSERT INTO {USER_TRADES_TABLE} (
                id, symbol, side, trade_date, price, position_pct,
                cost_basis, entry_date, pnl_pct, holding_days, source, market, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                trade_id,
                symbol,
                side,
                trade_date,
                price,
                position_pct,
                cost_basis,
                entry_date,
                pnl_pct,
                holding_days,
                source,
                market,
                note,
            ),
        )
        row = cur.fetchone()
    assert row is not None
    return _normalize_row(dict(row))


def list_trades(*, limit: int = 50, symbol: str | None = None) -> list[dict[str, Any]]:
    """List legs newest first. Use dict_row so column order never matters."""
    limit = max(1, min(int(limit), 500))
    sql = f"""
        SELECT * FROM {USER_TRADES_TABLE}
        {("WHERE symbol = %s" if symbol else "")}
        ORDER BY trade_date DESC, created_at DESC
        LIMIT %s
    """
    params: list[Any] = []
    if symbol:
        params.append(symbol)
    params.append(limit)
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [_normalize_row(dict(r)) for r in rows]


def delete_trade(trade_id: str) -> bool:
    """Delete one leg (corrections only). Returns True if a row was removed."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {USER_TRADES_TABLE} WHERE id = %s", (trade_id,)
        )
        return cur.rowcount > 0


def fetch_sell_rows() -> list[dict[str, Any]]:
    """All SELL legs, oldest first (inputs to expectancy stats)."""
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT * FROM {USER_TRADES_TABLE}
            WHERE side = 'SELL'
            ORDER BY trade_date ASC, created_at ASC
            """
        )
        rows = cur.fetchall()
    return [_normalize_row(dict(r)) for r in rows]
