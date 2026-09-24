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
from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

USER_TRADES_TABLE = "user_trades"

SIDE_BUY = "BUY"
SIDE_ADD = "ADD"
SIDE_SELL = "SELL"
SIDES = (SIDE_BUY, SIDE_ADD, SIDE_SELL)

LEG_S3 = "s3"
LEG_PARKING = "parking"
LEG_SATELLITE = "satellite"
LEG_H2 = "h2"
LEG_B3 = "b3"
LEGS = (LEG_S3, LEG_PARKING, LEG_SATELLITE, LEG_H2, LEG_B3)

STRATEGY_MODE_LEGACY = "legacy_unknown"
STRATEGY_MODES = (
    "harbor",
    "homeport",
    "starport",
    "starship",
    "starship_robust",
    "twin_star",
    STRATEGY_MODE_LEGACY,
)

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
    alpha_snapshot JSONB,
    leg           TEXT NOT NULL DEFAULT 's3',
    strategy_mode TEXT NOT NULL DEFAULT 'legacy_unknown',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT user_trades_leg_check CHECK (leg IN ('s3', 'parking', 'satellite', 'h2', 'b3')),
    CONSTRAINT user_trades_strategy_mode_check CHECK (
        strategy_mode IN ('harbor', 'homeport', 'starport', 'starship', 'starship_robust', 'twin_star', 'legacy_unknown')
    )
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
        "leg": row.get("leg") or LEG_S3,
        "strategyMode": row.get("strategy_mode") or STRATEGY_MODE_LEGACY,
        "alphaSnapshot": row["alpha_snapshot"],
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
    alpha_snapshot: dict[str, Any] | None = None,
    leg: str = LEG_S3,
    strategy_mode: str = STRATEGY_MODE_LEGACY,
) -> dict[str, Any]:
    """Insert one trade leg and return the normalized row."""
    if side not in SIDES:
        raise ValueError(f"invalid side: {side}")
    if leg not in LEGS:
        raise ValueError(f"invalid leg: {leg}")
    if strategy_mode not in STRATEGY_MODES:
        raise ValueError(f"invalid strategy_mode: {strategy_mode}")
    trade_id = str(uuid.uuid4())
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            INSERT INTO {USER_TRADES_TABLE} (
                id, symbol, side, trade_date, price, position_pct,
                cost_basis, entry_date, pnl_pct, holding_days, source, market,
                note, alpha_snapshot, leg, strategy_mode
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                Json(alpha_snapshot) if alpha_snapshot else None,
                leg,
                strategy_mode,
            ),
        )
        row = cur.fetchone()
    assert row is not None
    return _normalize_row(dict(row))


def list_trades(
    *,
    limit: int = 50,
    symbol: str | None = None,
    leg: str | None = None,
    strategy_mode: str | None = None,
) -> list[dict[str, Any]]:
    """List legs newest first. Use dict_row so column order never matters."""
    limit = max(1, min(int(limit), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if symbol:
        clauses.append("symbol = %s")
        params.append(symbol)
    if leg:
        clauses.append("leg = %s")
        params.append(leg)
    if strategy_mode:
        clauses.append("strategy_mode = %s")
        params.append(strategy_mode)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f"""
        SELECT * FROM {USER_TRADES_TABLE}
        {where}
        ORDER BY trade_date DESC, created_at DESC
        LIMIT %s
    """
    params.append(limit)
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [_normalize_row(dict(r)) for r in rows]


def delete_trade(trade_id: str) -> bool:
    """Delete one leg (corrections only). Returns True if a row was removed."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {USER_TRADES_TABLE} WHERE id = %s", (trade_id,))
        return cur.rowcount > 0


def update_trade(
    trade_id: str,
    *,
    leg: str | None = None,
    strategy_mode: str | None = None,
    position_pct: float | None = None,
    note: str | None = None,
) -> dict[str, Any] | None:
    """Correct a journal leg. Only leg/strategy/position_pct/note are mutable —
    side/symbol/trade_date never change so audit pairing stays intact.
    Returns the normalized row, or None if the id does not exist.
    """
    sets: list[str] = []
    params: list[Any] = []
    if leg is not None:
        if leg not in LEGS:
            raise ValueError(f"invalid leg: {leg}")
        sets.append("leg = %s")
        params.append(leg)
    if strategy_mode is not None:
        if strategy_mode not in STRATEGY_MODES:
            raise ValueError(f"invalid strategy_mode: {strategy_mode}")
        sets.append("strategy_mode = %s")
        params.append(strategy_mode)
    if position_pct is not None:
        if not position_pct > 0:
            raise ValueError("position_pct must be positive")
        sets.append("position_pct = %s")
        params.append(position_pct)
    if note is not None:
        sets.append("note = %s")
        params.append(note)
    if not sets:
        raise ValueError("nothing to update")
    params.append(trade_id)
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            UPDATE {USER_TRADES_TABLE}
            SET {", ".join(sets)}, updated_at = now()
            WHERE id = %s
            RETURNING *
            """,
            params,
        )
        row = cur.fetchone()
    if row is None:
        return None
    return _normalize_row(dict(row))


def latest_buy_leg(symbol: str, *, strategy_mode: str | None = None) -> str:
    """Leg of the newest BUY for symbol (SELL/ADD inherit it when leg is omitted)."""
    with get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        try:
            if strategy_mode:
                cur.execute(
                    f"""
                    SELECT leg FROM {USER_TRADES_TABLE}
                    WHERE symbol = %s AND side = 'BUY' AND strategy_mode = %s
                    ORDER BY trade_date DESC, created_at DESC LIMIT 1
                    """,
                    (symbol, strategy_mode),
                )
            else:
                cur.execute(
                    f"""
                    SELECT leg FROM {USER_TRADES_TABLE}
                    WHERE symbol = %s AND side = 'BUY'
                    ORDER BY trade_date DESC, created_at DESC LIMIT 1
                    """,
                    (symbol,),
                )
            row = cur.fetchone()
        except Exception:
            return LEG_S3  # pre-0041 DBs have no leg column
    if not row:
        return LEG_S3
    return str(dict(row).get("leg") or LEG_S3)


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
