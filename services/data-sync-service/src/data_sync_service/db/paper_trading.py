"""paper_trades table — virtual paper-trading log (OPT-049).

The table is a faithful record of what Karios' own BUY/ADD signals WOULD HAVE
done if the user had followed every recommendation. Each row is one entry:
- `entry_date` / `entry_price` / `side` are the simulated order.
- `close_date` / `close_price` / `pnl_pct` / `holding_days` are filled when the
  trade is closed (intake cron + update cron decide when).
- `close_reason` is one of: `target_hit` | `stop_hit` | `max_hold` | `score_floor`
  | `pool_exit` | `manual`. v0.1 emits `max_hold`, `stop_hit`, `target_hit`,
  `score_floor` and `pool_exit`.

v0.1 scope:
- **CN-only**. HK paper-trading needs FX, T+0/T+2 settlement differences,
  and a separate update cadence — punted to OPT-050+.
- **No live P&L aggregation here**. Stats are computed in
  ``service/paper_trading.compute_stats`` from this table.

Design rules (see docs/designs/api-contract.md for the broader pattern):

- The table is the **single source of truth** for paper-trade state. The
  service layer must not hold any state that isn't recoverable from it.
- The intake cron is **idempotent on `(symbol, entry_date, side)`** — re-running
  it on the same day never produces a duplicate row.
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any

from data_sync_service.db import get_connection

PAPER_TRADES_TABLE = "paper_trades"

# Status values. Treat as a closed enum: extending requires bumping the
# paper-trade API description + adding to CloseReasonLiteral.
STATUS_OPEN = "open"
STATUS_CLOSED = "closed"
STATUSES = (STATUS_OPEN, STATUS_CLOSED)

# Side values. v0 only emits BUY / ADD. ADD requires an existing open trade
# for the same symbol (service layer enforces).
SIDE_BUY = "BUY"
SIDE_ADD = "ADD"
SIDES = (SIDE_BUY, SIDE_ADD)

# Close reasons. Treat as a closed enum: extending requires bumping the
# paper-trade API description + adding to CloseReasonLiteral.
# v0 (OPT-049) emitted only `max_hold` and `stop_hit`; v0.1 (OPT-058) adds
# `target_hit`, `score_floor` and `pool_exit` (see service/paper_trading.py).
CLOSE_REASON_MAX_HOLD = "max_hold"  # holding_days >= MAX_HOLD_DAYS
CLOSE_REASON_STOP_HIT = "stop_hit"  # pnl_pct <= STOP_LOSS_PCT
CLOSE_REASON_TARGET_HIT = "target_hit"  # pnl_pct >= TARGET_PNL_PCT
CLOSE_REASON_SCORE_FLOOR = "score_floor"  # latest TrendOK score < SCORE_FLOOR
CLOSE_REASON_POOL_EXIT = "pool_exit"  # symbol purged from the watchlist registry
CLOSE_REASONS = (
    CLOSE_REASON_MAX_HOLD,
    CLOSE_REASON_STOP_HIT,
    CLOSE_REASON_TARGET_HIT,
    CLOSE_REASON_SCORE_FLOOR,
    CLOSE_REASON_POOL_EXIT,
)

# TIP-011: source attribution (provenance of the BUY/ADD signal).
SOURCE_TV = "TV"  # originated from TV screener (funnel)
SOURCE_ALPHA = "ALPHA"  # originated from Alpha Radar catalyst
SOURCE_MANUAL = "MANUAL"  # user / external AI agent added
SOURCES = (SOURCE_TV, SOURCE_ALPHA, SOURCE_MANUAL)

# v0.1 close thresholds. Kept module-level so tests can assert against the
# exact values and operators can tune them in one place.
MAX_HOLD_DAYS = 5
STOP_LOSS_PCT = -5.0  # i.e. pnl_pct <= -5% triggers stop_hit
TARGET_PNL_PCT = 10.0  # i.e. pnl_pct >= +10% triggers target_hit
SCORE_FLOOR = 30.0  # latest TrendOK score < 30 triggers score_floor

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {PAPER_TRADES_TABLE} (
    id              TEXT PRIMARY KEY,
    symbol          TEXT NOT NULL,
    entry_date      TEXT NOT NULL,
    side            TEXT NOT NULL,
    entry_price     DOUBLE PRECISION NOT NULL,
    score_at_entry  DOUBLE PRECISION,
    why_at_entry    TEXT,
    sleeve_pct      DOUBLE PRECISION,
    status          TEXT NOT NULL DEFAULT 'open',
    close_date      TEXT,
    close_price     DOUBLE PRECISION,
    pnl_pct         DOUBLE PRECISION,
    holding_days    INTEGER,
    close_reason    TEXT,
    source          TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency: the intake cron is keyed on (symbol, entry_date, side).
CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_trades_unique_entry
    ON {PAPER_TRADES_TABLE}(symbol, entry_date, side);

-- For the update cron / list-by-status.
CREATE INDEX IF NOT EXISTS idx_paper_trades_status_open
    ON {PAPER_TRADES_TABLE}(status, entry_date DESC)
    WHERE status = 'open';

-- For stats since a given date.
CREATE INDEX IF NOT EXISTS idx_paper_trades_close_date
    ON {PAPER_TRADES_TABLE}(close_date DESC)
    WHERE status = 'closed';

-- TIP-011: source attribution by provenance (TV / ALPHA / MANUAL).
CREATE INDEX IF NOT EXISTS idx_paper_trades_source
    ON {PAPER_TRADES_TABLE}(source, entry_date DESC)
    WHERE source IS NOT NULL;
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def insert_paper_trade(
    *,
    symbol: str,
    entry_date: str,
    side: str,
    entry_price: float,
    score_at_entry: float | None = None,
    why_at_entry: str | None = None,
    sleeve_pct: float | None = None,
    source: str | None = None,
) -> dict[str, Any] | None:
    """Insert one paper trade. Returns the row or ``None`` on idempotent skip.

    Idempotent on ``(symbol, entry_date, side)``: re-running intake for the
    same day never produces duplicates.

    ``source`` (TIP-011) is one of 'TV' / 'ALPHA' / 'MANUAL' (closed enum) or
    None for legacy rows that pre-date attribution.
    """
    if side not in SIDES:
        raise ValueError(f"side must be one of {SIDES} (got {side!r})")
    if source is not None and source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES} or None (got {source!r})")
    ensure_tables()
    new_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {PAPER_TRADES_TABLE}
                    (id, symbol, entry_date, side, entry_price, score_at_entry,
                     why_at_entry, sleeve_pct, status, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open', %s)
                ON CONFLICT (symbol, entry_date, side) DO NOTHING
                RETURNING *
                """,
                (
                    new_id,
                    symbol,
                    entry_date,
                    side,
                    float(entry_price),
                    score_at_entry,
                    why_at_entry,
                    sleeve_pct,
                    source,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    if row is None:
        return None
    return _row_to_dict(row)


def list_paper_trades(
    *,
    status: str | None = None,
    since: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """List paper trades, optionally filtered by status and entry_date >= since.

    Ordered by entry_date DESC, then created_at DESC. Used by the /v1 API.
    """
    ensure_tables()
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES} (got {status!r})")
        clauses.append("status = %s")
        params.append(status)
    if since:
        clauses.append("entry_date >= %s")
        params.append(since)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(int(limit))
    sql = f"""
        SELECT * FROM {PAPER_TRADES_TABLE}
        {where}
        ORDER BY entry_date DESC, created_at DESC
        LIMIT %s
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def get_open_paper_trades() -> list[dict[str, Any]]:
    """Return all open trades. Used by the update cron."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * FROM {PAPER_TRADES_TABLE}
                WHERE status = 'open'
                ORDER BY entry_date ASC
                """
            )
            rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def update_paper_trade_price(
    *,
    trade_id: str,
    close_price: float,
    pnl_pct: float,
    holding_days: int,
) -> dict[str, Any] | None:
    """Touch an open trade's latest close_price + pnl_pct + holding_days.

    Does NOT change status. Returns the updated row, or None if the trade is
    no longer open.
    """
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {PAPER_TRADES_TABLE}
                SET close_price = %s,
                    pnl_pct = %s,
                    holding_days = %s,
                    updated_at = now()
                WHERE id = %s AND status = 'open'
                RETURNING *
                """,
                (float(close_price), float(pnl_pct), int(holding_days), trade_id),
            )
            row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row) if row else None


def close_paper_trade(
    *,
    trade_id: str,
    close_date: str,
    close_price: float,
    pnl_pct: float,
    holding_days: int,
    close_reason: str,
) -> dict[str, Any] | None:
    """Close an open trade. Returns the updated row, or None if not open."""
    if close_reason not in CLOSE_REASONS:
        raise ValueError(f"close_reason must be one of {CLOSE_REASONS} (got {close_reason!r})")
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {PAPER_TRADES_TABLE}
                SET status = 'closed',
                    close_date = %s,
                    close_price = %s,
                    pnl_pct = %s,
                    holding_days = %s,
                    close_reason = %s,
                    updated_at = now()
                WHERE id = %s AND status = 'open'
                RETURNING *
                """,
                (
                    close_date,
                    float(close_price),
                    float(pnl_pct),
                    int(holding_days),
                    close_reason,
                    trade_id,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row) if row else None


def count_since(since: str) -> tuple[int, int]:
    """Return (closed_count, winning_count) for closed trades since `since`."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins
                FROM {PAPER_TRADES_TABLE}
                WHERE status = 'closed'
                  AND close_date >= %s
                """,
                (since,),
            )
            row = cur.fetchone()
    if row is None:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def avg_pnl_pct_since(since: str) -> float | None:
    """Mean pnl_pct across closed trades since `since`. None if no rows."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT AVG(pnl_pct)
                FROM {PAPER_TRADES_TABLE}
                WHERE status = 'closed'
                  AND close_date >= %s
                """,
                (since,),
            )
            row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------


def _row_to_dict(row: tuple | list) -> dict[str, Any]:
    """psycopg returns rows as tuples; map them to a stable camelCase dict.

    Field names use camelCase to stay byte-identical to the ``/v1/paper-trades``
    contract and the existing ``/watchlist/registry`` shape (per OPT-009 / shared).

    TIP-011 added the ``source`` column AT THE END of the table (after
    ``updated_at``). All other positions remain unchanged. To stay
    robust against future appended columns, the helper infers ``source``
    from the trailing slot; if a future migration shifts ``source`` again,
    update this single function.
    """
    # Detect the source column slot: scan the row for the first non-tuple
    # entry that doesn't match the existing positional layout. For all
    # current cases (15-column legacy + 17-column TIP-011), the source
    # value lives at index 16. If psycopg ever returns a Row-like object,
    # prefer named access.
    try:
        source = row["source"]  # type: ignore[index]
        named = True
    except (KeyError, TypeError):
        named = False
        source = None
    return {
        "id": row[0],
        "symbol": row[1],
        "entryDate": row[2],
        "side": row[3],
        "entryPrice": float(row[4]) if row[4] is not None else None,
        "scoreAtEntry": float(row[5]) if row[5] is not None else None,
        "whyAtEntry": row[6],
        "sleevePct": float(row[7]) if row[7] is not None else None,
        "status": row[8],
        "closeDate": row[9],
        "closePrice": float(row[10]) if row[10] is not None else None,
        "pnlPct": float(row[11]) if row[11] is not None else None,
        "holdingDays": int(row[12]) if row[12] is not None else None,
        "closeReason": row[13],
        "source": row[16] if not named and len(row) > 16 else source,
        "createdAt": row[14].isoformat() if row[14] is not None else None,
        "updatedAt": row[15].isoformat() if row[15] is not None else None,
    }


def count_by_source(*, since: str | None = None, status: str | None = None) -> dict[str, dict[str, int]]:
    """Aggregate paper-trade counts + wins by ``source`` since the given date.

    Returns ``{source: {total, wins, losses, winRate}}``. Sources not present
    in the data are omitted from the dict (caller decides defaults).
    """
    ensure_tables()
    clauses: list[str] = []
    params: list[Any] = []
    if since:
        clauses.append("entry_date >= %s")
        params.append(since)
    if status:
        clauses.append("status = %s")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT source,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins,
                       COUNT(*) FILTER (WHERE pnl_pct <= 0) AS losses
                FROM {PAPER_TRADES_TABLE}
                {where}
                GROUP BY source
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        source = r[0] if r[0] is not None else "UNKNOWN"
        total = int(r[1] or 0)
        wins = int(r[2] or 0)
        losses = int(r[3] or 0)
        closed_total = wins + losses
        out[str(source)] = {
            "total": total,
            "wins": wins,
            "losses": losses,
            "winRate": round(wins / closed_total, 3) if closed_total > 0 else 0.0,
        }
    return out


# ---------------------------------------------------------------------------
# Convenience date helpers
# ---------------------------------------------------------------------------


def today_iso() -> str:
    return date.today().isoformat()
