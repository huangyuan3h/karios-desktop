"""paper_trades table — virtual paper-trading log (OPT-049, v0.2 / OPT-062).

The table is a faithful record of what Karios' own BUY/ADD signals WOULD HAVE
done if the user had followed every recommendation. Each row is one entry:
- `entry_date` / `entry_price` / `side` are the simulated order.
- `close_date` / `close_price` / `pnl_pct` / `holding_days` are filled when the
  trade is closed (intake cron + update cron decide when).
- `close_reason` is one of: `target_hit` | `stop_hit` | `max_hold` | `score_floor`
  | `pool_exit` | `manual`. v0.1 emits `max_hold`, `stop_hit`, `target_hit`,
  `score_floor` and `pool_exit`.

v0.2 scope (OPT-062 / L3-P1):
- **CN + HK**. HK bars share the `daily` table (ts_code like `00700.HK`) and
  are priced in HKD (no FX conversion — L3-P3 refinement). ETF stays out of
  scope (TrendOK/score semantics undefined); intake records it as a skip.
- **Cost model**: `pnl_pct` on CLOSED rows is the NET pnl (gross minus the
  market's round-trip cost from ``service.paper_cost_model``). `gross_pnl_pct`
  and `costs_pct` record the split. Open rows keep showing the current GROSS
  pnl (costs land once, at close time). Legacy rows (pre-v0.2) are backfilled
  with market='CN', costs_pct=0, gross_pnl_pct=pnl_pct — i.e. they were
  priced with no cost model.
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

from psycopg.rows import dict_row

from data_sync_service.db import get_connection
from data_sync_service.service.paper_cost_model import MARKETS

PAPER_TRADES_TABLE = "paper_trades"

# All row-shaping goes through dict rows so column order never matters —
# fresh CREATE_SQL tables and ALTER-appended migrations have different
# physical column layouts (OPT-062).

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
SOURCE_S3 = "S3"  # S-3 backtest entry rules (paper_s3 intake, G4)
SOURCES = (SOURCE_TV, SOURCE_ALPHA, SOURCE_MANUAL, SOURCE_S3)

# Close thresholds (S-3 backtest params, 2026-08-09 — backtest-strategy.md
# is the evidence record). Kept module-level so tests can assert against the
# exact values and operators can tune them in one place.
MAX_HOLD_DAYS = 60  # S-3: hold up to 60 days (5-day force-close was proven wrong)
STOP_LOSS_PCT = -5.0  # i.e. net pnl_pct <= -5% triggers stop_hit (v0.2: net)
TARGET_PNL_PCT = 100.0  # S-3: no active take-profit (10% target was proven a profit killer)
SCORE_FLOOR = 0.0  # S-3: never close on score retreat (floor 30 was proven to kill trends)

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
    market          TEXT NOT NULL DEFAULT 'CN',
    gross_pnl_pct   DOUBLE PRECISION,
    costs_pct       DOUBLE PRECISION,
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

-- OPT-062: per-market stats / filtering.
CREATE INDEX IF NOT EXISTS idx_paper_trades_market
    ON {PAPER_TRADES_TABLE}(market, entry_date DESC);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
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
    market: str = "CN",
) -> dict[str, Any] | None:
    """Insert one paper trade. Returns the row or ``None`` on idempotent skip.

    Idempotent on ``(symbol, entry_date, side)``: re-running intake for the
    same day never produces duplicates.

    ``source`` (TIP-011) is one of 'TV' / 'ALPHA' / 'MANUAL' (closed enum) or
    None for legacy rows that pre-date attribution. ``market`` (OPT-062) is
    'CN' | 'HK' and must have a cost model (``paper_cost_model``).
    """
    if side not in SIDES:
        raise ValueError(f"side must be one of {SIDES} (got {side!r})")
    if source is not None and source not in SOURCES:
        raise ValueError(f"source must be one of {SOURCES} or None (got {source!r})")
    if market not in MARKETS:
        raise ValueError(f"market must be one of {MARKETS} (got {market!r})")
    ensure_tables()
    new_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                INSERT INTO {PAPER_TRADES_TABLE}
                    (id, symbol, entry_date, side, entry_price, score_at_entry,
                     why_at_entry, sleeve_pct, status, source, market)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'open', %s, %s)
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
                    market,
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
    market: str | None = None,
) -> list[dict[str, Any]]:
    """List paper trades, optionally filtered by status, entry_date >= since
    and market ('CN' | 'HK'). Ordered by entry_date DESC, then created_at DESC."""
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
    if market:
        if market not in MARKETS:
            raise ValueError(f"market must be one of {MARKETS} (got {market!r})")
        clauses.append("market = %s")
        params.append(market)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(int(limit))
    sql = f"""
        SELECT * FROM {PAPER_TRADES_TABLE}
        {where}
        ORDER BY entry_date DESC, created_at DESC
        LIMIT %s
    """
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [_row_to_dict(r) for r in rows]


def get_open_paper_trades() -> list[dict[str, Any]]:
    """Return all open trades. Used by the update cron."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
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
        with conn.cursor(row_factory=dict_row) as cur:
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
    gross_pnl_pct: float | None = None,
    costs_pct: float | None = None,
) -> dict[str, Any] | None:
    """Close an open trade. Returns the updated row, or None if not open.

    v0.2 (OPT-062): ``pnl_pct`` is the NET pnl; ``gross_pnl_pct`` and
    ``costs_pct`` record the split so the cost assumption stays auditable.
    Legacy callers (or backfill paths) can omit the two new args — they stay
    NULL, meaning "no cost model" (indistinguishable from costs_pct=0).
    """
    if close_reason not in CLOSE_REASONS:
        raise ValueError(f"close_reason must be one of {CLOSE_REASONS} (got {close_reason!r})")
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE {PAPER_TRADES_TABLE}
                SET status = 'closed',
                    close_date = %s,
                    close_price = %s,
                    pnl_pct = %s,
                    gross_pnl_pct = COALESCE(%s, gross_pnl_pct),
                    costs_pct = COALESCE(%s, costs_pct),
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
                    gross_pnl_pct,
                    costs_pct,
                    int(holding_days),
                    close_reason,
                    trade_id,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row) if row else None


def count_since(since: str) -> tuple[int, int]:
    """Return (closed_count, winning_count) for closed trades since `since`.

    v0.2: wins are counted on the NET pnl_pct (costs deducted).
    """
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
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
    return int(row.get("total") or 0), int(row.get("wins") or 0)


def avg_pnl_pct_since(since: str) -> float | None:
    """Mean net pnl_pct across closed trades since `since`. None if no rows."""
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT AVG(pnl_pct) AS avg
                FROM {PAPER_TRADES_TABLE}
                WHERE status = 'closed'
                  AND close_date >= %s
                """,
                (since,),
            )
            row = cur.fetchone()
    if row is None or row.get("avg") is None:
        return None
    return float(row.get("avg"))


def count_by_market_since(since: str) -> dict[str, dict[str, float | int | None]]:
    """Per-market closed-trade stats since `since` (OPT-062).

    Returns ``{market: {closedCount, winningCount, winRate, avgPnlPct}}``.
    Markets with no closed trades are omitted (caller decides defaults).
    All numbers are NET-of-costs.
    """
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT market,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE pnl_pct > 0) AS wins,
                       AVG(pnl_pct) AS avg_pnl
                FROM {PAPER_TRADES_TABLE}
                WHERE status = 'closed'
                  AND close_date >= %s
                GROUP BY market
                """,
                (since,),
            )
            rows = cur.fetchall()
    out: dict[str, dict[str, float | int | None]] = {}
    for r in rows:
        market = str(r.get("market") or "CN")
        total = int(r.get("total") or 0)
        wins = int(r.get("wins") or 0)
        avg = float(r.get("avg_pnl")) if r.get("avg_pnl") is not None else None
        out[market] = {
            "closedCount": total,
            "winningCount": wins,
            "winRate": round(wins / total, 3) if total > 0 else None,
            "avgPnlPct": round(avg, 3) if avg is not None else None,
        }
    return out


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------


def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
    """Map a dict_row row to the stable camelCase /v1/paper-trades shape.

    All reads in this module use ``row_factory=dict_row`` so column ORDER
    never matters (fresh CREATE_SQL and migrated layouts differ). Field names
    stay byte-identical to the ``/v1/paper-trades`` contract (OPT-009).
    """
    ts = _iso_timestamp(row.get("created_at"))
    return {
        "id": row.get("id"),
        "symbol": row.get("symbol"),
        "entryDate": row.get("entry_date"),
        "side": row.get("side"),
        "entryPrice": _float(row.get("entry_price")),
        "scoreAtEntry": _float(row.get("score_at_entry")),
        "whyAtEntry": row.get("why_at_entry"),
        "sleevePct": _float(row.get("sleeve_pct")),
        "status": row.get("status"),
        "closeDate": row.get("close_date"),
        "closePrice": _float(row.get("close_price")),
        "pnlPct": _float(row.get("pnl_pct")),
        "grossPnlPct": _float(row.get("gross_pnl_pct")),
        "costsPct": _float(row.get("costs_pct")),
        "holdingDays": _int(row.get("holding_days")),
        "closeReason": row.get("close_reason"),
        "source": row.get("source"),
        "market": row.get("market") or "CN",
        "createdAt": ts,
        "updatedAt": _iso_timestamp(row.get("updated_at")),
    }


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _iso_timestamp(v: Any) -> str | None:
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


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
        with conn.cursor(row_factory=dict_row) as cur:
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
        source = r.get("source") or "UNKNOWN"
        total = int(r.get("total") or 0)
        wins = int(r.get("wins") or 0)
        losses = int(r.get("losses") or 0)
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
