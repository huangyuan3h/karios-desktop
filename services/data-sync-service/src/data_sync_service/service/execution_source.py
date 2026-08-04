"""Execution source attribution (TIP-011).

Three provenances are tracked end-to-end:
  - 'TV'      — BUY/ADD originating from TV screener (funnel path)
  - 'ALPHA'   — BUY/ADD originating from Alpha Radar catalyst (compute_alpha_additions)
  - 'MANUAL'  — user / external AI agent added to watchlist directly
  - None / 'UNKNOWN' — pre-TIP-011 rows (NULL source)

The primary write path is execution_journal.diff_snapshots, which propagates
``card.source`` into ``execution_decision_changes.source``. paper_trades
intake then mirrors the change row's source onto the inserted trade.

This module handles the **read-side aggregation** for the
``/v1/execution/source-stats`` endpoint and a defensive ``infer_source``
helper used by backfill scripts (NOT in the hot path).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from data_sync_service.db import execution_journal as ej_db
from data_sync_service.db import paper_trading as pt_db

DEFAULT_LOOKBACK_DAYS = 30
KNOWN_SOURCES: tuple[str, ...] = ("TV", "ALPHA", "MANUAL")


def _lookback_iso(days: int) -> str:
    days = max(1, min(int(days), 365))
    return (datetime.now(UTC) - timedelta(days=days)).isoformat()


def _empty_source_bucket() -> dict[str, Any]:
    return {
        "buySignals": 0,
        "closed": 0,
        "wins": 0,
        "losses": 0,
        "winRate": 0.0,
    }


def aggregate_source_stats(
    *,
    since_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """Return per-source BUY-signal counts + paper-trade win-rate aggregates.

    Output schema (stable contract for /v1/execution/source-stats):

    {
      "sinceDays": int,
      "generatedAt": iso,
      "lookbackDays": int,
      "bySource": {
        "TV":      {"buySignals": int, "closed": int, "wins": int, "losses": int, "winRate": float},
        "ALPHA":   {...},
        "MANUAL":  {...},
        "UNKNOWN": {...}        // only present if pre-TIP-011 rows exist
      }
    }

    ``buySignals`` counts ``execution_decision_changes`` rows where
    field='action' AND new_value='BUY' (the canonical "user-visible BUY
    transition"). ``closed / wins / losses / winRate`` aggregate paper_trades
    keyed by source for the same window.
    """
    since_iso = _lookback_iso(since_days)
    since_date = (datetime.now(UTC) - timedelta(days=since_days)).date().isoformat()

    by_source: dict[str, dict[str, Any]] = {
        name: _empty_source_bucket() for name in KNOWN_SOURCES
    }

    # BUY signal volume (changes field=action, new_value=BUY)
    try:
        buy_counts = ej_db.count_changes_by_source(
            since=since_iso, field="action", new_value="BUY"
        )
    except Exception:  # noqa: BLE001
        buy_counts = {}
    for k, v in buy_counts.items():
        bucket = by_source.setdefault(k, _empty_source_bucket())
        bucket["buySignals"] = int(v or 0)

    # Paper-trade closed outcomes by source
    try:
        closed = pt_db.count_by_source(since=since_date, status="closed")
    except Exception:  # noqa: BLE001
        closed = {}
    for k, v in closed.items():
        bucket = by_source.setdefault(k, _empty_source_bucket())
        bucket["closed"] = int(v.get("total") or 0)
        bucket["wins"] = int(v.get("wins") or 0)
        bucket["losses"] = int(v.get("losses") or 0)
        bucket["winRate"] = float(v.get("winRate") or 0.0)

    # Open trades are exposed too — useful for monitoring in-flight ALPHA adds
    open_buckets: dict[str, int] = {}
    try:
        open_trades = pt_db.count_by_source(since=since_date, status="open")
    except Exception:  # noqa: BLE001
        open_trades = {}
    for k, v in open_trades.items():
        open_buckets[str(k)] = int(v.get("total") or 0)

    # Drop zero-zero buckets to keep the response lean (UNLESS pre-TIP-011
    # rows already pushed UNKNOWN into the dict).
    final: dict[str, dict[str, Any]] = {}
    for name, bucket in by_source.items():
        if bucket["buySignals"] or bucket["closed"] or bucket["wins"] or bucket["losses"]:
            final[name] = bucket
        elif name == "UNKNOWN":
            final[name] = bucket

    return {
        "sinceDays": since_days,
        "lookbackDays": since_days,
        "generatedAt": datetime.now(UTC).isoformat(),
        "bySource": final,
        "openTradesBySource": open_buckets,
    }


def infer_source(
    *,
    symbol: str,
    tv_screener_symbols: set[str] | None = None,
    alpha_catalyst_symbols: set[str] | None = None,
) -> str:
    """Defensive source inference for backfill scripts.

    NOT used in the hot path — deriveActionCard (frontend) and
    compute_alpha_additions (backend) are the canonical attributions.
    Only used by ad-hoc backfill scripts that want to populate
    ``paper_trades.source`` for rows that pre-date TIP-011.

    Returns one of KNOWN_SOURCES. 'TV' wins over 'ALPHA' over 'MANUAL' on
    conflict (TV is the most-attributable path).
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return "MANUAL"
    tv = tv_screener_symbols or set()
    alpha = alpha_catalyst_symbols or set()
    if sym in tv:
        return "TV"
    if sym in alpha:
        return "ALPHA"
    return "MANUAL"


def backfill_paper_trades_source(*, dry_run: bool = True) -> dict[str, int]:
    """Walk pre-TIP-011 paper_trades with source IS NULL and backfill from
    execution_decision_changes (joined on trade_date + symbol) when available.

    Returns counts of {backfilled, already_set, no_match}. With
    ``dry_run=True`` (default) the function only reports counts; pass
    ``dry_run=False`` to actually persist updates.

    Intended to be invoked manually after the alembic 0018 upgrade ships.
    """
    # Lazy import to avoid pulling psycopg at module load
    from data_sync_service.db import get_connection
    from data_sync_service.db.execution_journal import CHANGES_TABLE
    from data_sync_service.db.paper_trading import PAPER_TRADES_TABLE

    counts = {"backfilled": 0, "already_set": 0, "no_match": 0}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT pt.id, pt.symbol, pt.entry_date, pt.side, pt.source
                FROM {PAPER_TRADES_TABLE} pt
                WHERE pt.source IS NULL
                ORDER BY pt.entry_date DESC, pt.created_at DESC
                LIMIT 5000
                """
            )
            rows = cur.fetchall()
            targets = []
            for r in rows:
                pt_id, symbol, entry_date, side, source = r
                if source is not None:
                    counts["already_set"] += 1
                    continue
                cur.execute(
                    f"""
                    SELECT source FROM {CHANGES_TABLE}
                    WHERE trade_date = %s::date
                      AND symbol = %s
                      AND field = 'action'
                      AND new_value = %s
                    LIMIT 1
                    """,
                    (entry_date, symbol, side),
                )
                src_row = cur.fetchone()
                if not src_row or src_row[0] is None:
                    counts["no_match"] += 1
                    continue
                targets.append((pt_id, src_row[0]))
            if not dry_run and targets:
                for pt_id, src in targets:
                    cur.execute(
                        f"UPDATE {PAPER_TRADES_TABLE} SET source = %s WHERE id = %s",
                        (src, pt_id),
                    )
                conn.commit()
            counts["backfilled"] = len(targets)
    return counts


def get_default_lookback_days() -> int:
    try:
        raw = os.environ.get("EXECUTION_SOURCE_STATS_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS))
        return max(1, min(int(raw), 365))
    except ValueError:
        return DEFAULT_LOOKBACK_DAYS


__all__ = [
    "DEFAULT_LOOKBACK_DAYS",
    "KNOWN_SOURCES",
    "aggregate_source_stats",
    "backfill_paper_trades_source",
    "get_default_lookback_days",
    "infer_source",
]