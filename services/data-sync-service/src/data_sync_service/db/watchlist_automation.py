"""Watchlist automation: registry, daily scores, and run records."""

from __future__ import annotations

import json
import uuid
from typing import Any

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

REGISTRY_TABLE = "watchlist_registry"
SCORE_TABLE = "watchlist_score_daily"
RUNS_TABLE = "watchlist_automation_runs"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
    symbol      TEXT PRIMARY KEY,
    source      TEXT,
    added_at    TEXT,
    payload     JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {SCORE_TABLE} (
    symbol      TEXT NOT NULL,
    trade_date  TEXT NOT NULL,
    score       DOUBLE PRECISION,
    industry    TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_score_daily_date
    ON {SCORE_TABLE}(trade_date DESC);

CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    id             TEXT PRIMARY KEY,
    trade_date     TEXT NOT NULL,
    trigger_type   TEXT NOT NULL,
    skipped        BOOLEAN NOT NULL DEFAULT FALSE,
    skip_reason    TEXT,
    remove_items   JSONB NOT NULL DEFAULT '[]'::jsonb,
    alpha_add      JSONB NOT NULL DEFAULT '[]'::jsonb,
    meta           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_at     TIMESTAMPTZ,
    screener_added INTEGER
);

CREATE INDEX IF NOT EXISTS idx_watchlist_automation_runs_trade_date
    ON {RUNS_TABLE}(trade_date DESC, created_at DESC);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def upsert_registry(items: list[dict[str, Any]]) -> int:
    ensure_tables()
    if not items:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {REGISTRY_TABLE}")
            conn.commit()
        return 0
    values = []
    for item in items:
        sym = str(item.get("symbol") or "").strip()
        if not sym:
            continue
        values.append(
            (
                sym,
                str(item.get("source") or "manual"),
                str(item.get("addedAt") or item.get("added_at") or ""),
                Json(item),
            )
        )
    if not values:
        return 0
    symbols = [v[0] for v in values]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {REGISTRY_TABLE}(symbol, source, added_at, payload, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT(symbol) DO UPDATE SET
                    source = excluded.source,
                    added_at = excluded.added_at,
                    payload = excluded.payload,
                    updated_at = now()
                """,
                values,
            )
            cur.execute(
                f"DELETE FROM {REGISTRY_TABLE} WHERE NOT (symbol = ANY(%s))",
                (symbols,),
            )
        conn.commit()
    return len(values)


def list_registry() -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, source, added_at, payload
                FROM {REGISTRY_TABLE}
                ORDER BY added_at ASC NULLS LAST, symbol ASC
                """
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = row[3] if isinstance(row[3], dict) else {}
        if not isinstance(payload, dict):
            try:
                payload = json.loads(row[3]) if row[3] else {}
            except (TypeError, json.JSONDecodeError):
                payload = {}
        out.append(
            {
                "symbol": str(row[0]),
                "source": str(row[1] or "manual"),
                "addedAt": str(row[2] or ""),
                **payload,
            }
        )
    return out


def upsert_score_daily(rows: list[dict[str, Any]]) -> int:
    ensure_tables()
    if not rows:
        return 0
    values = []
    for r in rows:
        sym = str(r.get("symbol") or "").strip()
        td = str(r.get("trade_date") or "").strip()
        if not sym or not td:
            continue
        score = r.get("score")
        score_val = float(score) if score is not None else None
        values.append((sym, td, score_val, str(r.get("industry") or "") or None))
    if not values:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {SCORE_TABLE}(symbol, trade_date, score, industry, recorded_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT(symbol, trade_date) DO UPDATE SET
                    score = excluded.score,
                    industry = excluded.industry,
                    recorded_at = now()
                """,
                values,
            )
        conn.commit()
    return len(values)


def get_scores_for_symbol(symbol: str, trade_dates: list[str]) -> list[dict[str, Any]]:
    ensure_tables()
    if not trade_dates:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, score, industry
                FROM {SCORE_TABLE}
                WHERE symbol = %s AND trade_date = ANY(%s)
                ORDER BY trade_date ASC
                """,
                (symbol, trade_dates),
            )
            rows = cur.fetchall()
    return [
        {
            "trade_date": str(r[0]),
            "score": float(r[1]) if r[1] is not None else None,
            "industry": str(r[2] or "") or None,
        }
        for r in rows
    ]


def fetch_latest_score_since(symbol: str, since_trade_date: str) -> float | None:
    """Return the most recent TrendOK score on/after ``since_trade_date``.

    Used by paper-trading's ``score_floor`` close condition. Returns None when
    no score row exists (callers fail open — a missing score never closes a
    paper trade by itself).
    """
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT score FROM {SCORE_TABLE}
                WHERE symbol = %s AND trade_date >= %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (symbol, since_trade_date),
            )
            row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return float(row[0])


def insert_automation_run(
    *,
    trade_date: str,
    trigger_type: str,
    skipped: bool,
    skip_reason: str | None,
    remove_items: list[dict[str, Any]],
    alpha_add: list[dict[str, Any]],
    meta: dict[str, Any],
) -> str:
    ensure_tables()
    run_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {RUNS_TABLE}(
                    id, trade_date, trigger_type, skipped, skip_reason,
                    remove_items, alpha_add, meta
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    trade_date,
                    trigger_type,
                    skipped,
                    skip_reason,
                    Json(remove_items),
                    Json(alpha_add),
                    Json(meta),
                ),
            )
        conn.commit()
    return run_id


def get_run_by_id(run_id: str) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, trade_date, trigger_type, skipped, skip_reason,
                       remove_items, alpha_add, meta, created_at, applied_at, screener_added
                FROM {RUNS_TABLE}
                WHERE id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return _row_to_run(row)


def get_latest_run() -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, trade_date, trigger_type, skipped, skip_reason,
                       remove_items, alpha_add, meta, created_at, applied_at, screener_added
                FROM {RUNS_TABLE}
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
    if not row:
        return None
    return _row_to_run(row)


def list_recent_runs(*, limit: int = 10) -> list[dict[str, Any]]:
    """Return the most recent acknowledged run per trade_date, newest first.

    Used by the funnel-history table (TIP-002 N-day view): one row per trading
    day, so repeated manual+scheduled runs on the same day collapse into the
    latest acknowledged one. Rows without an acknowledged funnel are skipped.
    """
    ensure_tables()
    limit = max(1, min(int(limit), 30))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (trade_date)
                       id, trade_date, trigger_type, skipped, skip_reason,
                       remove_items, alpha_add, meta, created_at, applied_at, screener_added
                FROM {RUNS_TABLE}
                WHERE applied_at IS NOT NULL
                  AND meta IS NOT NULL
                  AND meta->>'funnel' IS NOT NULL
                ORDER BY trade_date DESC, created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return [_row_to_run(r) for r in rows]


def get_pending_run(trade_date: str | None = None) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if trade_date:
                cur.execute(
                    f"""
                    SELECT id, trade_date, trigger_type, skipped, skip_reason,
                           remove_items, alpha_add, meta, created_at, applied_at, screener_added
                    FROM {RUNS_TABLE}
                    WHERE trade_date = %s AND applied_at IS NULL AND skipped = FALSE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (trade_date,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, trade_date, trigger_type, skipped, skip_reason,
                           remove_items, alpha_add, meta, created_at, applied_at, screener_added
                    FROM {RUNS_TABLE}
                    WHERE applied_at IS NULL AND skipped = FALSE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                )
            row = cur.fetchone()
    if not row:
        return None
    return _row_to_run(row)


def merge_funnel_into_meta(
    meta: dict[str, Any] | None,
    funnel: dict[str, Any] | None,
) -> dict[str, Any]:
    """Pure merge used by ack_run so funnel persistence is unit-testable."""
    out: dict[str, Any] = dict(meta) if isinstance(meta, dict) else {}
    if funnel is not None and isinstance(funnel, dict):
        out["funnel"] = funnel
    return out


def ack_run(
    run_id: str,
    screener_added: int | None = None,
    funnel: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT meta FROM {RUNS_TABLE} WHERE id = %s
                """,
                (run_id,),
            )
            existing = cur.fetchone()
            if not existing:
                return None
            current_meta = existing[0] if isinstance(existing[0], dict) else {}
            new_meta = merge_funnel_into_meta(current_meta, funnel)
            cur.execute(
                f"""
                UPDATE {RUNS_TABLE}
                SET applied_at = now(),
                    screener_added = %s,
                    meta = %s
                WHERE id = %s
                RETURNING id, trade_date, trigger_type, skipped, skip_reason,
                          remove_items, alpha_add, meta, created_at, applied_at, screener_added
                """,
                (screener_added, Json(new_meta), run_id),
            )
            row = cur.fetchone()
        conn.commit()
    if not row:
        return None
    return _row_to_run(row)


def _row_to_run(row: tuple[Any, ...]) -> dict[str, Any]:
    created_at = row[8]
    applied_at = row[9]
    return {
        "runId": str(row[0]),
        "tradeDate": str(row[1]),
        "trigger": str(row[2]),
        "skipped": bool(row[3]),
        "skipReason": str(row[4]) if row[4] else None,
        "remove": row[5] if isinstance(row[5], list) else [],
        "alphaAdd": row[6] if isinstance(row[6], list) else [],
        "meta": row[7] if isinstance(row[7], dict) else {},
        "createdAt": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
        "appliedAt": applied_at.isoformat() if applied_at and hasattr(applied_at, "isoformat") else None,
        "screenerAdded": row[10],
    }
