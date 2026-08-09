"""Execution decision snapshots + change log (Postgres)."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any
from uuid import uuid4

from data_sync_service.db import get_connection

SNAPSHOTS_TABLE = "execution_snapshots"
CHANGES_TABLE = "execution_decision_changes"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SNAPSHOTS_TABLE} (
    id              TEXT PRIMARY KEY,
    trade_date      DATE NOT NULL,
    captured_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    source          TEXT NOT NULL,
    gate            JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    cards           JSONB NOT NULL DEFAULT '[]'::jsonb,
    content_hash    TEXT NOT NULL,
    meta            JSONB NOT NULL DEFAULT '{{}}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_execution_snapshots_trade_captured
    ON {SNAPSHOTS_TABLE}(trade_date DESC, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_snapshots_hash
    ON {SNAPSHOTS_TABLE}(content_hash);

CREATE TABLE IF NOT EXISTS {CHANGES_TABLE} (
    id                  TEXT PRIMARY KEY,
    trade_date          DATE NOT NULL,
    changed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    from_snapshot_id    TEXT,
    to_snapshot_id      TEXT NOT NULL,
    scope               TEXT NOT NULL,
    symbol              TEXT,
    field               TEXT NOT NULL,
    old_value           TEXT,
    new_value           TEXT,
    source              TEXT
);

CREATE INDEX IF NOT EXISTS idx_execution_changes_trade_changed
    ON {CHANGES_TABLE}(trade_date DESC, changed_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_changes_symbol
    ON {CHANGES_TABLE}(symbol, changed_at DESC);

-- TIP-011: source attribution by provenance (TV / ALPHA / MANUAL).
CREATE INDEX IF NOT EXISTS idx_execution_changes_source
    ON {CHANGES_TABLE}(source, changed_at DESC)
    WHERE source IS NOT NULL;
"""

_TABLE_ENSURED = False


def ensure_table() -> None:
    global _TABLE_ENSURED
    if _TABLE_ENSURED:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()
    _TABLE_ENSURED = True


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def _iso(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    return str(val)


def _parse_jsonb(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except json.JSONDecodeError:
            return val
    return val


def insert_snapshot(
    *,
    snapshot_id: str | None = None,
    trade_date: str,
    source: str,
    gate: dict[str, Any],
    cards: list[dict[str, Any]],
    content_hash: str,
    meta: dict[str, Any] | None = None,
    captured_at: datetime | None = None,
) -> dict[str, Any]:
    ensure_table()
    sid = snapshot_id or str(uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            if captured_at is None:
                cur.execute(
                    f"""
                    INSERT INTO {SNAPSHOTS_TABLE}
                        (id, trade_date, source, gate, cards, content_hash, meta)
                    VALUES (%s, %s::date, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb)
                    RETURNING id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    """,
                    (
                        sid,
                        trade_date,
                        source,
                        _json_dump(gate),
                        _json_dump(cards),
                        content_hash,
                        _json_dump(meta or {}),
                    ),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {SNAPSHOTS_TABLE}
                        (id, trade_date, captured_at, source, gate, cards, content_hash, meta)
                    VALUES (%s, %s::date, %s, %s, %s::jsonb, %s::jsonb, %s, %s::jsonb)
                    RETURNING id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    """,
                    (
                        sid,
                        trade_date,
                        captured_at,
                        source,
                        _json_dump(gate),
                        _json_dump(cards),
                        content_hash,
                        _json_dump(meta or {}),
                    ),
                )
            row = cur.fetchone()
        conn.commit()
    return _snapshot_row(row)


def touch_snapshot_captured_at(snapshot_id: str) -> dict[str, Any] | None:
    """Heartbeat: update captured_at without changing decision content."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {SNAPSHOTS_TABLE}
                SET captured_at = now()
                WHERE id = %s
                RETURNING id, trade_date, captured_at, source, gate, cards, content_hash, meta
                """,
                (snapshot_id,),
            )
            row = cur.fetchone()
        conn.commit()
    return _snapshot_row(row) if row else None


def fetch_latest_snapshot(*, trade_date: str | None = None) -> dict[str, Any] | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if trade_date:
                cur.execute(
                    f"""
                    SELECT id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    FROM {SNAPSHOTS_TABLE}
                    WHERE trade_date = %s::date
                    ORDER BY captured_at DESC
                    LIMIT 1
                    """,
                    (trade_date,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    FROM {SNAPSHOTS_TABLE}
                    ORDER BY captured_at DESC
                    LIMIT 1
                    """
                )
            row = cur.fetchone()
    return _snapshot_row(row) if row else None


def fetch_snapshot_by_id(snapshot_id: str) -> dict[str, Any] | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, trade_date, captured_at, source, gate, cards, content_hash, meta
                FROM {SNAPSHOTS_TABLE}
                WHERE id = %s
                """,
                (snapshot_id,),
            )
            row = cur.fetchone()
    return _snapshot_row(row) if row else None


def list_snapshots(
    *,
    trade_date: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_table()
    limit = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            if trade_date:
                cur.execute(
                    f"""
                    SELECT id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    FROM {SNAPSHOTS_TABLE}
                    WHERE trade_date = %s::date
                    ORDER BY captured_at DESC
                    LIMIT %s
                    """,
                    (trade_date, limit),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, trade_date, captured_at, source, gate, cards, content_hash, meta
                    FROM {SNAPSHOTS_TABLE}
                    ORDER BY captured_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
    return [_snapshot_row(r) for r in rows]


def has_source_on_date(trade_date: str, sources: list[str]) -> bool:
    ensure_table()
    if not sources:
        return False
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 1 FROM {SNAPSHOTS_TABLE}
                WHERE trade_date = %s::date AND source = ANY(%s)
                LIMIT 1
                """,
                (trade_date, sources),
            )
            return cur.fetchone() is not None


def insert_changes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    ensure_table()
    out: list[dict[str, Any]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cid = r.get("id") or str(uuid4())
                cur.execute(
                    f"""
                    INSERT INTO {CHANGES_TABLE}
                        (id, trade_date, changed_at, from_snapshot_id, to_snapshot_id,
                         scope, symbol, field, old_value, new_value, source)
                    VALUES (%s, %s::date, COALESCE(%s::timestamptz, now()), %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, trade_date, changed_at, from_snapshot_id, to_snapshot_id,
                              scope, symbol, field, old_value, new_value, source
                    """,
                    (
                        cid,
                        r["trade_date"],
                        r.get("changed_at"),
                        r.get("from_snapshot_id"),
                        r["to_snapshot_id"],
                        r["scope"],
                        r.get("symbol"),
                        r["field"],
                        r.get("old_value"),
                        r.get("new_value"),
                        r.get("source"),
                    ),
                )
                out.append(_change_row(cur.fetchone()))
        conn.commit()
    return out


def list_changes(
    *,
    trade_date: str | None = None,
    since: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    ensure_table()
    limit = max(1, min(int(limit), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if trade_date:
        clauses.append("trade_date = %s::date")
        params.append(trade_date)
    if since:
        clauses.append("changed_at >= %s::timestamptz")
        params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, trade_date, changed_at, from_snapshot_id, to_snapshot_id,
                       scope, symbol, field, old_value, new_value, source
                FROM {CHANGES_TABLE}
                {where}
                ORDER BY changed_at DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    return [_change_row(r) for r in rows]


def count_changes_by_source(
    *,
    since: str | None = None,
    field: str | None = "action",
    new_value: str | None = "BUY",
) -> dict[str, int]:
    """Aggregate change counts by ``source``. Optional filter on field + new_value.

    Used by ``/v1/execution/source-stats`` to surface BUY-signal volume per
    provenance. Pre-TIP-011 rows have NULL source and are bucketed under
    'UNKNOWN'.
    """
    ensure_table()
    clauses: list[str] = []
    params: list[Any] = []
    if since:
        clauses.append("changed_at >= %s::timestamptz")
        params.append(since)
    if field:
        clauses.append("field = %s")
        params.append(field)
    if new_value:
        clauses.append("new_value = %s")
        params.append(new_value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT source, COUNT(*) AS total
                FROM {CHANGES_TABLE}
                {where}
                GROUP BY source
                """,
                tuple(params),
            )
            rows = cur.fetchall()
    out: dict[str, int] = {}
    for r in rows:
        key = str(r[0]) if r[0] is not None else "UNKNOWN"
        out[key] = int(r[1] or 0)
    return out


def _snapshot_row(row: tuple[Any, ...] | None) -> dict[str, Any]:
    if not row:
        return {}
    (
        sid,
        trade_date,
        captured_at,
        source,
        gate,
        cards,
        content_hash,
        meta,
    ) = row
    return {
        "id": sid,
        "tradeDate": _iso(trade_date),
        "capturedAt": _iso(captured_at),
        "source": source,
        "gate": _parse_jsonb(gate) or {},
        "cards": _parse_jsonb(cards) or [],
        "contentHash": content_hash,
        "meta": _parse_jsonb(meta) or {},
    }


def _change_row(row: tuple[Any, ...] | None) -> dict[str, Any]:
    if not row:
        return {}
    (
        cid,
        trade_date,
        changed_at,
        from_id,
        to_id,
        scope,
        symbol,
        field,
        old_value,
        new_value,
        source,
    ) = row
    return {
        "id": cid,
        "tradeDate": _iso(trade_date),
        "changedAt": _iso(changed_at),
        "fromSnapshotId": from_id,
        "toSnapshotId": to_id,
        "scope": scope,
        "symbol": symbol,
        "field": field,
        "oldValue": old_value,
        "newValue": new_value,
        "source": source,
    }
