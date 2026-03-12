from __future__ import annotations

import json
from typing import Any

from data_sync_service.db import get_connection

CHIPS_TABLE = "market_chips"
FUND_FLOW_TABLE = "market_fund_flow"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CHIPS_TABLE} (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    PRIMARY KEY(symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_market_chips_symbol_date ON {CHIPS_TABLE}(symbol, date DESC);

CREATE TABLE IF NOT EXISTS {FUND_FLOW_TABLE} (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    raw_json    JSONB NOT NULL,
    PRIMARY KEY(symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_market_fund_flow_symbol_date ON {FUND_FLOW_TABLE}(symbol, date DESC);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def list_chips_cached(symbol: str, *, limit: int) -> list[tuple[str, dict[str, Any]]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, raw_json
                FROM {CHIPS_TABLE}
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s
                """,
                (symbol, lim),
            )
            rows = cur.fetchall()
    out: list[tuple[str, dict[str, Any]]] = []
    for r in rows:
        d = str(r[0] or "")
        raw = r[1]
        if isinstance(raw, dict):
            out.append((d, raw))
        else:
            try:
                out.append((d, json.loads(str(raw or "{}"))))
            except Exception:
                out.append((d, {}))
    return out


def upsert_chips(symbol: str, items: list[dict[str, Any]], *, updated_at: str) -> None:
    ensure_tables()
    values: list[tuple[str, str, str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        d = str(it.get("date") or "").strip()
        if not d:
            continue
        raw = json.dumps(it, ensure_ascii=False)
        values.append((symbol, d, updated_at, raw))
    if not values:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {CHIPS_TABLE}(symbol, date, updated_at, raw_json)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    raw_json = EXCLUDED.raw_json
                """,
                values,
            )
        conn.commit()


def list_fund_flow_cached(symbol: str, *, limit: int) -> list[tuple[str, dict[str, Any]]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, raw_json
                FROM {FUND_FLOW_TABLE}
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s
                """,
                (symbol, lim),
            )
            rows = cur.fetchall()
    out: list[tuple[str, dict[str, Any]]] = []
    for r in rows:
        d = str(r[0] or "")
        raw = r[1]
        if isinstance(raw, dict):
            out.append((d, raw))
        else:
            try:
                out.append((d, json.loads(str(raw or "{}"))))
            except Exception:
                out.append((d, {}))
    return out


def upsert_fund_flow(symbol: str, items: list[dict[str, Any]], *, updated_at: str) -> None:
    ensure_tables()
    values: list[tuple[str, str, str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        d = str(it.get("date") or "").strip()
        if not d:
            continue
        raw = json.dumps(it, ensure_ascii=False)
        values.append((symbol, d, updated_at, raw))
    if not values:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {FUND_FLOW_TABLE}(symbol, date, updated_at, raw_json)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT(symbol, date) DO UPDATE SET
                    updated_at = EXCLUDED.updated_at,
                    raw_json = EXCLUDED.raw_json
                """,
                values,
            )
        conn.commit()

