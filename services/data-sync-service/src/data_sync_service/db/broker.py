from __future__ import annotations

import json
from typing import Any

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

ACCOUNTS_TABLE = "broker_accounts"
STATE_TABLE = "broker_account_state"
SNAPSHOTS_TABLE = "broker_snapshots"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {ACCOUNTS_TABLE} (
    id             TEXT PRIMARY KEY,
    broker         TEXT NOT NULL,
    title          TEXT NOT NULL,
    account_masked TEXT,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_accounts_broker_updated
    ON {ACCOUNTS_TABLE}(broker, updated_at DESC);

CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    account_id          TEXT PRIMARY KEY,
    broker              TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    overview_json       JSONB NOT NULL,
    positions_json      JSONB NOT NULL,
    conditional_json    JSONB NOT NULL,
    trades_json         JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_account_state_broker_updated
    ON {STATE_TABLE}(broker, updated_at DESC);

CREATE TABLE IF NOT EXISTS {SNAPSHOTS_TABLE} (
    id            TEXT PRIMARY KEY,
    broker        TEXT NOT NULL,
    account_id    TEXT,
    captured_at   TEXT NOT NULL,
    kind          TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    image_bytes   BYTEA NOT NULL,
    image_type    TEXT NOT NULL,
    image_name    TEXT NOT NULL,
    extracted_json JSONB NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_broker_snapshots_broker_captured
    ON {SNAPSHOTS_TABLE}(broker, captured_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_broker_snapshots_broker_account_sha
    ON {SNAPSHOTS_TABLE}(broker, account_id, sha256);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def list_accounts(*, broker: str | None = None) -> list[dict[str, Any]]:
    ensure_tables()
    b = (broker or "").strip().lower()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if b:
                cur.execute(
                    f"""
                    SELECT id, broker, title, account_masked, updated_at
                    FROM {ACCOUNTS_TABLE}
                    WHERE broker = %s
                    ORDER BY updated_at DESC
                    """,
                    (b,),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id, broker, title, account_masked, updated_at
                    FROM {ACCOUNTS_TABLE}
                    ORDER BY updated_at DESC
                    """
                )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "broker": str(r[1]),
            "title": str(r[2]),
            "accountMasked": str(r[3]) if r[3] is not None else None,
            "updatedAt": str(r[4]),
        }
        for r in rows
    ]


def create_account(
    *,
    account_id: str,
    broker: str,
    title: str,
    account_masked: str | None,
    created_at: str,
    updated_at: str,
) -> dict[str, Any]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {ACCOUNTS_TABLE}(id, broker, title, account_masked, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (account_id, broker, title, account_masked, created_at, updated_at),
            )
        conn.commit()
    return {
        "id": account_id,
        "broker": broker,
        "title": title,
        "accountMasked": account_masked,
        "updatedAt": updated_at,
    }


def update_account_title(*, account_id: str, title: str, updated_at: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {ACCOUNTS_TABLE} SET title = %s, updated_at = %s WHERE id = %s",
                (title, updated_at, account_id),
            )
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def delete_account(*, account_id: str) -> bool:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ACCOUNTS_TABLE} WHERE id = %s", (account_id,))
            ok = (cur.rowcount or 0) > 0
        conn.commit()
    return ok


def get_account_state_row(account_id: str) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT account_id, broker, updated_at, overview_json, positions_json, conditional_json, trades_json
                FROM {STATE_TABLE}
                WHERE account_id = %s
                """,
                (account_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {
        "accountId": str(row[0]),
        "broker": str(row[1]),
        "updatedAt": str(row[2]),
        "overview": row[3] if isinstance(row[3], dict) else json.loads(str(row[3]) or "{}"),
        "positions": row[4] if isinstance(row[4], list) else json.loads(str(row[4]) or "[]"),
        "conditionalOrders": row[5] if isinstance(row[5], list) else json.loads(str(row[5]) or "[]"),
        "trades": row[6] if isinstance(row[6], list) else json.loads(str(row[6]) or "[]"),
    }


def ensure_account_state(*, account_id: str, broker: str, updated_at: str) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT account_id FROM {STATE_TABLE} WHERE account_id = %s",
                (account_id,),
            )
            row = cur.fetchone()
            if row:
                return
            cur.execute(
                f"""
                INSERT INTO {STATE_TABLE}(
                    account_id, broker, updated_at, overview_json, positions_json, conditional_json, trades_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    account_id,
                    broker,
                    updated_at,
                    Json({}),
                    Json([]),
                    Json([]),
                    Json([]),
                ),
            )
        conn.commit()


def upsert_account_state(
    *,
    account_id: str,
    broker: str,
    updated_at: str,
    overview: dict[str, Any] | None,
    positions: list[dict[str, Any]] | None,
    conditional_orders: list[dict[str, Any]] | None,
    trades: list[dict[str, Any]] | None,
) -> None:
    ensure_account_state(account_id=account_id, broker=broker, updated_at=updated_at)
    current = get_account_state_row(account_id) or {
        "overview": {},
        "positions": [],
        "conditionalOrders": [],
        "trades": [],
    }
    next_overview = overview if overview is not None else (current.get("overview") or {})
    next_positions = positions if positions is not None else (current.get("positions") or [])
    next_orders = (
        conditional_orders if conditional_orders is not None else (current.get("conditionalOrders") or [])
    )
    next_trades = trades if trades is not None else (current.get("trades") or [])
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {STATE_TABLE}
                SET updated_at = %s,
                    overview_json = %s,
                    positions_json = %s,
                    conditional_json = %s,
                    trades_json = %s
                WHERE account_id = %s
                """,
                (
                    updated_at,
                    Json(next_overview),
                    Json(next_positions),
                    Json(next_orders),
                    Json(next_trades),
                    account_id,
                ),
            )
            cur.execute(
                f"UPDATE {ACCOUNTS_TABLE} SET updated_at = %s WHERE id = %s",
                (updated_at, account_id),
            )
        conn.commit()


def insert_snapshot(
    *,
    snapshot_id: str,
    broker: str,
    account_id: str | None,
    captured_at: str,
    kind: str,
    sha256: str,
    image_bytes: bytes,
    image_type: str,
    image_name: str,
    extracted: dict[str, Any],
    created_at: str,
) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {SNAPSHOTS_TABLE}(
                    id, broker, account_id, captured_at, kind, sha256,
                    image_bytes, image_type, image_name, extracted_json, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (broker, account_id, sha256) DO NOTHING
                """,
                (
                    snapshot_id,
                    broker,
                    account_id,
                    captured_at,
                    kind,
                    sha256,
                    image_bytes,
                    image_type,
                    image_name,
                    Json(extracted),
                    created_at,
                ),
            )
        conn.commit()


def list_snapshots(*, broker: str, account_id: str | None, limit: int = 20) -> list[dict[str, Any]]:
    ensure_tables()
    lim = max(1, min(int(limit), 200))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, broker, account_id, captured_at, kind, created_at
                FROM {SNAPSHOTS_TABLE}
                WHERE broker = %s AND (account_id = %s OR (%s IS NULL AND account_id IS NULL))
                ORDER BY captured_at DESC
                LIMIT %s
                """,
                (broker, account_id, account_id, lim),
            )
            rows = cur.fetchall()
    return [
        {
            "id": str(r[0]),
            "broker": str(r[1]),
            "accountId": str(r[2]) if r[2] is not None else None,
            "capturedAt": str(r[3]),
            "kind": str(r[4]),
            "createdAt": str(r[5]),
        }
        for r in rows
    ]


def get_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    ensure_tables()
    sid = (snapshot_id or "").strip()
    if not sid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, broker, account_id, captured_at, kind,
                       image_type, image_name, extracted_json, created_at
                FROM {SNAPSHOTS_TABLE}
                WHERE id = %s
                """,
                (sid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    extracted = row[7] if isinstance(row[7], dict) else json.loads(str(row[7]) or "{}")
    return {
        "id": str(row[0]),
        "broker": str(row[1]),
        "accountId": str(row[2]) if row[2] is not None else None,
        "capturedAt": str(row[3]),
        "kind": str(row[4]),
        "imageType": str(row[5]),
        "imageName": str(row[6]),
        "extracted": extracted if isinstance(extracted, dict) else {"raw": extracted},
        "createdAt": str(row[8]),
    }


def get_snapshot_image(snapshot_id: str) -> dict[str, Any] | None:
    ensure_tables()
    sid = (snapshot_id or "").strip()
    if not sid:
        return None
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT image_bytes, image_type, image_name
                FROM {SNAPSHOTS_TABLE}
                WHERE id = %s
                """,
                (sid,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {"bytes": row[0], "mediaType": str(row[1]), "name": str(row[2])}
