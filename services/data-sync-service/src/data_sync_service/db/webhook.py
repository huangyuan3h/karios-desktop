"""Webhook event subscription (todo §14 #3 · P1 · 2026-08-12).

Three-layer event delivery:
  webhook_events        — event log (dedupe_key unique per event occurrence)
  webhook_subscriptions — consumer endpoints (url + HMAC secret + event types)
  webhook_deliveries    — per (event, subscription) delivery state machine:
                          pending -> sent | failed(×3 retries) -> dead

emit_event() is called at the point each product already produces the data
(e.g. sync_job_record failures); deliver_pending() (see
service/webhook_delivery.py) runs every minute from the scheduler.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from data_sync_service.db import get_connection

EVENTS_TABLE = "webhook_events"
SUBSCRIPTIONS_TABLE = "webhook_subscriptions"
DELIVERIES_TABLE = "webhook_deliveries"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
    id          SERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL DEFAULT '{{}}',
    dedupe_key  TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_webhook_events_type_created ON {EVENTS_TABLE} (event_type, created_at);

CREATE TABLE IF NOT EXISTS {SUBSCRIPTIONS_TABLE} (
    id          SERIAL PRIMARY KEY,
    url         TEXT NOT NULL,
    secret      TEXT NOT NULL,
    event_types TEXT[] NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {DELIVERIES_TABLE} (
    id              SERIAL PRIMARY KEY,
    event_id        INTEGER NOT NULL REFERENCES {EVENTS_TABLE} (id) ON DELETE CASCADE,
    subscription_id INTEGER NOT NULL REFERENCES {SUBSCRIPTIONS_TABLE} (id) ON DELETE CASCADE,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    last_error      TEXT,
    delivered_at    TIMESTAMPTZ,
    UNIQUE (event_id, subscription_id)
);
CREATE INDEX IF NOT EXISTS ix_webhook_deliveries_pending
    ON {DELIVERIES_TABLE} (status, next_retry_at);
"""

RETRY_DELAYS_MINUTES = (5, 15, 60)
MAX_ATTEMPTS = len(RETRY_DELAYS_MINUTES) + 1


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def emit_event(event_type: str, payload: dict[str, Any], dedupe_key: str) -> bool:
    """Log an event and queue a pending delivery for every enabled
    subscription matching the type. Returns False when a duplicate
    dedupe_key (same-day occurrence) is dropped."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    INSERT INTO {EVENTS_TABLE} (event_type, payload, dedupe_key)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (event_type, json.dumps(payload), dedupe_key),
                )
            except Exception:
                conn.rollback()
                return False
            event_id = cur.fetchone()[0]
            cur.execute(
                f"""
                INSERT INTO {DELIVERIES_TABLE} (event_id, subscription_id)
                SELECT %s, id
                FROM {SUBSCRIPTIONS_TABLE}
                WHERE enabled AND %s = ANY(event_types)
                ON CONFLICT (event_id, subscription_id) DO NOTHING
                """,
                (event_id, event_type),
            )
        conn.commit()
    return True


def list_subscriptions() -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, url, secret, event_types, enabled, created_at
                FROM {SUBSCRIPTIONS_TABLE}
                ORDER BY id
                """
            )
            rows = cur.fetchall()
    return [
        {
            "id": r[0],
            "url": r[1],
            "secret": r[2],
            "eventTypes": list(r[3]),
            "enabled": bool(r[4]),
            "createdAt": r[5].isoformat() if hasattr(r[5], "isoformat") else str(r[5]),
        }
        for r in rows
    ]


def upsert_subscription(
    *,
    url: str,
    secret: str,
    event_types: list[str],
    enabled: bool = True,
    sub_id: int | None = None,
) -> dict[str, Any]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            if sub_id is not None:
                cur.execute(
                    f"""
                    UPDATE {SUBSCRIPTIONS_TABLE}
                    SET url = %s, secret = %s, event_types = %s, enabled = %s
                    WHERE id = %s
                    RETURNING id, url, secret, event_types, enabled, created_at
                    """,
                    (url, secret, event_types, enabled, sub_id),
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {SUBSCRIPTIONS_TABLE} (url, secret, event_types, enabled)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, url, secret, event_types, enabled, created_at
                    """,
                    (url, secret, event_types, enabled),
                )
            row = cur.fetchone()
        conn.commit()
    return {
        "id": row[0],
        "url": row[1],
        "secret": row[2],
        "eventTypes": list(row[3]),
        "enabled": bool(row[4]),
        "createdAt": row[5].isoformat() if hasattr(row[5], "isoformat") else str(row[5]),
    }


def delete_subscription(sub_id: int) -> bool:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SUBSCRIPTIONS_TABLE} WHERE id = %s", (sub_id,))
            deleted = cur.rowcount > 0
        conn.commit()
    return deleted


def list_pending_deliveries(limit: int = 100) -> list[dict[str, Any]]:
    ensure_table()
    now = datetime.now(UTC)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT d.id, d.event_id, d.subscription_id, e.event_type, e.payload,
                       s.url, s.secret
                FROM {DELIVERIES_TABLE} d
                JOIN {EVENTS_TABLE} e ON e.id = d.event_id
                JOIN {SUBSCRIPTIONS_TABLE} s ON s.id = d.subscription_id
                WHERE d.status = 'pending'
                  AND (d.next_retry_at IS NULL OR d.next_retry_at <= %s)
                  AND s.enabled
                ORDER BY d.id
                LIMIT %s
                """,
                (now, limit),
            )
            rows = cur.fetchall()
    return [
        {
            "delivery_id": r[0],
            "event_id": r[1],
            "subscription_id": r[2],
            "event_type": r[3],
            "payload": r[4],
            "url": r[5],
            "secret": r[6],
        }
        for r in rows
    ]


def mark_delivery_sent(delivery_id: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {DELIVERIES_TABLE}
                SET status = 'sent', attempts = attempts + 1, last_error = NULL,
                    delivered_at = now(), next_retry_at = NULL
                WHERE id = %s
                """,
                (delivery_id,),
            )
        conn.commit()


def mark_delivery_failed(delivery_id: int, error: str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {DELIVERIES_TABLE}
                SET attempts = attempts + 1, last_error = %s
                WHERE id = %s
                RETURNING attempts
                """,
                (error, delivery_id),
            )
            attempts = cur.fetchone()[0]
            if attempts >= MAX_ATTEMPTS:
                cur.execute(
                    f"UPDATE {DELIVERIES_TABLE} SET status = 'dead' WHERE id = %s",
                    (delivery_id,),
                )
            else:
                delay = RETRY_DELAYS_MINUTES[attempts - 1]
                cur.execute(
                    f"""
                    UPDATE {DELIVERIES_TABLE}
                    SET status = 'pending', next_retry_at = %s
                    WHERE id = %s
                    """,
                    (datetime.now(UTC) + timedelta(minutes=delay), delivery_id),
                )
        conn.commit()
