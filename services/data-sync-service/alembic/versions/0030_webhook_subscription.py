"""0030 webhook event subscription (2026-08-12 · todo §14 #3 P1).

Three-layer webhook delivery: webhook_events (event log with unique
dedupe_key), webhook_subscriptions (consumer url + HMAC secret + event
types), webhook_deliveries (per (event, subscription) state machine
pending -> sent / failed x3 -> dead). Emitted at existing product points
(e.g. sync_job_record failures); deliver_pending() runs every minute.

Kept in sync with db/webhook.py CREATE_SQL.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0030_webhook_subscription"
down_revision: str | Sequence[str] | None = "0029_paper_signal_snapshot"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The current baseline also creates these tables for existing deployments;
    # use idempotent SQL so fresh and stamped databases follow the same chain.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS webhook_events (
            id SERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}',
            dedupe_key TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE INDEX IF NOT EXISTS ix_webhook_events_type_created
            ON webhook_events (event_type, created_at);
        CREATE TABLE IF NOT EXISTS webhook_subscriptions (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            secret TEXT NOT NULL,
            event_types TEXT[] NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS webhook_deliveries (
            id SERIAL PRIMARY KEY,
            event_id INTEGER NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
            subscription_id INTEGER NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_retry_at TIMESTAMPTZ,
            last_error TEXT,
            delivered_at TIMESTAMPTZ,
            CONSTRAINT uq_webhook_deliveries_event_sub UNIQUE (event_id, subscription_id)
        );
        CREATE INDEX IF NOT EXISTS ix_webhook_deliveries_pending
            ON webhook_deliveries (status, next_retry_at);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS webhook_deliveries;")
    op.execute("DROP TABLE IF EXISTS webhook_subscriptions;")
    op.execute("DROP TABLE IF EXISTS webhook_events;")
