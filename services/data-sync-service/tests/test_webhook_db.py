"""db/webhook.py integration tests (requires_postgres)."""

from __future__ import annotations

import pytest

from data_sync_service.db import webhook

TEST_URL_PREFIX = "https://test-webhook.invalid/"


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    with webhook.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {webhook.SUBSCRIPTIONS_TABLE}
                WHERE url LIKE %s
                """,
                (f"{TEST_URL_PREFIX}%",),
            )
            # IMPORTANT: only delete rows this suite created. Do NOT filter by
            # event_type — that used to delete real job_failed events (2026-08-14
            # incident: broke the per-day dedupe and re-pushed stale Bark
            # notifications). Deliveries cascade via event_id FK.
            cur.execute(
                f"""
                DELETE FROM {webhook.EVENTS_TABLE}
                WHERE dedupe_key LIKE 'test:%'
                """
            )
        conn.commit()


@pytest.mark.requires_postgres
def test_emit_subscribe_deliver_roundtrip() -> None:
    sub = webhook.upsert_subscription(
        url=f"{TEST_URL_PREFIX}roundtrip",
        secret="test-secret-1234",
        event_types=["job_failed"],
    )
    assert sub["enabled"] is True and sub["secret"] == "test-secret-1234"

    ok = webhook.emit_event(
        "job_failed",
        {"job_type": "test_job", "error": "boom"},
        dedupe_key="test:roundtrip:2026-08-12",
    )
    assert ok is True

    # Duplicate dedupe_key is dropped (same-day occurrence guard).
    ok2 = webhook.emit_event(
        "job_failed",
        {"job_type": "test_job", "error": "boom"},
        dedupe_key="test:roundtrip:2026-08-12",
    )
    assert ok2 is False

    pending = webhook.list_pending_deliveries()
    mine = [d for d in pending if d["url"].startswith(TEST_URL_PREFIX)]
    assert len(mine) == 1
    assert mine[0]["event_type"] == "job_failed"
    assert mine[0]["payload"]["job_type"] == "test_job"

    webhook.mark_delivery_sent(mine[0]["delivery_id"])
    pending_after = webhook.list_pending_deliveries()
    assert not [d for d in pending_after if d["delivery_id"] == mine[0]["delivery_id"]]


@pytest.mark.requires_postgres
def test_emit_only_matching_subscriptions() -> None:
    webhook.upsert_subscription(
        url=f"{TEST_URL_PREFIX}match",
        secret="s1",
        event_types=["job_failed"],
    )
    webhook.upsert_subscription(
        url=f"{TEST_URL_PREFIX}nomatch",
        secret="s2",
        event_types=["intraday_drawdown"],
    )
    webhook.emit_event("job_failed", {"job_type": "x"}, dedupe_key="test:match:1")
    pending = webhook.list_pending_deliveries()
    mine = [d for d in pending if d["url"].startswith(TEST_URL_PREFIX)]
    assert [d["url"] for d in mine] == [f"{TEST_URL_PREFIX}match"]


@pytest.mark.requires_postgres
def test_disabled_subscription_receives_nothing() -> None:
    webhook.upsert_subscription(
        url=f"{TEST_URL_PREFIX}disabled",
        secret="s3",
        event_types=["job_failed"],
        enabled=False,
    )
    webhook.emit_event("job_failed", {"job_type": "x"}, dedupe_key="test:disabled:1")
    pending = webhook.list_pending_deliveries()
    assert not [d for d in pending if d["url"] == f"{TEST_URL_PREFIX}disabled"]


@pytest.mark.requires_postgres
def test_failure_retry_then_dead() -> None:
    sub = webhook.upsert_subscription(
        url=f"{TEST_URL_PREFIX}retry",
        secret="s4",
        event_types=["job_failed"],
    )
    webhook.emit_event("job_failed", {"job_type": "x"}, dedupe_key="test:retry:1")
    pending = [d for d in webhook.list_pending_deliveries() if d["url"] == f"{TEST_URL_PREFIX}retry"]
    delivery_id = pending[0]["delivery_id"]

    for _ in range(webhook.MAX_ATTEMPTS):
        webhook.mark_delivery_failed(delivery_id, "conn refused")
    with webhook.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, attempts FROM webhook_deliveries WHERE id = %s",
                (delivery_id,),
            )
            row = cur.fetchone()
    assert row[0] == "dead" and row[1] == webhook.MAX_ATTEMPTS

    # Subscriptions list hides nothing; delete works.
    subs = webhook.list_subscriptions()
    assert any(s["id"] == sub["id"] for s in subs)
    assert webhook.delete_subscription(sub["id"]) is True
    assert webhook.delete_subscription(sub["id"]) is False
