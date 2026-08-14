"""api/webhook_routes.py + service/webhook_delivery.py coverage."""

from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi import HTTPException

from data_sync_service.api import webhook_routes as wr
from data_sync_service.service import webhook_delivery as wd


class TestRoutes:
    def test_create_subscription_generates_secret(self, monkeypatch) -> None:
        captured: dict = {}

        def fake_upsert(**kw):
            captured.update(kw)
            return {"id": 1, **kw}

        monkeypatch.setattr(wr.webhook_db, "upsert_subscription", fake_upsert)
        out = wr.create_subscription(
            wr.SubscriptionRequest(url="https://hook.example/a", event_types=["job_failed"])
        )
        assert out["ok"] is True
        assert len(captured["secret"]) == 32  # token_hex(16)
        assert captured["event_types"] == ["job_failed"]

    def test_create_subscription_unknown_type(self) -> None:
        with pytest.raises(HTTPException) as exc:
            wr.create_subscription(
                wr.SubscriptionRequest(url="https://hook.example/a", event_types=["nope"])
            )
        assert exc.value.status_code == 422

    def test_list_and_delete(self, monkeypatch) -> None:
        monkeypatch.setattr(wr.webhook_db, "list_subscriptions", lambda: [{"id": 1}])
        assert wr.list_subscriptions()["items"] == [{"id": 1}]
        monkeypatch.setattr(wr.webhook_db, "delete_subscription", lambda i: True)
        assert wr.delete_subscription(1)["ok"] is True
        monkeypatch.setattr(wr.webhook_db, "delete_subscription", lambda i: False)
        with pytest.raises(HTTPException) as exc:
            wr.delete_subscription(9)
        assert exc.value.status_code == 404

    def test_send_test(self, monkeypatch) -> None:
        monkeypatch.setattr(wr.webhook_db, "emit_event", lambda *a, **k: True)
        out = wr.send_test()
        assert out["ok"] is True


class TestDelivery:
    def _payload(self, d: dict) -> bytes:
        return json.dumps(
            {
                "event_id": d["event_id"],
                "event_type": d["event_type"],
                "payload": d["payload"],
                "sent_at": "2026-08-12T00:00:00+00:00",
            },
            ensure_ascii=False,
        ).encode("utf-8")

    def test_signature_matches_consumer_verification(self) -> None:
        body = b'{"a":1}'
        sig = wd._sign(body, "sekrit")
        expected = hmac.new(b"sekrit", body, hashlib.sha256).hexdigest()
        assert sig == expected
        assert sig != wd._sign(body, "other-sekrit")

    def test_deliver_success(self, monkeypatch) -> None:
        delivery = {
            "delivery_id": 1,
            "event_id": 7,
            "subscription_id": 3,
            "event_type": "job_failed",
            "payload": {"job_type": "x"},
            "url": "https://hook.example/x",
            "secret": "sekrit",
        }
        monkeypatch.setattr(wd, "datetime", type("DT", (), {"now": lambda tz=None: __import__("datetime").datetime(2026, 8, 12)}))
        captured: dict = {}

        def fake_post(url, body, signature):
            captured["url"] = url
            captured["signature"] = signature
            assert hmac.compare_digest(signature, wd._sign(body, "sekrit"))

        monkeypatch.setattr(wd, "_post", fake_post)
        monkeypatch.setattr(wd.webhook_db, "list_pending_deliveries", lambda limit=100: [delivery])
        monkeypatch.setattr(wd.webhook_db, "mark_delivery_sent", lambda i: None)
        monkeypatch.setattr(wd, "_rate_limited", lambda d, now: set())
        out = wd.deliver_pending()
        assert out == {"ok": True, "delivered": 1, "failed": 0, "blocked": 0}
        assert captured["url"] == "https://hook.example/x"

    def test_deliver_failure_marks_retry(self, monkeypatch) -> None:
        delivery = {
            "delivery_id": 2,
            "event_id": 8,
            "subscription_id": 3,
            "event_type": "job_failed",
            "payload": {},
            "url": "https://hook.example/y",
            "secret": "sekrit",
        }
        monkeypatch.setattr(wd, "_post", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("refused")))
        monkeypatch.setattr(wd.webhook_db, "list_pending_deliveries", lambda limit=100: [delivery])
        marks: list[int] = []
        monkeypatch.setattr(wd.webhook_db, "mark_delivery_failed", lambda i, e: marks.append(i))
        monkeypatch.setattr(wd, "_rate_limited", lambda d, now: set())
        out = wd.deliver_pending()
        assert out["failed"] == 1 and marks == [2]

    def test_rate_limited_deliveries_skipped(self, monkeypatch) -> None:
        delivery = {
            "delivery_id": 3,
            "event_id": 9,
            "subscription_id": 3,
            "event_type": "job_failed",
            "payload": {},
            "url": "https://hook.example/z",
            "secret": "sekrit",
        }
        monkeypatch.setattr(wd.webhook_db, "list_pending_deliveries", lambda limit=100: [delivery])
        monkeypatch.setattr(wd, "_rate_limited", lambda d, now: {3})
        monkeypatch.setattr(wd, "_post", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not post")))
        out = wd.deliver_pending()
        assert out["blocked"] == 1 and out["delivered"] == 0

    def test_deliver_bark_provider_formats_body(self, monkeypatch) -> None:
        delivery = {
            "delivery_id": 9,
            "event_id": 12,
            "subscription_id": 4,
            "event_type": "execution_card",
            "payload": {"day": "2026-08-14", "gate": {"A股": {"regime": "Diverging"}},
                        "candidates": [], "exits": []},
            "url": "https://api.day.app/device-key",
            "secret": "sekrit",
            "provider": "bark",
        }
        captured: dict = {}

        def fake_post(url, body, signature):
            captured["url"] = url
            captured["body"] = json.loads(body)
            assert hmac.compare_digest(signature, wd._sign(body, "sekrit"))

        monkeypatch.setattr(wd, "_post", fake_post)
        monkeypatch.setattr(wd.webhook_db, "list_pending_deliveries", lambda limit=100: [delivery])
        monkeypatch.setattr(wd.webhook_db, "mark_delivery_sent", lambda i: None)
        monkeypatch.setattr(wd, "_rate_limited", lambda d, now: set())
        out = wd.deliver_pending()
        assert out == {"ok": True, "delivered": 1, "failed": 0, "blocked": 0}
        assert captured["url"] == "https://api.day.app/device-key"
        assert captured["body"]["title"].startswith("📋 执行卡")
        assert "A股" in captured["body"]["body"]
