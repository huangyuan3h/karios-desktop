"""Webhook subscription management (todo §14 #3 · P1 · 2026-08-12).

Endpoints:
- POST /api/webhook/subscriptions   — create (secret auto-generated) or update
- GET  /api/webhook/subscriptions   — list (secret echoed for convenience)
- DELETE /api/webhook/subscriptions/{id}
- POST /api/webhook/test            — emit a test event to verify connectivity

Consumers verify payloads with HMAC-SHA256 (X-Karios-Signature header).
Event catalog: job_failed, intraday_drawdown (P1); more events in P2.
"""

from __future__ import annotations

import secrets
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from data_sync_service.db import webhook as webhook_db

router = APIRouter(prefix="/api/webhook", tags=["webhook"])

KNOWN_EVENT_TYPES = {
    "job_failed",
    "intraday_drawdown",
    "paper_chain_issue",
    "near_stop",
    "oos_warning",
    "recon_missing",
    "candidate_added",
    "test",
}


class SubscriptionRequest(BaseModel):
    url: str = Field(..., min_length=8, max_length=2048)
    event_types: list[str] = Field(..., min_length=1)
    enabled: bool = True
    secret: str | None = Field(default=None, min_length=8, max_length=256)


@router.get("/subscriptions")
def list_subscriptions() -> dict[str, Any]:
    """All webhook subscriptions (url + event types + enabled)."""
    return {"ok": True, "items": webhook_db.list_subscriptions()}


@router.post("/subscriptions")
def create_subscription(req: SubscriptionRequest) -> dict[str, Any]:
    """Create (or update when `id` provided) a subscription. The HMAC secret
    is auto-generated unless supplied; treat it like a password."""
    unknown = set(req.event_types) - KNOWN_EVENT_TYPES
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"unknown event type(s): {sorted(unknown)}; known: {sorted(KNOWN_EVENT_TYPES)}",
        )
    sub = webhook_db.upsert_subscription(
        url=req.url,
        secret=req.secret or secrets.token_hex(16),
        event_types=req.event_types,
        enabled=req.enabled,
    )
    return {"ok": True, "subscription": sub}


@router.delete("/subscriptions/{sub_id}")
def delete_subscription(sub_id: int) -> dict[str, Any]:
    if not webhook_db.delete_subscription(sub_id):
        raise HTTPException(status_code=404, detail="subscription not found")
    return {"ok": True}


@router.post("/test")
def send_test() -> dict[str, Any]:
    """Emit a `test` event (delivered by the scheduler's next minute tick)."""
    ok = webhook_db.emit_event(
        "test",
        {"message": "webhook connectivity test"},
        dedupe_key=f"test:{secrets.token_hex(8)}",
    )
    return {"ok": ok, "note": "delivered at the next scheduler minute tick"}
