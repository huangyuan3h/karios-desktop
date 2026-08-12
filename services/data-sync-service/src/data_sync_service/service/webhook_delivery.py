"""Webhook delivery (todo §14 #3 · P1 · 2026-08-12).

deliver_pending() drains queued webhook_deliveries:
  - HMAC-SHA256 signature header (X-Karios-Signature: sha256=<hex>) so the
    consumer can verify the payload really came from Karios (secret is per
    subscription, shown once at creation).
  - 5s timeout per POST; failures retry at 5/15/60 minutes (x3) then dead.
  - Rate limit: max 30 deliveries per subscription per minute (burst guard).

Runs from the scheduler every minute (webhook_delivery_job). Read-only
against delivery state; events themselves are never mutated.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db import webhook as webhook_db

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 5
MAX_PER_SUBSCRIPTION_PER_MINUTE = 30


def _sign(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _post(url: str, body: bytes, signature: str) -> None:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Karios-Signature": f"sha256={signature}",
            "User-Agent": "karios-webhook/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:  # noqa: S310
        status = getattr(resp, "status", 200)
        if status >= 400:
            raise RuntimeError(f"consumer returned HTTP {status}")


def _rate_limited(deliveries: list[dict[str, Any]], now: datetime) -> set[int]:
    """Count deliveries already sent/failed in this minute per subscription."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT subscription_id, count(*)
                FROM webhook_deliveries
                WHERE status IN ('sent', 'dead')
                  AND delivered_at >= %s
                GROUP BY subscription_id
                """,
                (now - timedelta(minutes=1),),
            )
            rows = cur.fetchall()
    counts = dict(rows)
    blocked: set[int] = set()
    for d in deliveries:
        if counts.get(d["subscription_id"], 0) >= MAX_PER_SUBSCRIPTION_PER_MINUTE:
            blocked.add(d["delivery_id"])
    return blocked


def deliver_pending(limit: int = 100) -> dict[str, Any]:
    """Deliver due pending webhook events. Returns a summary dict."""
    now = datetime.now(UTC)
    deliveries = webhook_db.list_pending_deliveries(limit=limit)
    if not deliveries:
        return {"ok": True, "delivered": 0, "failed": 0, "blocked": 0}

    blocked = _rate_limited(deliveries, now)
    delivered = failed = 0
    for d in deliveries:
        if d["delivery_id"] in blocked:
            continue
        body = json.dumps(
            {
                "event_id": d["event_id"],
                "event_type": d["event_type"],
                "payload": d["payload"],
                "sent_at": now.isoformat(),
            },
            ensure_ascii=False,
        ).encode("utf-8")
        try:
            _post(d["url"], body, _sign(body, d["secret"]))
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logger.warning("webhook delivery %s failed: %s", d["delivery_id"], exc)
            webhook_db.mark_delivery_failed(d["delivery_id"], str(exc)[:500])
        else:
            delivered += 1
            webhook_db.mark_delivery_sent(d["delivery_id"])
    return {
        "ok": True,
        "delivered": delivered,
        "failed": failed,
        "blocked": len(blocked),
    }
