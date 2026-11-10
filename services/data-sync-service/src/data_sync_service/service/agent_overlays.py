"""Agent overlays — human-confirmed annotations, never engine rewrites.

P0 ships one type: ``hold_same_key_rebuy``. It records "do not rebuy the ETF
key we just trail-exited until <date>" so the Watchlist, the parking health
card and the agent contract all read the same server-side state instead of a
browser ``localStorage`` memory.

Storage is the existing ``app_settings`` JSONB store (no fifth ledger, no
schema migration). An overlay can be *proposed* by the agent and becomes
effective only after Yuan accepts it. Overlays never change
``sleeve.action``, ``TRAIL_PCT``, the parking cooldown (k=0) or any weight.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import app_settings

logger = logging.getLogger(__name__)

OVERLAY_SETTINGS_KEY = "agent_overlays_v1"
OVERLAY_TYPES = frozenset({"hold_same_key_rebuy"})
OVERLAY_STATUSES = frozenset({"proposed", "accepted", "rejected", "cleared"})
DEFAULT_VALID_DAYS = 5
MAX_OVERLAYS = 60


def _today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _norm_key(value: str | None) -> str | None:
    s = str(value or "").strip().upper()
    return s or None


def _load_raw() -> list[dict[str, Any]]:
    try:
        raw = app_settings.get_setting(OVERLAY_SETTINGS_KEY, [])
    except Exception as exc:  # noqa: BLE001 — read path is fail-open
        logger.warning("agent overlays read failed: %s", exc)
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and item.get("id"):
            out.append(dict(item))
    return out


def _save_raw(items: list[dict[str, Any]]) -> None:
    try:
        app_settings.set_setting(OVERLAY_SETTINGS_KEY, items[-MAX_OVERLAYS:])
    except Exception as exc:  # noqa: BLE001 — a failed write must not break a read
        logger.warning("agent overlays write failed: %s", exc)


def _effective_status(item: dict[str, Any], day: str) -> str:
    status = str(item.get("status") or "proposed")
    valid_until = str(item.get("validUntil") or "")[:10]
    if status in ("proposed", "accepted") and valid_until and valid_until < day:
        return "expired"
    return status


def _decorate(item: dict[str, Any], day: str) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "type": item.get("type"),
        "pickKey": item.get("pickKey"),
        "symbol": item.get("symbol"),
        "status": item.get("status"),
        "effectiveStatus": _effective_status(item, day),
        "validFrom": item.get("validFrom"),
        "validUntil": item.get("validUntil"),
        "note": item.get("note"),
        "source": item.get("source"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
    }


def default_valid_until(day: str, valid_days: int = DEFAULT_VALID_DAYS) -> str:
    """``valid_days`` open sessions after ``day`` (calendar-aware, fail-open)."""
    days = max(1, int(valid_days))
    try:
        from data_sync_service.service.trade_calendar_utils import nth_open_date_from

        iso = nth_open_date_from(day, days + 1)
        if iso:
            return iso
    except Exception:  # noqa: BLE001 — unseeded calendar → calendar-day fallback
        pass
    try:
        d = date.fromisoformat(str(day)[:10])
    except ValueError:
        d = date.today()
    return (d + timedelta(days=days * 2 + 2)).isoformat()


def list_overlays(
    *,
    day: str | None = None,
    status: str | None = None,
    pick_key: str | None = None,
    include_expired: bool = True,
) -> list[dict[str, Any]]:
    """All overlays, newest first, with ``effectiveStatus`` computed for ``day``."""
    as_of = str(day or _today_iso())[:10]
    want_key = _norm_key(pick_key)
    out: list[dict[str, Any]] = []
    for item in reversed(_load_raw()):
        rec = _decorate(item, as_of)
        if want_key and rec.get("pickKey") != want_key:
            continue
        if status and rec.get("effectiveStatus") != status:
            continue
        if not include_expired and rec.get("effectiveStatus") == "expired":
            continue
        out.append(rec)
    return out


def group_overlays(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Split overlays for the contract / UI (active = accepted, not expired)."""
    active = [o for o in items if o.get("effectiveStatus") == "accepted"]
    proposed = [o for o in items if o.get("effectiveStatus") == "proposed"]
    expired = [o for o in items if o.get("effectiveStatus") == "expired"]
    return {"active": active, "proposed": proposed, "expired": expired, "all": items}


def find_same_key_overlay(pick_key: str | None, *, day: str | None = None) -> dict[str, Any] | None:
    """Newest accepted (else proposed) ``hold_same_key_rebuy`` for one key."""
    key = _norm_key(pick_key)
    if not key:
        return None
    items = list_overlays(day=day, pick_key=key)
    accepted = [o for o in items if o.get("effectiveStatus") == "accepted"]
    if accepted:
        return accepted[0]
    proposed = [o for o in items if o.get("effectiveStatus") == "proposed"]
    return proposed[0] if proposed else None


def upsert_overlay(
    *,
    overlay_type: str = "hold_same_key_rebuy",
    pick_key: str,
    symbol: str | None = None,
    status: str = "proposed",
    valid_until: str | None = None,
    valid_days: int | None = None,
    note: str | None = None,
    source: str = "agent",
    day: str | None = None,
) -> dict[str, Any]:
    """Create or update the overlay for ``(type, pickKey)``.

    Re-posting the same key with ``status='accepted'`` is how Yuan confirms an
    agent proposal; ``status='cleared'`` removes the effective state. The
    overlay's ``validUntil`` is kept unless explicitly replaced.
    """
    kind = str(overlay_type or "").strip()
    if kind not in OVERLAY_TYPES:
        raise ValueError(f"type must be one of {sorted(OVERLAY_TYPES)} (got {kind!r})")
    state = str(status or "").strip()
    if state not in OVERLAY_STATUSES:
        raise ValueError(f"status must be one of {sorted(OVERLAY_STATUSES)} (got {state!r})")
    key = _norm_key(pick_key)
    if not key:
        raise ValueError("pickKey is required")
    as_of = str(day or _today_iso())[:10]
    now_iso = _now_iso()
    items = _load_raw()
    existing: dict[str, Any] | None = None
    for item in items:
        if (
            str(item.get("type") or "") == kind
            and _norm_key(item.get("pickKey")) == key
            and str(item.get("status") or "") not in ("cleared", "rejected")
        ):
            existing = item
            break

    until = str(valid_until or "")[:10] or None
    if existing is None:
        until = until or default_valid_until(as_of, valid_days or DEFAULT_VALID_DAYS)
        rec: dict[str, Any] = {
            "id": f"ov-{uuid.uuid4().hex[:10]}",
            "type": kind,
            "pickKey": key,
            "symbol": symbol,
            "status": state,
            "validFrom": as_of,
            "validUntil": until,
            "note": note,
            "source": source,
            "createdAt": now_iso,
            "updatedAt": now_iso,
        }
        items.append(rec)
    else:
        rec = existing
        rec.update(
            {
                "status": state,
                "symbol": symbol or rec.get("symbol"),
                "note": note if note is not None else rec.get("note"),
                "source": source or rec.get("source"),
                "updatedAt": now_iso,
            }
        )
        if until:
            rec["validUntil"] = until
        elif state in ("accepted", "proposed") and not rec.get("validUntil"):
            rec["validUntil"] = default_valid_until(as_of, valid_days or DEFAULT_VALID_DAYS)
    _save_raw(items)
    return _decorate(rec, as_of)


def update_overlay_status(
    overlay_id: str,
    *,
    status: str,
    day: str | None = None,
    note: str | None = None,
) -> dict[str, Any] | None:
    """Update one overlay by id (used by the UI's confirm / clear buttons)."""
    state = str(status or "").strip()
    if state not in OVERLAY_STATUSES:
        raise ValueError(f"status must be one of {sorted(OVERLAY_STATUSES)} (got {state!r})")
    as_of = str(day or _today_iso())[:10]
    items = _load_raw()
    for item in items:
        if item.get("id") != overlay_id:
            continue
        item["status"] = state
        item["updatedAt"] = _now_iso()
        if note is not None:
            item["note"] = note
        _save_raw(items)
        return _decorate(item, as_of)
    return None


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()
