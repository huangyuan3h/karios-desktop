"""Notification API (2026-08-12).

GET /api/notifications — aggregated actionable alerts for the UI hub
(stop/trail near-line + EXIT + cron failures + recon missing + rolling OOS
warning). Pure aggregation of existing products; see service/notifications.py.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from data_sync_service.service.notifications import build_notifications

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


@router.get("")
def notifications_list(mode: str = Query("single_track")) -> dict[str, Any]:
    """Actionable notifications (high first). Live default is ``single_track``
    so S-3 pyramid/recon appear. ``mode`` is accepted for client compatibility.
    Each item: id/type/severity/title/detail/anchor/lane/book/createdAt."""
    return {"ok": True, "items": build_notifications()}
