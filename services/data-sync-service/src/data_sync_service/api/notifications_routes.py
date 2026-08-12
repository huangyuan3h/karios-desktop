"""Notification API (2026-08-12).

GET /api/notifications — aggregated actionable alerts for the UI hub
(stop/trail near-line + EXIT + cron failures + recon missing + rolling OOS
warning). Pure aggregation of existing products; see service/notifications.py.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from data_sync_service.service.notifications import build_notifications

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


@router.get("")
def notifications_list() -> dict[str, Any]:
    """Actionable notifications (high first). Each item: id/type/severity/
    title/detail/anchor/createdAt — anchor names a watchlist-page block
    (holdings | recon | scheduler | backtest) the UI scrolls to."""
    return {"ok": True, "items": build_notifications()}
