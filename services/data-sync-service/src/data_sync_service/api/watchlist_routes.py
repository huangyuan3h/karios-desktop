from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel

# Field names must match @karios/shared WatchlistRegistryItemSchema
from data_sync_service.db.watchlist_automation import list_registry, upsert_registry
from data_sync_service.service.watchlist_automation import (
    ack_automation_run,
    get_automation_latest,
    get_automation_pending,
    get_automation_run,
    run_watchlist_automation,
)

router = APIRouter()


class WatchlistRegistryItem(BaseModel):
    symbol: str
    name: str | None = None
    addedAt: str | None = None
    source: str | None = None
    color: str | None = None
    positionPct: float | None = None
    costPrice: float | None = None
    maxPrice: float | None = None
    entryDate: str | None = None


class WatchlistRegistryRequest(BaseModel):
    items: list[WatchlistRegistryItem] = []


class WatchlistAckRequest(BaseModel):
    screenerAdded: int | None = None
    funnel: dict[str, Any] | None = None


@router.get("/watchlist/registry")
def get_watchlist_registry() -> dict:
    try:
        items = list_registry()
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/registry")
def watchlist_registry(req: WatchlistRegistryRequest) -> dict:
    try:
        items = [x.model_dump(exclude_none=False) for x in req.items]
        count = upsert_registry(items)
        return {"ok": True, "count": count}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/pending")
def watchlist_automation_pending(tradeDate: str | None = Query(None)) -> dict:
    try:
        pending = get_automation_pending(tradeDate)
        if not pending:
            return {"pending": False}
        return {"pending": True, **pending}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/latest")
def watchlist_automation_latest() -> dict:
    try:
        latest = get_automation_latest()
        if not latest:
            return {"found": False}
        return {"found": True, **latest}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/run")
def watchlist_automation_run(force: bool = Query(False)) -> dict:
    try:
        return run_watchlist_automation(trigger="manual", force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/{run_id}/ack")
def watchlist_automation_ack(run_id: str, req: WatchlistAckRequest | None = None) -> dict:
    try:
        screener_added = req.screenerAdded if req else None
        funnel = req.funnel if req else None
        row = ack_automation_run(run_id, screener_added=screener_added, funnel=funnel)
        if not row:
            raise HTTPException(status_code=404, detail="run not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/{run_id}")
def watchlist_automation_get(run_id: str) -> dict:
    try:
        row = get_automation_run(run_id)
        if not row:
            raise HTTPException(status_code=404, detail="run not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
