from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]

from data_sync_service.service.dashboard import dashboard_summary, dashboard_sync

router = APIRouter()


@router.get("/dashboard/summary")
def get_dashboard_summary() -> dict[str, Any]:
    try:
        return dashboard_summary()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.post("/dashboard/sync")
def post_dashboard_sync(force: bool = Query(True), screeners: bool = Query(True)) -> dict[str, Any]:
    try:
        return dashboard_sync(force=bool(force), screeners=bool(screeners))
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e

