from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from data_sync_service.service.dashboard import (
    dashboard_summary,
    dashboard_sync_parallel,
    dashboard_sync_stream,
)

router = APIRouter()


@router.get("/dashboard/summary")
def get_dashboard_summary(include_macro: bool = Query(True)) -> dict[str, Any]:
    try:
        return dashboard_summary(include_macro=bool(include_macro))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.post("/dashboard/sync")
def post_dashboard_sync(force: bool = Query(True), screeners: bool = Query(True)) -> dict[str, Any]:
    try:
        return dashboard_sync_parallel(force=bool(force), screeners=bool(screeners))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.get("/dashboard/sync/stream")
def get_dashboard_sync_stream(force: bool = Query(True), screeners: bool = Query(True)) -> StreamingResponse:
    def event_generator():
        for line in dashboard_sync_stream(force=bool(force), screeners=bool(screeners)):
            yield f"data: {line}\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

