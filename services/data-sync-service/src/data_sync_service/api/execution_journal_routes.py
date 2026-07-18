"""Execution decision journal API."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from data_sync_service.db import execution_journal as ej_db
from data_sync_service.service import execution_journal as ej_svc

router = APIRouter(prefix="/execution", tags=["execution-journal"])

VALID_SOURCES = frozenset({"sync_all", "poll", "registry", "manual", "eod"})


class SnapshotIngestRequest(BaseModel):
    source: str
    tradeDate: str
    gate: dict[str, Any] = Field(default_factory=dict)
    cards: list[dict[str, Any]] = Field(default_factory=list)
    meta: dict[str, Any] | None = None


class SnapshotIngestResponse(BaseModel):
    snapshotId: str
    changed: bool
    heartbeat: bool = False
    snapshot: dict[str, Any]
    changes: list[dict[str, Any]] = Field(default_factory=list)


class SnapshotListResponse(BaseModel):
    items: list[dict[str, Any]]


class ChangeListResponse(BaseModel):
    items: list[dict[str, Any]]


@router.post("/snapshots", response_model=SnapshotIngestResponse)
def post_snapshot(req: SnapshotIngestRequest) -> SnapshotIngestResponse:
    source = (req.source or "").strip()
    if source not in VALID_SOURCES:
        raise HTTPException(
            status_code=400,
            detail=f"source must be one of {sorted(VALID_SOURCES)}",
        )
    trade_date = (req.tradeDate or "").strip()
    if len(trade_date) < 10:
        raise HTTPException(status_code=400, detail="tradeDate is required (YYYY-MM-DD)")
    if not isinstance(req.gate, dict):
        raise HTTPException(status_code=400, detail="gate must be an object")
    result = ej_svc.ingest_snapshot(
        trade_date=trade_date[:10],
        source=source,
        gate=req.gate,
        cards=list(req.cards or []),
        meta=req.meta,
    )
    return SnapshotIngestResponse(**result)


@router.get("/snapshots", response_model=SnapshotListResponse)
def list_snapshots(
    trade_date: str | None = Query(None, alias="trade_date"),
    limit: int = Query(50, ge=1, le=200),
) -> SnapshotListResponse:
    items = ej_db.list_snapshots(trade_date=trade_date, limit=limit)
    return SnapshotListResponse(items=items)


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(snapshot_id: str) -> dict[str, Any]:
    sid = (snapshot_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="snapshot_id is required")
    snap = ej_db.fetch_snapshot_by_id(sid)
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snap


@router.get("/changes", response_model=ChangeListResponse)
def list_changes(
    trade_date: str | None = Query(None, alias="trade_date"),
    since: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> ChangeListResponse:
    items = ej_db.list_changes(trade_date=trade_date, since=since, limit=limit)
    return ChangeListResponse(items=items)


@router.get("/journal.md")
def get_journal_md(
    trade_date: str | None = Query(None, alias="trade_date"),
    days: int = Query(5, ge=1, le=30),
) -> Response:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    td = (trade_date or "").strip()
    if not td:
        td = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    else:
        td = td[:10]
    body = ej_svc.build_journal_markdown(trade_date=td, days=days)
    return Response(content=body, media_type="text/markdown; charset=utf-8")
