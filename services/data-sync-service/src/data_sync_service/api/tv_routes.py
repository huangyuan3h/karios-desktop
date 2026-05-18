from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel

from data_sync_service.service import tv as tvsvc

router = APIRouter()


class CreateTvScreenerRequest(BaseModel):
    name: str
    url: str
    enabled: bool = True


class UpdateTvScreenerRequest(BaseModel):
    name: str
    url: str
    enabled: bool


class MigrateTvFromSqliteRequest(BaseModel):
    sqlitePath: str | None = None


@router.get("/integrations/tradingview/screeners")
def list_tv_screeners() -> dict[str, Any]:
    return tvsvc.list_screeners()


@router.post("/integrations/tradingview/screeners")
def create_tv_screener(req: CreateTvScreenerRequest) -> dict[str, str]:
    return tvsvc.create_screener(name=req.name, url=req.url, enabled=req.enabled)


@router.put("/integrations/tradingview/screeners/{screener_id}")
def update_tv_screener(screener_id: str, req: UpdateTvScreenerRequest) -> dict[str, bool]:
    return tvsvc.update_screener(screener_id=screener_id, name=req.name, url=req.url, enabled=req.enabled)


@router.delete("/integrations/tradingview/screeners/{screener_id}")
def delete_tv_screener(screener_id: str) -> dict[str, bool]:
    return tvsvc.delete_screener(screener_id=screener_id)


@router.get("/integrations/tradingview/screeners/{screener_id}/snapshots")
def list_tv_screener_snapshots(screener_id: str, limit: int = Query(10, ge=1, le=50)) -> dict[str, Any]:
    return tvsvc.list_snapshots(screener_id=screener_id, limit=int(limit))


@router.get("/integrations/tradingview/snapshots/{snapshot_id}")
def get_tv_screener_snapshot(snapshot_id: str) -> dict[str, Any]:
    return tvsvc.get_snapshot(snapshot_id=snapshot_id)


@router.get("/integrations/tradingview/screeners/{screener_id}/history")
def tv_screener_history(screener_id: str, days: int = Query(10, ge=1, le=30)) -> dict[str, Any]:
    return tvsvc.screener_history(screener_id=screener_id, days=int(days))


@router.post("/integrations/tradingview/screeners/{screener_id}/sync")
def sync_tv_screener(screener_id: str) -> dict[str, Any]:
    return tvsvc.sync_screener(screener_id=screener_id)


@router.post("/integrations/tradingview/migrate/sqlite")
def migrate_tv_from_sqlite(req: MigrateTvFromSqliteRequest) -> dict[str, Any]:
    return tvsvc.migrate_from_sqlite(sqlite_path=req.sqlitePath)

