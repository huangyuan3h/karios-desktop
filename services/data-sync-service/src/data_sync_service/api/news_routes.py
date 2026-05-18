"""News RSS API routes."""

from __future__ import annotations

from fastapi import APIRouter

from data_sync_service.db.news import (
    ensure_tables,
    fetch_sources,
    fetch_items,
    create_source,
    update_source,
    delete_source,
    mark_item_read,
    mark_item_important,
)
from data_sync_service.service.news import fetch_all_sources, add_default_sources

router = APIRouter(prefix="/api/news", tags=["news"])


@router.get("/sources")
def list_sources(enabled_only: bool = False):
    ensure_tables()
    return {"sources": fetch_sources(enabled_only=enabled_only)}


@router.post("/sources")
def add_source(body: dict):
    ensure_tables()
    import uuid

    sid = body.get("id") or str(uuid.uuid4())[:8]
    name = body.get("name", "")
    url = body.get("url", "")
    enabled = body.get("enabled", True)
    if not name or not url:
        return {"error": "name and url are required"}
    src = create_source(source_id=sid, name=name, url=url, enabled=enabled)
    return {"source": src}


@router.patch("/sources/{source_id}")
def patch_source(source_id: str, body: dict):
    ensure_tables()
    name = body.get("name")
    enabled = body.get("enabled")
    src = update_source(source_id=source_id, name=name, enabled=enabled)
    if not src:
        return {"error": "source not found"}
    return {"source": src}


@router.delete("/sources/{source_id}")
def remove_source(source_id: str):
    ensure_tables()
    ok = delete_source(source_id)
    return {"deleted": ok}


@router.get("/items")
def list_items(
    limit: int = 100,
    offset: int = 0,
    source_id: str | None = None,
    is_read: bool | None = None,
    hours: int | None = 24,
):
    ensure_tables()
    total, items = fetch_items(
        limit=limit,
        offset=offset,
        source_id=source_id,
        is_read=is_read,
        hours=hours,
    )
    return {"total": total, "items": items}


@router.post("/items/{item_id}/read")
def set_item_read(item_id: str):
    ensure_tables()
    ok = mark_item_read(item_id)
    return {"updated": ok}


@router.post("/items/{item_id}/important")
def set_item_important(item_id: str, body: dict):
    ensure_tables()
    is_important = body.get("important", True)
    ok = mark_item_important(item_id, is_important)
    return {"updated": ok}


@router.post("/refresh")
def refresh_feeds():
    ensure_tables()
    results = fetch_all_sources()
    return {"results": results}


@router.post("/init-defaults")
def init_defaults():
    ensure_tables()
    add_default_sources()
    return {"ok": True}