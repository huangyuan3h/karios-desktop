"""News RSS API routes."""

from __future__ import annotations

from fastapi import APIRouter

from data_sync_service.db.news import (
    count_by_enrichment_status,
    create_source,
    delete_source,
    ensure_tables,
    fetch_items,
    fetch_sources,
    mark_item_important,
    mark_item_read,
    update_source,
)
from data_sync_service.service.news import add_default_sources, fetch_all_sources

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


@router.get("/enrichment/status")
def enrichment_status():
    """Return counts of news_items by enrichment_status for monitoring."""
    ensure_tables()
    return {"counts": count_by_enrichment_status()}


@router.post("/enrichment/run")
def run_enrichment(max_batches: int = 10):
    """Manually trigger LLM enrichment on pending items."""
    ensure_tables()
    from data_sync_service.service.news_enrich import run_enrichment_cycle

    summary = run_enrichment_cycle(max_batches=max_batches)
    return summary


@router.get("/brief/latest")
def get_latest_brief(brief_type: str | None = None):
    """Fetch the most recent morning/midday brief."""
    ensure_tables()
    from data_sync_service.db.morning_brief import fetch_latest_brief

    brief = fetch_latest_brief(brief_type=brief_type)
    return {"brief": brief}


@router.get("/brief/recent")
def get_recent_briefs(limit: int = 7):
    """Fetch the most recent N briefs."""
    ensure_tables()
    from data_sync_service.db.morning_brief import fetch_recent_briefs

    briefs = fetch_recent_briefs(limit=limit)
    return {"briefs": briefs}


@router.post("/brief/generate")
def generate_brief(brief_type: str = "morning"):
    """Manually generate a news brief (morning/midday) or a trading-session
    brief (trading-open/trading-midday/trading-action)."""
    ensure_tables()
    if brief_type.startswith("trading-"):
        from data_sync_service.service.trading_brief import generate_trading_brief

        brief = generate_trading_brief(brief_type.removeprefix("trading-"))
        return {"brief": brief}

    from data_sync_service.service.morning_brief import generate_brief

    brief = generate_brief(brief_type=brief_type)
    return {"brief": brief}