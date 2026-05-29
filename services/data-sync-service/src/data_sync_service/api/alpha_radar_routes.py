"""Alpha Radar API routes."""

from __future__ import annotations

from fastapi import APIRouter

from data_sync_service.db.alpha_radar import (
    ensure_tables,
    fetch_documents,
    fetch_sources,
    fetch_trend_by_id,
    fetch_trends,
    get_meta,
    update_trend_risk_status,
    delete_trend_by_id,
)
from data_sync_service.service.alpha_radar_ingest import add_default_sources, fetch_all_sources
from data_sync_service.service.alpha_radar_process import process_document, process_pending_documents
from data_sync_service.service.alpha_radar_pipeline import pipeline_status, run_alpha_radar_pipeline
from data_sync_service.service.alpha_radar_risk import build_mainline_score_map, compute_risk_status
from data_sync_service.service.mainline import get_cn_industry_mainline
from data_sync_service.service.alpha_radar_mapping import remap_trend_by_id
from data_sync_service.service.alpha_radar_catalyst import list_catalyst_stocks

router = APIRouter(prefix="/api/alpha-radar", tags=["alpha-radar"])


@router.get("/sources")
def list_sources(enabled_only: bool = True, category: str | None = None):
    ensure_tables()
    return {"sources": fetch_sources(enabled_only=enabled_only, category=category)}


@router.post("/init-defaults")
def init_defaults():
    ensure_tables()
    add_default_sources()
    return {"ok": True}


@router.get("/documents")
def list_documents(
    limit: int = 50,
    offset: int = 0,
    category: str | None = None,
    processing_status: str | None = None,
    hours: int | None = None,
):
    ensure_tables()
    total, items = fetch_documents(
        limit=limit,
        offset=offset,
        category=category,
        processing_status=processing_status,
        hours=hours,
    )
    return {"total": total, "items": items}


@router.get("/status")
def alpha_radar_status():
    ensure_tables()
    return {"ok": True, **pipeline_status()}


@router.post("/run-pipeline")
def run_pipeline(body: dict | None = None):
    ensure_tables()
    body = body or {}
    force = bool(body.get("force", False))
    result = run_alpha_radar_pipeline(force=force, trigger="manual")
    return {"ok": True, **result}


@router.post("/generate-daily")
def generate_daily(body: dict | None = None):
    """Backward-compatible alias for run-pipeline."""
    ensure_tables()
    body = body or {}
    force = bool(body.get("force", False))
    result = run_alpha_radar_pipeline(force=force, trigger="manual")
    return {"ok": True, **result}


@router.get("/catalyst-stocks")
def catalyst_stocks(limit: int = 50, maxAgeDays: int | None = None):
    ensure_tables()
    return list_catalyst_stocks(limit=limit, max_age_days=maxAgeDays)


@router.get("/trends")
def list_trends(
    limit: int = 50,
    offset: int = 0,
    document_id: str | None = None,
    risk_status: str | None = None,
    day: str | None = None,
    since: str | None = None,
    latest_batch: bool = True,
):
    ensure_tables()
    day_filter = day if day != "all" else None
    since_filter = since
    if latest_batch and since_filter is None and day_filter is None:
        since_filter = get_meta("last_batch_started_at")
    total, items = fetch_trends(
        limit=limit,
        offset=offset,
        document_id=document_id,
        risk_status=risk_status,
        day=day_filter,
        since=since_filter,
    )
    return {
        "total": total,
        "items": items,
        "day": day_filter,
        "since": since_filter,
    }


@router.post("/sync")
def sync_feeds(body: dict | None = None):
    ensure_tables()
    body = body or {}
    enrich = body.get("enrichFulltext")
    if enrich is None:
        enrich_opt: bool | None = None
    else:
        enrich_opt = bool(enrich)
    apply_filter = bool(body.get("applyFilter", True))
    ingest_result = fetch_all_sources(enrich_fulltext=enrich_opt, apply_filter=apply_filter)
    return {"ok": True, **ingest_result}


@router.post("/process")
def process_feeds(body: dict | None = None):
    ensure_tables()
    body = body or {}
    limit = int(body.get("limit") or 10)
    map_cn = bool(body.get("mapCn", True))
    mode = str(body.get("mode") or "batch").strip().lower()
    document_id = body.get("documentId")
    if document_id:
        result = process_document(str(document_id), map_cn=map_cn)
        return {"ok": True, **result}
    result = process_pending_documents(limit=limit, map_cn=map_cn, mode=mode)
    return {"ok": True, **result}


@router.post("/trends/{trend_id}/refresh-risk")
def refresh_trend_risk(trend_id: str):
    ensure_tables()
    trend = fetch_trend_by_id(trend_id)
    if not trend:
        return {"error": "trend not found"}

    mainline = get_cn_industry_mainline()
    mainline_map = build_mainline_score_map(mainline)
    hot_names = [
        str(r.get("industryName") or "").strip()
        for r in (mainline.get("currentMainline") or [])
        if str(r.get("industryName") or "").strip()
    ]
    keywords = list(trend.get("keywordsForMapping") or [])
    risk_status = compute_risk_status(
        keywords=keywords,
        hot_industry_names=hot_names,
        mainline_by_industry=mainline_map,
    )
    update_trend_risk_status(trend_id, risk_status)
    return {"ok": True, "trendId": trend_id, "riskStatus": risk_status}


@router.post("/trends/{trend_id}/remap")
def remap_trend(trend_id: str):
    ensure_tables()
    try:
        result = remap_trend_by_id(trend_id)
        return {"ok": True, **result}
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.delete("/trends/{trend_id}")
def delete_trend(trend_id: str):
    ensure_tables()
    if not fetch_trend_by_id(trend_id):
        return {"ok": False, "error": "trend not found"}
    deleted = delete_trend_by_id(trend_id)
    return {"ok": deleted, "trendId": trend_id}
