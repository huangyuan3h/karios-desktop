"""Research report endpoints (研报 → Alpha channel, TIP-012).

Responses use camelCase field names (project API convention).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

router = APIRouter(prefix="/api/research", tags=["research"])


def _report_row_to_camel(row: dict[str, Any]) -> dict[str, Any]:
    key_map = {
        "info_code": "infoCode",
        "stock_code": "stockCode",
        "stock_name": "stockName",
        "org_name": "orgName",
        "target_price": "targetPrice",
        "eps_this_year": "epsThisYear",
        "pe_this_year": "peThisYear",
        "industry_name": "industryName",
        "publish_date": "publishDate",
        "encode_url": "encodeUrl",
        "alpha_score": "alphaScore",
        "created_at": "createdAt",
    }
    out: dict[str, Any] = {}
    for k, v in row.items():
        out[key_map.get(k, k)] = v
    return out


@router.get("/reports")
def list_reports_endpoint(
    limit: int = Query(50, ge=1, le=200),
    days: int = Query(7, ge=1, le=60),
) -> dict[str, Any]:
    """Recent research reports (newest first)."""
    from data_sync_service.service.research import list_research_reports

    reports = [_report_row_to_camel(r) for r in list_research_reports(limit=limit, window_days=days)]
    return {"ok": True, "reports": reports}


@router.get("/stats")
def research_stats_endpoint() -> dict[str, Any]:
    """Ingestion stats: totals and 24h/7d counts."""
    from data_sync_service.service.research import research_stats

    return {"ok": True, "stats": research_stats()}


@router.post("/sync")
def research_sync_endpoint(days: int = Query(3, ge=1, le=14)) -> dict[str, Any]:
    """Force a research report sync (manual trigger)."""
    from data_sync_service.service.research import sync_research_reports

    result = sync_research_reports(days=days, max_pages=3)
    return {**result}
