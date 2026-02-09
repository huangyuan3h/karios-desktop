from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]

from data_sync_service.service.industry_fund_flow import (
    get_cn_industry_fund_flow,
    sync_cn_industry_fund_flow,
)

router = APIRouter()


@router.get("/market/cn/industry-fund-flow")
def market_cn_industry_fund_flow(
    days: int = Query(10, ge=1, le=60),
    topN: int = Query(30, ge=1, le=300),
    asOfDate: str | None = Query(None),
) -> dict:
    try:
        return get_cn_industry_fund_flow(days=days, top_n=topN, as_of_date=asOfDate)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/market/cn/industry-fund-flow/sync")
def market_cn_industry_fund_flow_sync(
    payload: dict,
) -> dict:
    days = int(payload.get("days") or 10)
    top_n = int(payload.get("topN") or 10)
    try:
        return sync_cn_industry_fund_flow(days=days, top_n=top_n)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
