from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel, Field

from data_sync_service.service.industry_fund_flow import (
    get_cn_industry_fund_flow,
    sync_cn_industry_fund_flow,
)
from data_sync_service.service.mainline import get_cn_industry_mainline, sync_cn_industry_mainline

router = APIRouter()


class IndustryFundFlowSyncRequest(BaseModel):
    days: int = Field(default=10, ge=1, le=60)
    topN: int = Field(default=10, ge=1, le=300)
    force: bool = False


class IndustryMainlineSyncRequest(BaseModel):
    asOfDate: str | None = None
    force: bool = False


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
    payload: IndustryFundFlowSyncRequest = IndustryFundFlowSyncRequest(),
) -> dict:
    try:
        return sync_cn_industry_fund_flow(days=payload.days, top_n=payload.topN, force=payload.force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/market/cn/industry-mainline")
def market_cn_industry_mainline(
    asOfDate: str | None = Query(None),
) -> dict:
    try:
        return get_cn_industry_mainline(as_of_date=asOfDate)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/market/cn/industry-mainline/sync")
def market_cn_industry_mainline_sync(payload: IndustryMainlineSyncRequest = IndustryMainlineSyncRequest()) -> dict:
    as_of = payload.asOfDate or None
    try:
        return sync_cn_industry_mainline(as_of_date=as_of, force=payload.force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
