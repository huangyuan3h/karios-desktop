"""Trade review API routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from data_sync_service.db import trade_review as trade_review_db

router = APIRouter()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class TradeReview(BaseModel):
    id: str
    symbol: str
    stockName: str | None = None
    buyDate: str | None = None
    sellDate: str | None = None
    holdingDays: int | None = None
    pnlAmount: float | None = None
    pnlPct: float | None = None
    totalCapitalImpactPct: float | None = None
    maxLossGuardrailPct: float = 2.0
    marketLightEntry: str | None = None
    marketLightExit: str | None = None
    buyLogicFundResonance: bool = False
    buyLogicPatternBreakout: bool = False
    buyLogicMacroSentiment: bool = False
    buyLogicNotes: str | None = None
    positionPct: float | None = None
    buyAvgPrice: float | None = None
    initialDefensePrice: float | None = None
    sellAvgPrice: float | None = None
    sellReason: str | None = None
    executionNotes: str | None = None
    goodActions: str | None = None
    improvementAreas: str | None = None
    customPayload: dict[str, Any] = Field(default_factory=dict)
    createdAt: str
    updatedAt: str


class TradeReviewCreateRequest(BaseModel):
    symbol: str
    stockName: str | None = None
    buyDate: str | None = None
    sellDate: str | None = None
    holdingDays: int | None = None
    pnlAmount: float | None = None
    pnlPct: float | None = None
    totalCapitalImpactPct: float | None = None
    maxLossGuardrailPct: float = 2.0
    marketLightEntry: str | None = None
    marketLightExit: str | None = None
    buyLogicFundResonance: bool = False
    buyLogicPatternBreakout: bool = False
    buyLogicMacroSentiment: bool = False
    buyLogicNotes: str | None = None
    positionPct: float | None = None
    buyAvgPrice: float | None = None
    initialDefensePrice: float | None = None
    sellAvgPrice: float | None = None
    sellReason: str | None = None
    executionNotes: str | None = None
    goodActions: str | None = None
    improvementAreas: str | None = None
    customPayload: dict[str, Any] = Field(default_factory=dict)


class TradeReviewUpdateRequest(BaseModel):
    symbol: str | None = None
    stockName: str | None = None
    buyDate: str | None = None
    sellDate: str | None = None
    holdingDays: int | None = None
    pnlAmount: float | None = None
    pnlPct: float | None = None
    totalCapitalImpactPct: float | None = None
    maxLossGuardrailPct: float | None = None
    marketLightEntry: str | None = None
    marketLightExit: str | None = None
    buyLogicFundResonance: bool | None = None
    buyLogicPatternBreakout: bool | None = None
    buyLogicMacroSentiment: bool | None = None
    buyLogicNotes: str | None = None
    positionPct: float | None = None
    buyAvgPrice: float | None = None
    initialDefensePrice: float | None = None
    sellAvgPrice: float | None = None
    sellReason: str | None = None
    executionNotes: str | None = None
    goodActions: str | None = None
    improvementAreas: str | None = None
    customPayload: dict[str, Any] | None = None


class ListTradeReviewsResponse(BaseModel):
    total: int
    items: list[TradeReview]


@router.get("/trade-reviews", response_model=ListTradeReviewsResponse)
def list_trade_reviews(
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    symbol: str | None = Query(None, description="Filter by ticker symbol, e.g. 000001"),
) -> ListTradeReviewsResponse:
    """List trade reviews with pagination."""
    total, items = trade_review_db.fetch_all(limit=limit, offset=offset, symbol=symbol)
    return ListTradeReviewsResponse(total=total, items=[TradeReview(**it) for it in items])


@router.get("/trade-reviews/{review_id}", response_model=TradeReview)
def get_trade_review(review_id: str) -> TradeReview:
    """Get one trade review by id."""
    rid = (review_id or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="review_id is required")
    review = trade_review_db.fetch_by_id(rid)
    if not review:
        raise HTTPException(status_code=404, detail="Trade review not found")
    return TradeReview(**review)


@router.post("/trade-reviews", response_model=TradeReview)
def create_trade_review(req: TradeReviewCreateRequest) -> TradeReview:
    """Create one trade review."""
    symbol = (req.symbol or "").strip()
    if not symbol:
        raise HTTPException(status_code=400, detail="symbol is required")
    now = _now_iso()
    rid = str(uuid4())
    review = trade_review_db.create_review(
        review_id=rid,
        payload={**req.model_dump(), "symbol": symbol},
        created_at=now,
        updated_at=now,
    )
    return TradeReview(**review)


@router.put("/trade-reviews/{review_id}", response_model=TradeReview)
def update_trade_review(review_id: str, req: TradeReviewUpdateRequest) -> TradeReview:
    """Patch-update one trade review."""
    rid = (review_id or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="review_id is required")
    payload = req.model_dump(exclude_none=True)
    if "symbol" in payload:
        payload["symbol"] = str(payload["symbol"]).strip()
    now = _now_iso()
    review = trade_review_db.update_review(review_id=rid, payload=payload, updated_at=now)
    if not review:
        raise HTTPException(status_code=404, detail="Trade review not found")
    return TradeReview(**review)


@router.delete("/trade-reviews/{review_id}")
def delete_trade_review(review_id: str) -> dict[str, Any]:
    """Delete one trade review."""
    rid = (review_id or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="review_id is required")
    ok = trade_review_db.delete_review(rid)
    if not ok:
        raise HTTPException(status_code=404, detail="Trade review not found")
    return {"ok": True}
