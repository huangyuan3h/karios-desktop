from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]

from data_sync_service.service.market_sentiment import get_cn_sentiment, sync_cn_sentiment

router = APIRouter()


@router.get("/market/cn/sentiment")
def market_cn_sentiment(
    days: int = Query(10, ge=1, le=30),
    asOfDate: str | None = Query(None),
) -> dict:
    try:
        return get_cn_sentiment(days=days, as_of_date=asOfDate)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/market/cn/sentiment/sync")
def market_cn_sentiment_sync(payload: dict) -> dict:
    date_str = str(payload.get("date") or "").strip()
    force = bool(payload.get("force") or False)
    if not date_str:
        date_str = datetime.now(tz=UTC).date().isoformat()
    try:
        return sync_cn_sentiment(date_str=date_str, force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
