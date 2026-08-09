from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]

from data_sync_service.service.market_sentiment import (
    get_cn_sentiment,
    get_panic_cooldown,
    sync_cn_sentiment,
)

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


@router.get("/market/cn/sentiment/panic-cooldown")
def market_cn_sentiment_panic_cooldown(
    days: int = Query(10, ge=1, le=30),
    cooldownDays: int = Query(3, ge=0, le=10),
) -> dict:
    """S-3 panic protection status (matches the backtest engine semantics).

    Returns the most recent panic day (risk_mode in no_new_positions /
    extreme_caution) within ``days`` and the cooldown end date computed over
    the CN trade calendar (panic day + cooldownDays trading days — same as
    BacktestConfig.panic_cooldown_days). ``active`` is True when today is
    still inside the cooldown window: no new S-3 entries then.
    """
    try:
        return get_panic_cooldown(days=days, cooldown_days=cooldownDays)
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
