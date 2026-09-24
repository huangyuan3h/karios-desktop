"""User trade journal routes (real buys / adds / sells entered in the UI).

- ``POST /trades`` — record one leg (BUY / ADD / SELL).
- ``GET /trades`` — list legs (newest first).
- ``GET /trades/stats`` — expectancy board stats.
- ``DELETE /trades/{id}`` — delete a leg (correction).

Field names mirror @karios/shared userTrades schemas.
"""

from __future__ import annotations

import re
from datetime import datetime
from logging import getLogger

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel

from data_sync_service.db.user_trades import (
    LEG_S3,
    LEGS,
    SIDES,
    STRATEGY_MODE_LEGACY,
    STRATEGY_MODES,
    delete_trade,
    ensure_tables,
    insert_trade,
    latest_buy_leg,
    list_trades,
    update_trade,
)
from data_sync_service.service.user_trades_stats import compute_trade_stats

logger = getLogger(__name__)

router = APIRouter()

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SYMBOL_RE = re.compile(r"^(CN|HK|ETF):[0-9A-Z.\-]{1,16}$")


class TradeLegRequest(BaseModel):
    symbol: str
    side: str
    price: float
    positionPct: float
    tradeDate: str | None = None
    costBasis: float | None = None
    entryDate: str | None = None
    source: str | None = None
    market: str | None = None
    note: str | None = None
    leg: str | None = None
    strategyMode: str | None = None


def _validate_leg(req: TradeLegRequest) -> dict:
    if not _SYMBOL_RE.match(req.symbol):
        raise HTTPException(status_code=400, detail=f"invalid symbol: {req.symbol}")
    if req.side not in SIDES:
        raise HTTPException(status_code=400, detail=f"invalid side: {req.side}")
    if req.price <= 0 or not req.positionPct > 0:
        raise HTTPException(status_code=400, detail="price and positionPct must be positive")
    # 2026-08-09: SELL no longer REQUIRES costBasis/entryDate — holdings that
    # lack them (manually added rows) must still be recordable; pnl_pct /
    # holding_days are just left null and the expectancy board counts only
    # rows with pnl. The watchlist keeps being the source of truth for cost.
    if req.tradeDate is not None and not _DATE_RE.match(req.tradeDate):
        raise HTTPException(status_code=400, detail="tradeDate must be YYYY-MM-DD")
    if req.leg is not None and req.leg not in LEGS:
        raise HTTPException(status_code=400, detail=f"invalid leg: {req.leg}")
    if req.strategyMode is not None and req.strategyMode not in STRATEGY_MODES:
        raise HTTPException(status_code=400, detail=f"invalid strategyMode: {req.strategyMode}")
    return {}


@router.post("/trades")
def record_trade(req: TradeLegRequest) -> dict:
    _validate_leg(req)
    try:
        ensure_tables()
        if req.tradeDate is None:
            from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

            trade_date = shanghai_today_iso()
        else:
            trade_date = req.tradeDate
        pnl_pct = None
        holding_days = None
        if req.side == "SELL" and req.costBasis is not None and req.costBasis > 0:
            pnl_pct = round((req.price - req.costBasis) / req.costBasis * 100, 3)
        if req.side == "SELL" and req.entryDate is not None and _DATE_RE.match(req.entryDate):
            d0 = datetime.strptime(req.entryDate, "%Y-%m-%d").date()
            d1 = datetime.strptime(trade_date, "%Y-%m-%d").date()
            holding_days = max(0, (d1 - d0).days)
        row = insert_trade(
            symbol=req.symbol,
            side=req.side,
            trade_date=trade_date,
            price=req.price,
            position_pct=req.positionPct,
            cost_basis=req.costBasis,
            entry_date=req.entryDate,
            pnl_pct=pnl_pct,
            holding_days=holding_days,
            source=req.source,
            market=req.market or "CN",
            note=req.note,
            alpha_snapshot=_alpha_snapshot_for(req.symbol, trade_date),
            leg=req.leg or (
                latest_buy_leg(req.symbol, strategy_mode=req.strategyMode)
                if req.side in ("SELL", "ADD")
                else LEG_S3
            ),
            strategy_mode=req.strategyMode or STRATEGY_MODE_LEGACY,
        )
        return {"ok": True, "trade": row}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _alpha_snapshot_for(symbol: str, trade_date: str) -> dict | None:
    """As-of alpha snapshot (best-effort — capture never blocks the trade)."""
    try:
        from data_sync_service.service.user_trades_alpha import alpha_snapshot_for

        return alpha_snapshot_for(symbol, trade_date)
    except Exception as exc:  # noqa: BLE001
        logger.warning("user trade alpha snapshot failed: %s", exc)
        return None


@router.get("/trades")
def get_trades(
    limit: int = Query(default=50, ge=1, le=500),
    symbol: str | None = Query(default=None),
    leg: str | None = Query(default=None),
    strategyMode: str | None = Query(default=None),
) -> dict:
    if leg is not None and leg not in LEGS:
        raise HTTPException(status_code=400, detail=f"invalid leg: {leg}")
    if strategyMode is not None and strategyMode not in STRATEGY_MODES:
        raise HTTPException(status_code=400, detail=f"invalid strategyMode: {strategyMode}")
    try:
        ensure_tables()
        rows = list_trades(limit=limit, symbol=symbol, leg=leg, strategy_mode=strategyMode)
        return {"ok": True, "trades": rows, "count": len(rows)}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/trades/stats")
def get_trade_stats() -> dict:
    try:
        ensure_tables()
        stats = compute_trade_stats()
        return {"ok": True, "stats": stats}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/trades/{trade_id}")
def remove_trade(trade_id: str) -> dict:
    try:
        ensure_tables()
        removed = delete_trade(trade_id)
        if not removed:
            raise HTTPException(status_code=404, detail=f"trade not found: {trade_id}")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


class TradePatchRequest(BaseModel):
    leg: str | None = None
    strategyMode: str | None = None
    positionPct: float | None = None
    note: str | None = None


@router.patch("/trades/{trade_id}")
def patch_trade(trade_id: str, req: TradePatchRequest) -> dict:
    """Correct a journal leg: leg / strategyMode / positionPct / note only."""
    if req.leg is not None and req.leg not in LEGS:
        raise HTTPException(status_code=400, detail=f"invalid leg: {req.leg}")
    if req.strategyMode is not None and req.strategyMode not in STRATEGY_MODES:
        raise HTTPException(status_code=400, detail=f"invalid strategyMode: {req.strategyMode}")
    if req.positionPct is not None and not req.positionPct > 0:
        raise HTTPException(status_code=400, detail="positionPct must be positive")
    if req.leg is None and req.strategyMode is None and req.positionPct is None and req.note is None:
        raise HTTPException(status_code=400, detail="nothing to update")
    try:
        ensure_tables()
        row = update_trade(
            trade_id,
            leg=req.leg,
            strategy_mode=req.strategyMode,
            position_pct=req.positionPct,
            note=req.note,
        )
        if row is None:
            raise HTTPException(status_code=404, detail=f"trade not found: {trade_id}")
        return {"ok": True, "trade": row}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
