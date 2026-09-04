from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from data_sync_service.service.commodity_signals import all_signals
from data_sync_service.service.multi_asset_sleeve import build_multi_asset_sleeve, build_pulse_hints
from data_sync_service.service.portfolio_health import _health_block

router = APIRouter(prefix="/commodities", tags=["commodities"])

@router.get("/signals")
def get_signals():
    return all_signals()


@router.get("/sleeve")
def get_sleeve(day: str | None = None):
    from data_sync_service.db.watchlist_automation import list_registry

    d = day or date.today().isoformat()
    cn_block = _health_block(market="CN", day=d)
    # Use real watchlist holdings so idlePct/message reflects today's manual buys (e.g. 513350 bought 2026-08-24)
    raw_holdings = [
        {"symbol": str(r.get("symbol") or "").upper(), "positionPct": (r.get("payload") or {}).get("positionPct", r.get("positionPct")), "ts_code": r.get("ts_code")}
        for r in list_registry()
        if str(r.get("symbol") or "").upper().startswith(("CN:", "ETF:"))
    ]
    return build_multi_asset_sleeve(day=d, cn_block=cn_block, holdings_override=raw_holdings)


@router.get("/sleeve/paper")
def get_sleeve_paper(day: str | None = None):
    from data_sync_service.db.paper_trading import list_paper_trades

    d = day or date.today().isoformat()
    cn_block = _health_block(market="CN", day=d)
    open_trades = list_paper_trades(status="open")
    holdings = [
        {"symbol": t.get("symbol"), "ts_code": t.get("ts_code"), "sleeve_pct": t.get("sleeve_pct") or 0}
        for t in open_trades
        if str(t.get("symbol") or "").upper().startswith(("CN:", "ETF:"))
    ]
    return build_multi_asset_sleeve(day=d, cn_block=cn_block, holdings_override=holdings)


@router.get("/pulse")
def get_pulse(day: str | None = None):
    d = day or date.today().isoformat()
    return {"tradeDate": d, "hints": build_pulse_hints(day=d)}


class SleeveExecBody(BaseModel):
    tradeDate: str | None = None
    pickKey: str
    status: str
    symbol: str | None = None
    premiumBps: float | None = None
    signalPrice: float | None = None
    fillPrice: float | None = None
    note: str | None = None
    meta: dict[str, Any] | None = None


@router.get("/sleeve/execution-log")
def get_sleeve_execution_log(limit: int = 30):
    from data_sync_service.db.sleeve_execution_log import list_events

    return {"ok": True, "items": list_events(limit=limit)}


@router.post("/sleeve/execution-log")
def post_sleeve_execution_log(body: SleeveExecBody):
    from data_sync_service.db.sleeve_execution_log import STATUSES, insert_event

    if body.status not in STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {sorted(STATUSES)}")
    row = insert_event(
        trade_date=body.tradeDate or date.today().isoformat(),
        pick_key=body.pickKey,
        status=body.status,
        symbol=body.symbol,
        premium_bps=body.premiumBps,
        signal_price=body.signalPrice,
        fill_price=body.fillPrice,
        note=body.note,
        meta=body.meta,
    )
    return {"ok": True, "item": row}
