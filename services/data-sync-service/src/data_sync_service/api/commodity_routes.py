from fastapi import APIRouter

from data_sync_service.service.commodity_signals import all_signals
from data_sync_service.service.multi_asset_sleeve import build_multi_asset_sleeve
from data_sync_service.service.portfolio_health import _health_block

router = APIRouter(prefix="/commodities", tags=["commodities"])

@router.get("/signals")
def get_signals():
    return all_signals()


@router.get("/sleeve")
def get_sleeve(day: str | None = None):
    from datetime import date

    d = day or date.today().isoformat()
    cn_block = _health_block(market="CN", day=d)
    return build_multi_asset_sleeve(day=d, cn_block=cn_block)


@router.get("/sleeve/paper")
def get_sleeve_paper(day: str | None = None):
    from datetime import date

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
