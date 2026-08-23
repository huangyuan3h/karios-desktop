"""Factor signals — morphology / microstructure signals (independent of S-3)."""
from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Query

from data_sync_service.db.factor_signals import fetch_by_date
from data_sync_service.service.factor_signals_service import scan_strong_scoop_exhaustion

router = APIRouter(prefix="/factors", tags=["factors"])


@router.get("/signals")
def get_signals(trade_date: str | None = Query(None, description="YYYY-MM-DD, defaults to latest")):
    if trade_date is None:
        # latest signals (most recent trade_date with data)
        from data_sync_service.db import get_connection
        from data_sync_service.db.factor_signals import ensure_table
        ensure_table()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT max(trade_date) FROM factor_signals")
                row = cur.fetchone()
                trade_date = str(row[0]) if row and row[0] else str(date.today().isoformat())
    rows = fetch_by_date(trade_date)
    # normalize dates to iso
    for r in rows:
        if r.get("trade_date"):
            r["trade_date"] = str(r["trade_date"])[:10]
    return {"asOfDate": trade_date, "signals": rows}


@router.post("/sync")
def sync_signals(trade_date: str | None = None):
    target = trade_date or date.today().isoformat()
    # if weekend, use last trading day (simple: try scan, if 0 then previous day)
    n = scan_strong_scoop_exhaustion(target)
    if n == 0:
        # try previous trading day
        prev = (date.fromisoformat(target) - timedelta(days=3)).isoformat()
        # fallback: scan previous 5 days
        from data_sync_service.db.trade_calendar import get_open_dates
        opens = get_open_dates("SSE", date.fromisoformat(prev), date.fromisoformat(target))
        if opens:
            target = opens[-1].isoformat()
            n = scan_strong_scoop_exhaustion(target)
    return {"trade_date": target, "signals": n}
