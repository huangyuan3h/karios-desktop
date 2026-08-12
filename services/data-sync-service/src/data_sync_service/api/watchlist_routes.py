from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel

# Field names must match @karios/shared WatchlistRegistryItemSchema
from data_sync_service.db.watchlist_automation import list_registry, upsert_registry
from data_sync_service.service.watchlist_automation import (
    ack_automation_run,
    filter_pullback_window,
    get_automation_latest,
    get_automation_pending,
    get_automation_run,
    get_automation_runs,
    list_fallback_universe_symbols,
    run_intraday_scores,
    run_watchlist_automation,
)

router = APIRouter()


class WatchlistRegistryItem(BaseModel):
    symbol: str
    name: str | None = None
    addedAt: str | None = None
    source: str | None = None
    color: str | None = None
    positionPct: float | None = None
    costPrice: float | None = None
    maxPrice: float | None = None
    entryDate: str | None = None


class WatchlistRegistryRequest(BaseModel):
    items: list[WatchlistRegistryItem] = []


class WatchlistAckRequest(BaseModel):
    screenerAdded: int | None = None
    funnel: dict[str, Any] | None = None


class PullbackFilterRequest(BaseModel):
    symbols: list[str] = []
    asOf: str | None = None


@router.get("/watchlist/rs-ranks")
def watchlist_rs_ranks(
    symbols: str = Query(..., description="Comma-separated symbols (CN:/HK:)."),
) -> dict:
    """Whole-market 20-day RS percentile per symbol (S-2: top-50% filter).

    Percentile 0-1 (strongest = 1.0); ranking pool = ALL stocks with a bar
    on the latest trade date. Cached per as-of date.
    """
    from data_sync_service.service.watchlist_automation import compute_rs_ranks

    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    if not syms:
        return {"ok": True, "asOfDate": None, "ranks": {}}
    ranks = compute_rs_ranks(syms)
    as_of = ranks.pop("_asOf", None)
    return {"ok": True, "asOfDate": as_of, "ranks": ranks}


@router.get("/watchlist/registry")
def get_watchlist_registry() -> dict:
    try:
        items = list_registry()
        return {"ok": True, "items": items, "count": len(items)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/registry")
def watchlist_registry(req: WatchlistRegistryRequest) -> dict:
    try:
        items = [x.model_dump(exclude_none=False) for x in req.items]
        items = _backfill_names(items)
        count = upsert_registry(items)
        return {"ok": True, "count": count}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _backfill_names(items: list[dict]) -> list[dict]:
    """Fill in name from stock_basic when client-side resolve missed it (HK / ETF / CN)."""
    if not items:
        return items
    need: dict[str, str] = {}
    for it in items:
        sym = str(it.get("symbol") or "").strip()
        if not sym or it.get("name"):
            continue
        need[sym] = _to_ts_code(sym)
    if not need:
        return items
    ts_codes = list({v for v in need.values() if v})
    if not ts_codes:
        return items
    try:
        from data_sync_service.db import get_connection
        from data_sync_service.db.stock_basic import ensure_table as ensure_sb

        ensure_sb()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, name FROM stock_basic WHERE ts_code = ANY(%s)",
                    (ts_codes,),
                )
                rows = cur.fetchall()
        by_ts = {str(r[0]): str(r[1] or "") for r in rows if r and r[0]}
    except Exception:
        return items
    if not by_ts:
        return items
    out: list[dict] = []
    for it in items:
        sym = str(it.get("symbol") or "").strip()
        if sym in need and not it.get("name"):
            tc = need.get(sym)
            nm = by_ts.get(tc) if tc else ""
            if nm:
                it = {**it, "name": nm}
        out.append(it)
    return out


def _to_ts_code(sym: str) -> str:
    from data_sync_service.service.market_quotes import normalize_market_symbol

    s = normalize_market_symbol(sym)
    if s.startswith("HK:"):
        ticker = s.split(":", 1)[1].strip()
        if 1 <= len(ticker) <= 5 and ticker.isdigit():
            return f"{ticker.zfill(5)}.HK"
    elif s.startswith("CN:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) == 6 and ticker.isdigit():
            suffix = "SH" if ticker.startswith("6") else "SZ"
            return f"{ticker}.{suffix}"
    elif s.startswith("ETF:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) == 6 and ticker.isdigit():
            suffix = "SH" if ticker[0] in ("5", "6", "9") else "SZ"
            return f"{ticker}.{suffix}"
    return ""


@router.post("/watchlist/registry/backfill-names")
def backfill_registry_names() -> dict:
    """One-shot backfill: fill null `name` from stock_basic for every registered symbol.

    Useful after the resolver was missing for HK / ETF tickers or after data loss.
    """
    try:
        items = list_registry()
        before_null = sum(1 for x in items if not x.get("name"))
        filled = _backfill_names(items)
        if filled != items:
            upsert_registry(filled)
        after_null = sum(1 for x in filled if not x.get("name"))
        return {
            "ok": True,
            "total": len(items),
            "filledBefore": before_null,
            "filledAfter": after_null,
            "updatedCount": before_null - after_null,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/pending")
def watchlist_automation_pending(tradeDate: str | None = Query(None)) -> dict:
    try:
        pending = get_automation_pending(tradeDate)
        if not pending:
            return {"pending": False}
        return {"pending": True, **pending}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/latest")
def watchlist_automation_latest() -> dict:
    try:
        latest = get_automation_latest()
        if not latest:
            return {"found": False}
        return {"found": True, **latest}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/runs")
def watchlist_automation_runs(limit: int = Query(10, ge=1, le=30)) -> dict:
    """TIP-002 N-day funnel history: one acknowledged run per trade_date,
    newest first, including each run's meta.funnel counts.

    NOTE: must stay registered before ``/watchlist/automation/{run_id}`` so
    FastAPI matches the literal ``runs`` path instead of treating it as a
    run id.
    """
    try:
        from datetime import date

        runs = get_automation_runs(limit=limit)
        return {"ok": True, "runs": runs, "asOfDate": date.today().isoformat()}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/run")
def watchlist_automation_run(force: bool = Query(False)) -> dict:
    try:
        return run_watchlist_automation(trigger="manual", force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/intraday-scores")
def watchlist_automation_intraday_scores(force: bool = Query(False)) -> dict:
    """2026-08-11: realtime (trading-hours) score refresh for the CN + HK
    universes — makes the intraday S-3 candidate surface live. Runs on-demand
    here; the scheduler fires it at 10:30 / 14:00 Asia/Shanghai on weekdays.
    """
    try:
        return run_intraday_scores(trigger="manual", force=force)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/fallback-universe")
def watchlist_fallback_universe(
    maxTotal: int = Query(80, ge=1, le=200),
) -> dict:
    """TIP-003: empty-window fallback candidates (5D Top5 non-defense → EM LIKE)."""
    try:
        return list_fallback_universe_symbols(max_total=maxTotal)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/pullback-filter")
def watchlist_automation_pullback_filter(req: PullbackFilterRequest) -> dict:
    """52W pullback gate using DB K-lines (replaces unreliable TV High.Interval52Week)."""
    try:
        return filter_pullback_window(req.symbols or [], as_of=req.asOf)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/watchlist/automation/{run_id}/ack")
def watchlist_automation_ack(run_id: str, req: WatchlistAckRequest | None = None) -> dict:
    try:
        screener_added = req.screenerAdded if req else None
        funnel = req.funnel if req else None
        row = ack_automation_run(run_id, screener_added=screener_added, funnel=funnel)
        if not row:
            raise HTTPException(status_code=404, detail="run not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/watchlist/automation/{run_id}")
def watchlist_automation_get(run_id: str) -> dict:
    try:
        row = get_automation_run(run_id)
        if not row:
            raise HTTPException(status_code=404, detail="run not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
