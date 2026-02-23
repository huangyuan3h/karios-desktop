"""Sim trade API: trading days and daily bars for the stock trade simulator page."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query  # type: ignore[import-not-found]

from data_sync_service.db.daily import fetch_daily_for_codes
from data_sync_service.db.trade_calendar import get_open_dates
from data_sync_service.service.market_quotes import symbol_to_ts_code

router = APIRouter(prefix="/simtrade", tags=["simtrade"])


@router.get("/trading-days")
def get_trading_days(
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
) -> list[str]:
    """Return list of trading days in [start, end] (SSE calendar)."""
    start_d = _parse_date(start)
    end_d = _parse_date(end)
    if not start_d or not end_d or start_d > end_d:
        return []
    # Use SSE for A-share calendar
    dates = get_open_dates("SSE", start_d, end_d)
    return [d.isoformat() for d in dates]


def _parse_date(s: str) -> date | None:
    if not s or len(s) < 10:
        return None
    s = s.strip()[:10]
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            return date(int(s[:4]), int(s[5:7]), int(s[8:10]))
        except ValueError:
            pass
    return None


@router.get("/daily-bars")
def get_daily_bars(
    symbols: str = Query(..., description="Comma-separated symbols, e.g. CN:000001,CN:600000"),
    start: str = Query(..., description="Start date YYYY-MM-DD"),
    end: str = Query(..., description="End date YYYY-MM-DD"),
) -> dict:
    """
    Return daily bars for the given symbols in [start, end].
    Each bar includes avg_price = (open+high+low+close)/4.
    Response keys are symbols as given (e.g. CN:000001).
    """
    start_d = _parse_date(start)
    end_d = _parse_date(end)
    if not start_d or not end_d or start_d > end_d:
        return {"bars": {}}
    sym_list = [x.strip() for x in symbols.split(",") if x.strip()]
    ts_codes: list[str] = []
    symbol_to_ts: dict[str, str] = {}
    for sym in sym_list:
        code = symbol_to_ts_code(sym)
        if code:
            ts_codes.append(code)
            symbol_to_ts[sym] = code
    if not ts_codes:
        return {"bars": {}}
    start_s = start_d.isoformat()
    end_s = end_d.isoformat()
    rows = fetch_daily_for_codes(ts_codes, start_s, end_s)
    # ts_code -> list of bars (by date)
    by_code: dict[str, list[dict]] = {}
    for r in rows:
        tc = (r.get("ts_code") or "").strip()
        if not tc:
            continue
        o = _numeric(r.get("open"))
        h = _numeric(r.get("high"))
        lo = _numeric(r.get("low"))
        c = _numeric(r.get("close"))
        avg = (o + h + lo + c) / 4.0 if o is not None and h is not None and lo is not None and c is not None else None
        bar = {
            "date": r.get("trade_date"),
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "vol": _numeric(r.get("vol")),
            "amount": _numeric(r.get("amount")),
            "avg_price": avg,
        }
        by_code.setdefault(tc, []).append(bar)
    # ts_code -> symbol (first symbol that maps to this ts_code)
    ts_to_symbol: dict[str, str] = {}
    for sym, tc in symbol_to_ts.items():
        if tc not in ts_to_symbol:
            ts_to_symbol[tc] = sym
    out: dict[str, list[dict]] = {}
    for tc, bars in by_code.items():
        mapped_sym = ts_to_symbol.get(tc)
        if mapped_sym is not None:
            bars_sorted = sorted(bars, key=lambda b: b.get("date") or "")
            out[mapped_sym] = bars_sorted
    return {"bars": out}


def _numeric(val: object) -> float | None:
    if val is None:
        return None
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
