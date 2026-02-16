"""Market bars adapter for StockPage compatibility."""

from __future__ import annotations

from typing import Any, Tuple

from data_sync_service.db.daily import fetch_last_bars
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic


def _parse_symbol(symbol: str) -> Tuple[str, str, str] | None:
    """
    Parse UI symbol like 'CN:000001' into (market, ticker, ts_code).
    Only CN A-shares are supported.
    """
    s = (symbol or "").strip()
    if not s:
        return None
    if s.startswith("CN:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) == 6 and ticker.isdigit():
            suffix = "SH" if ticker.startswith("6") else "SZ"
            return "CN", ticker, f"{ticker}.{suffix}"
        return None
    # Allow direct ts_code input
    if len(s) == 9 and s[6] == "." and s[:6].isdigit() and s[7:].isalpha():
        ticker = s[:6]
        return "CN", ticker, s.upper()
    return None


def _lookup_name(ts_code: str) -> str | None:
    """
    Best-effort lookup from stock_basic table.
    We keep this optional; bars should still work even if stock_basic isn't synced yet.
    """
    try:
        from data_sync_service.db import get_connection

        ensure_stock_basic()
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM stock_basic WHERE ts_code = %s", (ts_code,))
                row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        return None
    return None


def get_market_bars(symbol: str, days: int = 60) -> dict[str, Any]:
    """
    Return a response compatible with quant-service `MarketBarsResponse`.
    """
    parsed = _parse_symbol(symbol)
    if not parsed:
        return {
            "symbol": symbol,
            "market": "",
            "ticker": "",
            "name": "",
            "currency": "",
            "bars": [],
        }
    market, ticker, ts_code = parsed
    name = _lookup_name(ts_code) or ticker
    currency = "CNY" if market == "CN" else ""
    bars = fetch_last_bars(ts_code=ts_code, days=days)
    return {
        "symbol": symbol,
        "market": market,
        "ticker": ticker,
        "name": name,
        "currency": currency,
        "bars": bars,
    }

