"""Market bars adapter for StockPage compatibility."""

from __future__ import annotations

from typing import Any

from data_sync_service.db.daily import fetch_last_bars
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic
from data_sync_service.service.daily import sync_daily_for_ts_code
from data_sync_service.service.etf_daily import sync_etf_daily_for_ts_code
from data_sync_service.service.hk_daily import sync_hk_daily_for_ts_code
from data_sync_service.service.hk_daily_yf import sync_hk_daily_for_ts_code_yf


def _parse_symbol(symbol: str) -> tuple[str, str, str] | None:
    """
    Parse UI symbol like 'CN:000001', 'HK:00700', 'ETF:510300' into
    (market, ticker, ts_code). Supports CN A-shares, HK tickers, and ETFs.
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
    if s.startswith("HK:"):
        ticker = s.split(":", 1)[1].strip()
        if 1 <= len(ticker) <= 5 and ticker.isdigit():
            padded = ticker.zfill(5)
            return "HK", padded, f"{padded}.HK"
        return None
    if s.startswith("ETF:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) == 6 and ticker.isdigit():
            suffix = "SH" if ticker[0] in ("5", "6", "9") else "SZ"
            return "ETF", ticker, f"{ticker}.{suffix}"
        return None
    # Allow direct ts_code input
    if len(s) == 9 and s[6] == "." and s[:6].isdigit() and s[7:].isalpha():
        ticker = s[:6]
        return "CN", ticker, s.upper()
    if len(s) == 8 and s[5] == "." and s[:5].isdigit() and s[6:].upper() == "HK":
        ticker = s[:5]
        return "HK", ticker, s.upper()
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


def get_market_bars(symbol: str, days: int = 60, *, force: bool = False) -> dict[str, Any]:
    """
    Return a response compatible with quant-service `MarketBarsResponse`.
    When force=True, best-effort incremental tushare sync runs before reading DB.
    """
    parsed = _parse_symbol(symbol)
    if force and parsed:
        market, _ticker, ts_code = parsed
        if market == "CN":
            sync_daily_for_ts_code(ts_code)
        elif market == "HK":
            # Try yfinance first (no tushare 1-call/hour rate cap); fall back to tushare if it fails.
            yf_result = sync_hk_daily_for_ts_code_yf(ts_code)
            if not yf_result.get("ok") or int(yf_result.get("updated") or 0) == 0:
                ts_result = sync_hk_daily_for_ts_code(ts_code)
                if ts_result.get("ok") and int(ts_result.get("updated") or 0) > 0:
                    pass
        elif market == "ETF":
            sync_etf_daily_for_ts_code(ts_code)
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
    if market == "CN":
        currency = "CNY"
    elif market == "HK":
        currency = "HKD"
    elif market == "ETF":
        # Most ETFs are CNY-denominated; QDII cross-border ones trade in CNY on A-share market too.
        currency = "CNY"
    else:
        currency = ""
    bars = fetch_last_bars(ts_code=ts_code, days=days)
    return {
        "symbol": symbol,
        "market": market,
        "ticker": ticker,
        "name": name,
        "currency": currency,
        "bars": bars,
    }

