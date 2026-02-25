"""Batch fetch market quotes (price, change%) for multiple stocks from daily table or realtime API."""

from __future__ import annotations

from typing import Any

from data_sync_service.db.daily import ensure_table as ensure_daily_table
from data_sync_service.db import get_connection
from data_sync_service.service.realtime_quote import fetch_realtime_quotes


def get_market_quotes_batch(
    ts_codes: list[str],
    use_realtime: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Fetch price and change% for multiple ts_codes.
    
    Args:
        ts_codes: List of ts_code (e.g., ["000001.SZ", "600000.SH"])
        use_realtime: If True, use realtime quote API; otherwise use latest daily close.
    
    Returns:
        Mapping: ts_code -> {"price": str, "changePct": str, "volume": str, "turnover": str}
        Missing values are None.
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return {}

    if use_realtime:
        # Use realtime quote API
        result = fetch_realtime_quotes(codes)
        if not result.get("ok"):
            return {}
        items = result.get("items", [])
        out: dict[str, dict[str, Any]] = {}
        for item in items:
            ts_code = item.get("ts_code")
            if not ts_code:
                continue
            out[ts_code] = {
                "price": item.get("price"),
                "changePct": item.get("pct_chg"),  # pct_chg is already percentage
                "volume": item.get("volume"),
                "turnover": item.get("amount"),
            }
        return out

    # Use latest daily close from DB
    ensure_daily_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get latest trade_date per ts_code using window function
            cur.execute(
                """
                SELECT ts_code, trade_date, close, pct_chg, vol, amount
                FROM (
                    SELECT ts_code, trade_date, close, pct_chg, vol, amount,
                           row_number() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                    FROM daily
                    WHERE ts_code = ANY(%s)
                ) t
                WHERE rn = 1
                """,
                (codes,),
            )
            rows = cur.fetchall()

    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        ts_code = str(r[0])
        close_val = r[2]
        pct_chg_val = r[3]
        vol_val = r[4]
        amount_val = r[5]

        out[ts_code] = {
            "price": str(close_val) if close_val is not None else None,
            "changePct": str(pct_chg_val) if pct_chg_val is not None else None,
            "volume": str(vol_val) if vol_val is not None else None,
            "turnover": str(amount_val) if amount_val is not None else None,
        }

    return out


def symbol_to_ts_code(symbol: str) -> str | None:
    """
    Convert symbol format (CN:000001) to ts_code format (000001.SZ).
    Returns None if format is invalid.
    """
    if not symbol or ":" not in symbol:
        return None
    parts = symbol.split(":", 1)
    if len(parts) != 2:
        return None
    market, ticker = parts[0].strip().upper(), parts[1].strip()
    if market == "CN" and len(ticker) == 6 and ticker.isdigit():
        suffix = "SH" if ticker.startswith("6") else "SZ"
        return f"{ticker}.{suffix}"
    # HK support can be added later
    return None


def ts_code_to_symbol(ts_code: str, market: str = "CN") -> str:
    """
    Convert ts_code format (000001.SZ) to symbol format (CN:000001).
    """
    if "." in ts_code:
        ticker = ts_code.split(".")[0]
        return f"{market}:{ticker}"
    return f"{market}:{ts_code}"
