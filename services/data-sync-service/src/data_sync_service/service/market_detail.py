from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Tuple
from zoneinfo import ZoneInfo

from fastapi import HTTPException  # type: ignore[import-not-found]

from data_sync_service.db.market_detail import (
    list_chips_cached,
    list_fund_flow_cached,
    upsert_chips,
    upsert_fund_flow,
)
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_cn_date_str() -> str:
    try:
        dt = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    except Exception:
        dt = datetime.now(tz=UTC)
    return dt.date().isoformat()


def _parse_symbol_cn_only(symbol: str) -> Tuple[str, str, str] | None:
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
    Keep optional; detail endpoints can still work without stock_basic synced.
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


def fetch_cn_a_chip_summary(ticker: str, *, days: int = 60) -> list[dict[str, str]]:
    """
    CN A-share chip distribution (cost distribution) summary time series.

    Returns rows with keys:
      date, profitRatio, avgCost,
      cost90Low/cost90High/cost90Conc,
      cost70Low/cost70High/cost70Conc
    """
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for CN chip distribution.\n"
            "Install in data-sync-service: cd services/data-sync-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e
    if not hasattr(ak, "stock_cyq_em"):
        raise RuntimeError("AkShare missing stock_cyq_em. Please upgrade AkShare.")
    try:
        df = ak.stock_cyq_em(symbol=ticker)
    except TypeError:
        df = ak.stock_cyq_em(ticker)
    rows = list(df.to_dict("records")) if hasattr(df, "to_dict") else []
    out: list[dict[str, str]] = []
    for r in rows:
        d = str(r.get("日期") or r.get("date") or "").strip()
        if not d:
            continue
        out.append(
            {
                "date": d,
                "profitRatio": str(r.get("获利比例") or r.get("profitRatio") or ""),
                "avgCost": str(r.get("平均成本") or r.get("avgCost") or ""),
                "cost90Low": str(r.get("90成本-低") or r.get("cost90Low") or ""),
                "cost90High": str(r.get("90成本-高") or r.get("cost90High") or ""),
                "cost90Conc": str(r.get("90集中度") or r.get("cost90Conc") or ""),
                "cost70Low": str(r.get("70成本-低") or r.get("cost70Low") or ""),
                "cost70High": str(r.get("70成本-高") or r.get("cost70High") or ""),
                "cost70Conc": str(r.get("70集中度") or r.get("cost70Conc") or ""),
            }
        )
    return out[-max(1, int(days)) :]


def fetch_cn_a_fund_flow(ticker: str, *, days: int = 60) -> list[dict[str, str]]:
    """
    CN A-share individual stock fund flow breakdown (Eastmoney via AkShare).

    Returns rows with keys:
      date, close, changePct,
      mainNetAmount/mainNetRatio,
      superNetAmount/superNetRatio,
      largeNetAmount/largeNetRatio,
      mediumNetAmount/mediumNetRatio,
      smallNetAmount/smallNetRatio
    """
    try:
        import akshare as ak  # type: ignore[import-not-found]
    except Exception as e:
        raise RuntimeError(
            "AkShare is required for CN fund flow.\n"
            "Install in data-sync-service: cd services/data-sync-service && uv add akshare\n"
            f"Original error: {e}"
        ) from e
    if not hasattr(ak, "stock_individual_fund_flow"):
        raise RuntimeError("AkShare missing stock_individual_fund_flow. Please upgrade AkShare.")
    market = "sh" if ticker.startswith("6") else "sz"
    try:
        df = ak.stock_individual_fund_flow(stock=ticker, market=market)
    except TypeError:
        df = ak.stock_individual_fund_flow(ticker, market=market)
    rows = list(df.to_dict("records")) if hasattr(df, "to_dict") else []
    out: list[dict[str, str]] = []
    for r in rows:
        d = str(r.get("日期") or r.get("date") or "").strip()
        if not d:
            continue
        out.append(
            {
                "date": d,
                "close": str(r.get("收盘价") or r.get("close") or ""),
                "changePct": str(r.get("涨跌幅") or r.get("changePct") or ""),
                "mainNetAmount": str(r.get("主力净流入-净额") or r.get("mainNetAmount") or ""),
                "mainNetRatio": str(r.get("主力净流入-净占比") or r.get("mainNetRatio") or ""),
                "superNetAmount": str(r.get("超大单净流入-净额") or r.get("superNetAmount") or ""),
                "superNetRatio": str(r.get("超大单净流入-净占比") or r.get("superNetRatio") or ""),
                "largeNetAmount": str(r.get("大单净流入-净额") or r.get("largeNetAmount") or ""),
                "largeNetRatio": str(r.get("大单净流入-净占比") or r.get("largeNetRatio") or ""),
                "mediumNetAmount": str(r.get("中单净流入-净额") or r.get("mediumNetAmount") or ""),
                "mediumNetRatio": str(r.get("中单净流入-净占比") or r.get("mediumNetRatio") or ""),
                "smallNetAmount": str(r.get("小单净流入-净额") or r.get("smallNetAmount") or ""),
                "smallNetRatio": str(r.get("小单净流入-净占比") or r.get("smallNetRatio") or ""),
            }
        )
    return out[-max(1, int(days)) :]


def get_market_chips(*, symbol: str, days: int = 60, force: bool = False) -> dict[str, Any]:
    days2 = max(10, min(int(days), 200))
    parsed = _parse_symbol_cn_only(symbol)
    if not parsed:
        raise HTTPException(status_code=400, detail="Invalid symbol format.")
    market, ticker, ts_code = parsed
    if market != "CN":
        raise HTTPException(
            status_code=400,
            detail="Chip distribution is only supported for CN A-shares (v0).",
        )
    name = _lookup_name(ts_code) or ticker
    currency = "CNY"

    cached = list_chips_cached(symbol, limit=days2)
    cached_last = str(cached[0][0]) if cached else ""
    expected_last = _today_cn_date_str()
    cache_stale = bool(cached_last and cached_last < expected_last)

    if (not force) and (not cache_stale) and len(cached) >= min(days2, 30):
        items = [raw for _d, raw in reversed(cached)]
        return {
            "symbol": symbol,
            "market": market,
            "ticker": ticker,
            "name": name,
            "currency": currency,
            "items": items,
        }

    ts = _now_iso()
    try:
        items2 = fetch_cn_a_chip_summary(ticker, days=days2)
    except Exception:
        # Chips are best-effort enrichment. Degrade gracefully to cached/empty.
        if cached:
            items = [raw for _d, raw in reversed(cached)]
            return {
                "symbol": symbol,
                "market": market,
                "ticker": ticker,
                "name": name,
                "currency": currency,
                "items": items,
            }
        return {
            "symbol": symbol,
            "market": market,
            "ticker": ticker,
            "name": name,
            "currency": currency,
            "items": [],
        }

    upsert_chips(symbol, items2, updated_at=ts)
    return {
        "symbol": symbol,
        "market": market,
        "ticker": ticker,
        "name": name,
        "currency": currency,
        "items": items2,
    }


def get_market_fund_flow(*, symbol: str, days: int = 60, force: bool = False) -> dict[str, Any]:
    days2 = max(10, min(int(days), 200))
    parsed = _parse_symbol_cn_only(symbol)
    if not parsed:
        raise HTTPException(status_code=400, detail="Invalid symbol format.")
    market, ticker, ts_code = parsed
    if market != "CN":
        raise HTTPException(
            status_code=400,
            detail="Fund flow distribution is only supported for CN A-shares (v0).",
        )
    name = _lookup_name(ts_code) or ticker
    currency = "CNY"

    cached = list_fund_flow_cached(symbol, limit=days2)
    cached_last = str(cached[0][0]) if cached else ""
    expected_last = _today_cn_date_str()
    cache_stale = bool(cached_last and cached_last < expected_last)

    if (not force) and (not cache_stale) and len(cached) >= min(days2, 30):
        items = [raw for _d, raw in reversed(cached)]
        return {
            "symbol": symbol,
            "market": market,
            "ticker": ticker,
            "name": name,
            "currency": currency,
            "items": items,
        }

    ts = _now_iso()
    try:
        items2 = fetch_cn_a_fund_flow(ticker, days=days2)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fund flow fetch failed for {ticker}: {e}") from e

    upsert_fund_flow(symbol, items2, updated_at=ts)
    return {
        "symbol": symbol,
        "market": market,
        "ticker": ticker,
        "name": name,
        "currency": currency,
        "items": items2,
    }

