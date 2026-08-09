"""When macro_daily is empty and realtime_quote has no offshore ticks, pull recent daily bars from Tushare (read-only)."""

from __future__ import annotations

import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.config import get_settings
from data_sync_service.service.macro_daily import (
    SID_A50,
    SID_COMM_COPPER,
    SID_COMM_ENERGY,
    SID_COMM_GOLD,
    SID_DJI,
    SID_HSI,
    SID_HSTECH,
    SID_IXIC,
    SID_SPX,
    SID_USDCNH,
    _normalize_fx_daily_df,
    resolve_ine_sc_main,
    resolve_main_fut_by_prefix,
    resolve_sgx_a50_main,
    try_tushare_pro,
)


def _lookback_range(days: int = 120) -> tuple[str, str]:
    end = datetime.now(UTC).date()
    start = end - timedelta(days=days)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


# Network failures are cached briefly so a dead yfinance endpoint does not
# stall every index-signals / snapshot call for the full timeout (each call
# can block tens of seconds). On success the marker is dropped.
_yf_fail_cache: dict[str, float] = {}
_YF_FAIL_TTL_SECONDS = 300.0


def _fetch_yfinance_index(ticker: str) -> dict[str, Any] | None:
    """Fetch index OHLC history via Yahoo Finance."""
    now = time.time()
    if _yf_fail_cache.get(ticker, 0.0) > now:
        return None
    try:
        import yfinance as yf  # type: ignore[import-not-found]

        hist = yf.Ticker(ticker).history(period="25d")
        if hist.empty or len(hist) < 5:
            _yf_fail_cache[ticker] = now + _YF_FAIL_TTL_SECONDS
            return None

        closes: list[float] = []
        for _, row in hist.iterrows():
            try:
                c = float(row["Close"])
                if math.isfinite(c):
                    closes.append(c)
            except Exception:
                pass

        if not closes:
            _yf_fail_cache[ticker] = now + _YF_FAIL_TTL_SECONDS
            return None

        _yf_fail_cache.pop(ticker, None)
        as_of_date = hist.index[-1].strftime("%Y-%m-%d")
        pct_chg = None
        if len(closes) >= 2:
            prev_c, last_c = closes[-2], closes[-1]
            if prev_c > 0:
                pct_chg = (last_c - prev_c) / prev_c * 100.0

        ma5 = sum(closes[-5:]) / 5.0 if len(closes) >= 5 else None
        ma20 = sum(closes[-20:]) / 20.0 if len(closes) >= 20 else None

        return {
            "close": closes[-1],
            "pctChg": pct_chg,
            "asOfDate": as_of_date,
            "ma5": ma5,
            "ma20": ma20,
        }
    except Exception:
        _yf_fail_cache[ticker] = time.time() + _YF_FAIL_TTL_SECONDS
        return None


def _fetch_ixic_via_yfinance() -> dict[str, Any] | None:
    return _fetch_yfinance_index("^IXIC")


def _fetch_dji_via_yfinance() -> dict[str, Any] | None:
    return _fetch_yfinance_index("^DJI")


def _fetch_spx_via_yfinance() -> dict[str, Any] | None:
    return _fetch_yfinance_index("^GSPC")


def _fetch_hsi_via_yfinance() -> dict[str, Any] | None:
    return _fetch_yfinance_index("^HSI")


def _fetch_hstech_via_yfinance() -> dict[str, Any] | None:
    return _fetch_yfinance_index("^HSTECH")


def _fetch_hstech_via_sina() -> dict[str, Any] | None:
    """Fetch HSTECH daily bars via akshare (Sina Finance).

    Reliable in regions where yfinance ``^HSTECH`` is IP rate-limited, and the
    same Sina feed that powers HK realtime quotes (realtime_quote.py).
    """
    # AkShare's Sina index decoder is backed by mini_racer (V8), which can
    # crash the whole process (FATAL in libmini_racer) on macOS.
    if sys.platform == "darwin":
        return None
    try:
        import akshare as ak  # type: ignore[import-not-found]

        df = ak.stock_hk_index_daily_sina(symbol="HSTECH")
    except Exception:
        return None
    if df is None or getattr(df, "empty", True):
        return None

    closes: list[float] = []
    for _, row in df.iterrows():
        try:
            c = float(row["close"])
            if math.isfinite(c):
                closes.append(c)
        except Exception:
            pass
    if not closes:
        return None

    try:
        as_of = pd.to_datetime(df["date"].iloc[-1], errors="coerce")
        as_of_date = as_of.strftime("%Y-%m-%d") if hasattr(as_of, "strftime") else None
    except Exception:
        as_of_date = None
    if not as_of_date:
        return None

    pct_chg = None
    if len(closes) >= 2:
        prev_c, last_c = closes[-2], closes[-1]
        if prev_c > 0:
            pct_chg = (last_c - prev_c) / prev_c * 100.0

    ma5 = sum(closes[-5:]) / 5.0 if len(closes) >= 5 else None
    ma20 = sum(closes[-20:]) / 20.0 if len(closes) >= 20 else None

    return {
        "close": closes[-1],
        "pctChg": pct_chg,
        "asOfDate": as_of_date,
        "ma5": ma5,
        "ma20": ma20,
    }


def _df_to_metrics(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None or df.empty:
        return {}
    d = df.copy()
    # Futures often use settle when close is empty
    if "settle" in d.columns:
        if "close" not in d.columns:
            d["close"] = d["settle"]
        else:
            d["close"] = d["close"].fillna(d["settle"])
    if "close" not in d.columns:
        return {}
    if "trade_date" not in d.columns:
        return {}
    raw_td = d["trade_date"].astype(str)
    parsed = pd.to_datetime(raw_td, format="%Y%m%d", errors="coerce")
    if parsed.isna().all():
        parsed = pd.to_datetime(d["trade_date"], errors="coerce")
    d["_td"] = parsed
    d = d.dropna(subset=["_td"]).sort_values("_td")
    if d.empty:
        return {}
    closes: list[float] = []
    for _, row in d.iterrows():
        try:
            c = float(row["close"])
            if math.isfinite(c):
                closes.append(c)
        except Exception:
            pass
    if not closes:
        return {}
    last = d.iloc[-1]
    as_of = last["_td"]
    as_of_str = as_of.strftime("%Y-%m-%d") if hasattr(as_of, "strftime") else str(as_of)[:10]
    pct = None
    for col in ("pct_chg", "pct_change"):
        if col in d.columns:
            try:
                v = last.get(col)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    pct = float(v)
                    break
            except Exception:
                pass
    if pct is None and len(closes) >= 2:
        prev_c, last_c = closes[-2], closes[-1]
        if prev_c > 0:
            pct = (last_c - prev_c) / prev_c * 100.0
    ma5 = sum(closes[-5:]) / 5.0 if len(closes) >= 5 else None
    ma20 = sum(closes[-20:]) / 20.0 if len(closes) >= 20 else None
    return {
        "close": closes[-1],
        "pctChg": pct,
        "asOfDate": as_of_str,
        "ma5": ma5,
        "ma20": ma20,
    }


def _fetch_on_demand_series(pro: Any | None, series_id: str) -> tuple[dict[str, Any], str | None, str | None]:
    """
    Returns (metrics, source_label, underlying_ts_code).
    US/HK indices prefer yfinance (works without Tushare token).
    """
    sd, ed = _lookback_range(120)
    try:
        if series_id == SID_IXIC:
            yf_metrics = _fetch_ixic_via_yfinance()
            if yf_metrics:
                return yf_metrics, "yfinance.on_demand", "IXIC"
            if pro is None:
                return {}, None, None
            df = pro.index_global(ts_code="IXIC", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "IXIC"
        if series_id == SID_DJI:
            yf_metrics = _fetch_dji_via_yfinance()
            if yf_metrics:
                return yf_metrics, "yfinance.on_demand", "DJI"
            if pro is None:
                return {}, None, None
            df = pro.index_global(ts_code="DJI", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "DJI"
        if series_id == SID_SPX:
            yf_metrics = _fetch_spx_via_yfinance()
            if yf_metrics:
                return yf_metrics, "yfinance.on_demand", "SPX"
            if pro is None:
                return {}, None, None
            df = pro.index_global(ts_code="SPX", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "SPX"
        if series_id == SID_HSI:
            yf_metrics = _fetch_hsi_via_yfinance()
            if yf_metrics:
                return yf_metrics, "yfinance.on_demand", "HSI"
            if pro is None:
                return {}, None, None
            df = pro.index_global(ts_code="HSI", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "HSI"
        if series_id == SID_HSTECH:
            sina_metrics = _fetch_hstech_via_sina()
            if sina_metrics:
                return sina_metrics, "akshare.on_demand", "HSTECH"
            yf_metrics = _fetch_hstech_via_yfinance()
            if yf_metrics:
                return yf_metrics, "yfinance.on_demand", "HSTECH"
            if pro is None:
                return {}, None, None
            df = pro.index_global(ts_code="HSTECH", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "HSTECH"
        if pro is None:
            return {}, None, None
        if series_id == SID_USDCNH:
            df = pro.fx_daily(ts_code="USDCNH.FXCM", start_date=sd, end_date=ed)
            df = _normalize_fx_daily_df(df)
            m = _df_to_metrics(df)
            return m, "tushare.fx_daily.on_demand" if m else None, "USDCNH.FXCM"
        if series_id == SID_A50:
            fut = resolve_sgx_a50_main(pro)
            if fut:
                df = pro.fut_daily(ts_code=fut, start_date=sd, end_date=ed)
                m = _df_to_metrics(df)
                return m, "tushare.fut_daily.on_demand" if m else None, fut
            df = pro.index_global(ts_code="XIN9", start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.index_global.on_demand" if m else None, "XIN9"
        if series_id == SID_COMM_ENERGY:
            und = resolve_ine_sc_main(pro)
            if not und:
                return {}, None, None
            df = pro.fut_daily(ts_code=und, start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.fut_daily.on_demand" if m else None, und
        if series_id == SID_COMM_GOLD:
            und = resolve_main_fut_by_prefix(pro, "SHFE", "AU")
            if not und:
                return {}, None, None
            df = pro.fut_daily(ts_code=und, start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.fut_daily.on_demand" if m else None, und
        if series_id == SID_COMM_COPPER:
            und = resolve_main_fut_by_prefix(pro, "SHFE", "CU")
            if not und:
                return {}, None, None
            df = pro.fut_daily(ts_code=und, start_date=sd, end_date=ed)
            m = _df_to_metrics(df)
            return m, "tushare.fut_daily.on_demand" if m else None, und
    except Exception:
        return {}, None, None
    return {}, None, None


ALWAYS_REFRESH_SERIES = [SID_IXIC, SID_DJI, SID_SPX, SID_USDCNH, SID_A50, SID_HSI, SID_HSTECH]

def _is_data_stale(as_of_date: str | None) -> bool:
    """Check if data is stale (older than 2 days for offshore series)."""
    if not as_of_date:
        return True
    try:
        dt = datetime.strptime(as_of_date, "%Y-%m-%d").date()
        today = datetime.now(UTC).date()
        age_days = (today - dt).days
        return age_days >= 2
    except Exception:
        return True

def enrich_macro_items_on_demand(macro_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill missing/stale macro rows using yfinance and/or Tushare daily APIs (no DB writes)."""
    pro = try_tushare_pro()

    to_fetch: list[tuple[int, str]] = []
    for idx, m in enumerate(macro_items):
        sid = str(m.get("seriesId") or "")
        should_fetch = False

        if m.get("close") is None:
            should_fetch = True
        elif sid in ALWAYS_REFRESH_SERIES:
            should_fetch = True
        elif m.get("realtime") is not True and _is_data_stale(m.get("asOfDate")):
            should_fetch = True

        if should_fetch:
            to_fetch.append((idx, sid))

    if not to_fetch:
        return macro_items

    def _fetch_one(item: tuple[int, str]) -> tuple[int, dict[str, Any], str | None, str | None]:
        idx, sid = item
        metrics, src, und = _fetch_on_demand_series(pro, sid)
        return idx, metrics, src, und

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_fetch_one, item) for item in to_fetch]
        for fut in as_completed(futures):
            idx, metrics, src, und = fut.result()
            if not metrics:
                continue
            m = macro_items[idx]
            m["close"] = metrics.get("close")
            m["pctChg"] = metrics.get("pctChg")
            m["asOfDate"] = metrics.get("asOfDate")
            m["ma5"] = metrics.get("ma5")
            m["ma20"] = metrics.get("ma20")
            if src:
                m["source"] = src
            if und:
                m["underlyingTsCode"] = und
            m["dataSource"] = "on_demand"
    return macro_items


def fetch_hk_index_on_demand(series_id: str) -> tuple[dict[str, Any], str | None]:
    """Public helper for index-signals HK refresh (HSI / HSTECH)."""
    pro = try_tushare_pro()
    metrics, src, _ = _fetch_on_demand_series(pro, series_id)
    return metrics, src


def macro_snapshot_warning() -> str | None:
    if not get_settings().tu_share_api_key:
        return (
            "TU_SHARE_API_KEY is not set; US/HK macro may still refresh via yfinance on demand."
        )
    return None
