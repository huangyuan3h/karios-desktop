"""HK daily K-line sync via yfinance (fallback when tushare hk_daily is rate-limited).

tushare `pro.hk_daily` is limited to 1 call per hour on lower-tier keys, so
single-stock refresh on user watchlists can stall for hours. Yahoo Finance
serves HK OHLC at `XXXX.HK` (4-digit padded, no leading-zero stripping).
We map our 5-digit ts_code (e.g. `00700.HK`) to yfinance's 4-digit format
(`0700.HK`). Doing `lstrip("0")` here is wrong: `00001.HK` would become
`1.HK`, which yfinance 404s. We right-trim to 4 digits instead.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe

# yfinance download cap (the lib itself accepts no timeout).
YF_FETCH_TIMEOUT_SECONDS = 60

logger = logging.getLogger(__name__)


def _ts_code_to_yf(ts_code: str) -> str | None:
    """Convert our 5-digit ts_code (e.g. '00700.HK') to yfinance's 4-digit ('0700.HK')."""
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return None
    digits = code[:-3]
    if not digits.isdigit() or not digits:
        return None
    # yfinance expects 4-digit padding (e.g. '0001.HK', '0700.HK', '9988.HK').
    # Our 5-digit tushare format -> last 4 digits is always correct.
    yf_ticker = digits[-4:].zfill(4)
    return f"{yf_ticker}.HK"


def _today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _df_to_daily_rows(ts_code: str, df: Any) -> list[dict[str, Any]]:
    """Convert a yfinance DataFrame into our daily upsert dict list."""
    if df is None or getattr(df, "empty", True):
        return []
    out: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        try:
            d = idx.strftime("%Y-%m-%d")
        except Exception:
            continue
        try:
            o = float(row["Open"]) if row.get("Open") is not None and row["Open"] == row["Open"] else None
            h = float(row["High"]) if row.get("High") is not None and row["High"] == row["High"] else None
            lo = float(row["Low"]) if row.get("Low") is not None and row["Low"] == row["Low"] else None
            c = float(row["Close"]) if row.get("Close") is not None and row["Close"] == row["Close"] else None
            v = float(row["Volume"]) if row.get("Volume") is not None and row["Volume"] == row["Volume"] else None
        except (TypeError, ValueError):
            continue
        if c is None:
            continue
        out.append(
            {
                "ts_code": ts_code,
                "trade_date": d,
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "vol": v,
                "amount": None,
            }
        )
    return out


def sync_hk_daily_for_ts_code_yf(
    ts_code: str,
    backfill_years: int = 5,
) -> dict[str, Any]:
    """Incremental yfinance HK K-line sync for one ts_code. Bypasses tushare rate limit.

    First-time sync pulls up to ``backfill_years`` of history (default 5y);
    subsequent calls are incremental from the cached last_trade_date.
    Pre-existing rows older than the backfill window are not removed.
    """
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return {"ok": False, "error": "ts_code must end with .HK", "ts_code": code}

    yf_symbol = _ts_code_to_yf(code)
    if yf_symbol is None:
        return {"ok": False, "error": "could not derive yfinance symbol", "ts_code": code}

    last_date = get_last_trade_date(code)
    end_date = datetime.now(UTC).date().isoformat()
    if last_date is None:
        # First-time backfill: cap at ``backfill_years`` years to keep
        # bootstrap time and DB size reasonable. TrendOK only needs ~1y,
        # but 5y keeps long-term backtests working.
        start_date = (datetime.now(UTC).date() - timedelta(days=365 * int(backfill_years))).isoformat()
    else:
        # Fetch from the day after our last known bar.
        start_date_obj = last_date + timedelta(days=1)
        start_date = start_date_obj.isoformat()
        if start_date > end_date:
            return {"ok": True, "updated": 0, "skipped": True, "ts_code": code, "source": "yfinance"}

    try:
        import pandas as pd  # type: ignore[import-not-found]
        import yfinance as yf  # type: ignore[import-not-found]
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"yfinance unavailable: {e}", "ts_code": code}

    try:
        # yfinance has no timeout parameter — run the download in a thread and
        # abandon it after YF_FETCH_TIMEOUT_SECONDS so a stalled host cannot
        # hang a user-facing force-refresh request indefinitely.
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                lambda: yf.Ticker(yf_symbol).history(
                    start=start_date,
                    end=end_date,
                    auto_adjust=False,
                    actions=False,
                )
            )
            df = future.result(timeout=YF_FETCH_TIMEOUT_SECONDS)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"yfinance error: {type(e).__name__}: {e}", "ts_code": code}

    rows = _df_to_daily_rows(code, df)
    if not rows:
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "message": "no new bars from yfinance",
            "ts_code": code,
            "source": "yfinance",
        }

    rows_df = pd.DataFrame(
        rows,
        columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"],
    )
    updated = upsert_from_dataframe(rows_df)
    if updated > 0:
        from data_sync_service.service.trendok import clear_trendok_cache

        clear_trendok_cache()
    return {"ok": True, "updated": updated, "ts_code": code, "source": "yfinance"}