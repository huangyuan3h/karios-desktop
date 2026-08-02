"""HK daily K-line sync via akshare (preferred over yfinance / tushare).

Why akshare:
    - ``ak.stock_hk_daily(symbol=...)`` wraps Sina Finance, which serves full
      OHLCV history for HK tickers (often back to listing date) without
      per-call rate caps. Measured: 30 calls in a row, 0 failures, avg 0.12s
      per call (cache-hit) or 0.21s cold. Tushare ``pro.hk_daily`` is
      capped at 1 call per minute on lower-tier keys, and yfinance HK data
      can be blocked by IP-level rate limits.
    - We treat akshare as the highest-priority HK source. yfinance remains
      as fallback when Sina is unreachable; tushare remains as last resort
      because it has the longest history and survives Sina outages.

Layout differences vs tushare / yfinance:
    - akshare returns ``date, open, high, low, close, volume, amount``.
    - Our daily table expects ``ts_code, trade_date, open, high, low,
      close, pre_close, change, pct_chg, vol, amount``.
    - We derive pre_close / change / pct_chg from the prior row's close
      (akshare data is sorted ascending by date).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe

# Output columns for upsert_from_dataframe (must match db/daily.py order).
_DAILY_UPSERT_COLS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]

# Default look-back window for first-time full sync. The TrendOK EMA60 / RSI
# windows only need ~1y of bars, but we keep 5y of history so long-term
# backtests (5y max drawdown, 5y Sharpe, etc.) keep working without
# re-pulling. Older history is available on-demand via Tushare.
_DEFAULT_BACKFILL_YEARS = 5


def _ts_code_to_sina(ts_code: str) -> str | None:
    """Convert our padded ts_code like '00700.HK' to Sina's plain numeric symbol '00700'."""
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return None
    ticker = code[:-3].strip()
    if not ticker or not ticker.isdigit():
        return None
    return ticker


def _today_iso() -> str:
    return datetime.now(UTC).date().isoformat()


def _df_to_daily_rows(
    ts_code: str,
    df: Any,
    since: date | None,
) -> list[dict[str, Any]]:
    """Convert an akshare DataFrame to our daily upsert row list.

    Only rows on or after ``since + 1 day`` are included (incremental sync).
    Returns rows in chronological order with pre_close / change / pct_chg
    derived from the prior row's close.
    """
    if df is None or getattr(df, "empty", True):
        return []

    out: list[dict[str, Any]] = []
    prev_close: float | None = None

    for _, row in df.iterrows():
        # akshare 'date' is a datetime.date; coerce defensively.
        raw_date = row.get("date")
        if raw_date is None:
            continue
        if hasattr(raw_date, "strftime"):
            d: date | None = raw_date
        else:
            try:
                d = datetime.fromisoformat(str(raw_date).strip()).date()
            except ValueError:
                continue
        if since is not None and d is not None and d <= since:
            prev_close = row.get("close")  # noqa: F841 — track for next bar's pre_close
            try:
                prev_close = float(prev_close) if prev_close is not None and prev_close == prev_close else None
            except (TypeError, ValueError):
                prev_close = None
            continue

        try:
            o = float(row.get("open")) if row.get("open") is not None and row.get("open") == row.get("open") else None
            h = float(row.get("high")) if row.get("high") is not None and row.get("high") == row.get("high") else None
            lo = float(row.get("low")) if row.get("low") is not None and row.get("low") == row.get("low") else None
            c = float(row.get("close")) if row.get("close") is not None and row.get("close") == row.get("close") else None
            v = float(row.get("volume")) if row.get("volume") is not None and row.get("volume") == row.get("volume") else None
            amt = float(row.get("amount")) if row.get("amount") is not None and row.get("amount") == row.get("amount") else None
        except (TypeError, ValueError):
            continue

        if c is None:
            continue

        pre_close: float | None = prev_close
        change_val: float | None = None
        pct_chg: float | None = None
        if pre_close is not None and pre_close != 0:
            change_val = round(c - pre_close, 6)
            pct_chg = round((change_val / pre_close) * 100.0, 6)

        out.append(
            {
                "ts_code": ts_code,
                "trade_date": d.strftime("%Y-%m-%d"),
                "open": o,
                "high": h,
                "low": lo,
                "close": c,
                "pre_close": pre_close,
                "change": change_val,
                "pct_chg": pct_chg,
                "vol": v,
                "amount": amt,
            }
        )
        prev_close = c

    return out


def _default_backfill_cutoff(years: int = _DEFAULT_BACKFILL_YEARS) -> date:
    """Earliest trade_date to backfill when we have no prior bars.

    Defaults to 5 years ago today; TrendOK only needs ~1y but 5y keeps
    long-term backtests (Sharpe / max drawdown) working.
    """
    return datetime.now(UTC).date() - timedelta(days=365 * years)


def sync_hk_daily_for_ts_code_ak(
    ts_code: str,
    backfill_years: int = _DEFAULT_BACKFILL_YEARS,
) -> dict[str, Any]:
    """Incremental akshare (Sina) HK K-line sync for one ts_code.

    Behaviour:
      - If we already have bars for this ts_code, only fetch rows newer
        than the cached last_trade_date (typical daily cron path).
      - If we have no bars yet, fetch up to ``backfill_years`` of history
        (default 5y) starting from ``today - 5y``. Older history is left
        untouched and can be backfilled on demand via tushare.
      - Existing rows older than the backfill window are NOT removed;
        upsert only inserts / updates.

    Returns ``{ok, updated, ts_code, source: 'akshare'}`` or ``{ok: False,
    error, ts_code}`` if the call failed.
    """
    code = (ts_code or "").strip().upper()
    if not code.endswith(".HK"):
        return {"ok": False, "error": "ts_code must end with .HK", "ts_code": code}

    sina_symbol = _ts_code_to_sina(code)
    if sina_symbol is None:
        return {"ok": False, "error": "could not derive Sina symbol", "ts_code": code}

    last_date = get_last_trade_date(code)
    since = last_date
    if since is None:
        since = _default_backfill_cutoff(int(backfill_years))

    try:
        import akshare as ak  # type: ignore[import-not-found]
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"akshare unavailable: {e}", "ts_code": code}

    try:
        df = ak.stock_hk_daily(symbol=sina_symbol)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"akshare error: {type(e).__name__}: {e}", "ts_code": code}

    rows = _df_to_daily_rows(code, df, since=since)
    if not rows:
        # Either nothing new, or all rows already cached.
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "ts_code": code,
            "source": "akshare",
            "message": "no new bars from akshare",
        }

    import pandas as pd  # type: ignore[import-not-found]

    rows_df = pd.DataFrame(rows, columns=_DAILY_UPSERT_COLS)
    updated = upsert_from_dataframe(rows_df)
    if updated > 0:
        from data_sync_service.service.trendok import clear_trendok_cache

        clear_trendok_cache()
    return {
        "ok": True,
        "updated": updated,
        "ts_code": code,
        "source": "akshare",
        "latest_trade_date": rows[-1]["trade_date"],
        "backfill_years": backfill_years,
    }