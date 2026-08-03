"""Daily HK K-line sync: full sync with resume and skip-if-today-ok.

Source priority for each ts_code (highest first):

  1. **Tencent ifzq** (``hk_daily_tx``) — plain-JSON HK K-lines, no JS
     decoding / V8 dependency. Reliable on macOS where akshare's Sina
     decoder crashes the whole process. Full OHLCV history via paging.
  2. **akshare** (``ak.stock_hk_daily``) — Sina Finance source. Fast, full
     history, but its py_mini_racer (V8) decoder crashes on macOS 26, so it
     is skipped there and only used on non-darwin platforms.
  3. **yfinance** — kept as fallback for tickers Sina/Tencent can't
     resolve, or when both are unreachable. Subject to IP-level rate caps
     and has been observed returning "delisted" for valid HK tickers.
  4. **tushare** (``pro.hk_daily``) — last-resort fallback. Long history
     and survives upstream outages, but rate-limited to ~1 call/min on
     lower-tier keys, so unsuitable for full-market batches.

The per-stock logic is incremental (only fetches rows newer than the
cached last_trade_date), so re-running daily is cheap and resilient.
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.daily import get_last_trade_date, upsert_from_dataframe
from data_sync_service.db.stock_basic import fetch_ts_codes_by_market
from data_sync_service.db.sync_job_record import get_today_run, insert_record

logger = logging.getLogger(__name__)

JOB_TYPE = "hk_daily_full"
# First-time backfill starts 5 years ago today (matches hk_daily_ak default).
# TrendOK only needs ~1y of bars; 5y keeps long-term backtests working.
DAILY_FIELDS = [
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

# Default pacing between per-ticker calls. Sources are documented as having
# no strict per-call rate cap, but we still space requests to be polite.
# 0.2s × 2700 stocks ≈ 9 minutes for a full sweep.
_AK_DELAY_SECONDS = 0.2
_PROGRESS_EVERY = 50
_BACKFILL_YEARS = 5


def _today_yyyymmdd() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _backfill_start_yyyymmdd() -> str:
    """Earliest date tushare will pull on first-time full sync (5y ago today)."""
    return _date_to_yyyymmdd(
        datetime.now(UTC).date() - timedelta(days=365 * _BACKFILL_YEARS)
    )


def _sync_one_with_fallback(ts_code: str) -> dict[str, Any]:
    """Sync a single HK ts_code: tencent → akshare → yfinance → tushare.

    Tencent ifzq is the primary source (plain JSON, no V8). akshare is only
    used on non-darwin platforms where its V8 decoder cannot crash the
    process. Returns the merged result with ``source`` set to whichever
    backend actually delivered bars (or the highest that didn't fail), and
    ``updated`` reflecting the rows written.
    """
    # 1. Tencent ifzq: no V8 / JS decoding, works on macOS.
    from data_sync_service.service.hk_daily_tx import sync_hk_daily_for_ts_code_tx

    tx_result = sync_hk_daily_for_ts_code_tx(ts_code)
    if tx_result.get("ok") and int(tx_result.get("updated") or 0) > 0:
        return tx_result

    # 2. akshare (Sina): fastest + no rate cap — non-darwin only.
    ak_result = {"ok": False, "error": "akshare_disabled_on_darwin"}
    if sys.platform != "darwin":
        from data_sync_service.service.hk_daily_ak import sync_hk_daily_for_ts_code_ak

        ak_result = sync_hk_daily_for_ts_code_ak(ts_code)
        if ak_result.get("ok") and int(ak_result.get("updated") or 0) > 0:
            return ak_result
    ak_error = None if ak_result.get("ok") else ak_result.get("error")

    # 3. yfinance: kept for tickers Sina/Tencent can't resolve.
    from data_sync_service.service.hk_daily_yf import sync_hk_daily_for_ts_code_yf

    yf_result = sync_hk_daily_for_ts_code_yf(ts_code)
    if yf_result.get("ok") and int(yf_result.get("updated") or 0) > 0:
        return yf_result

    # 4. tushare: last resort (1 call/min rate cap, but most reliable).
    ts_result = _tushare_sync_one(ts_code)
    if ts_result.get("ok") and int(ts_result.get("updated") or 0) > 0:
        return ts_result

    # Nothing delivered new bars. Return the most informative error from
    # whichever source failed first (tencent), then a normalised "no data".
    if not tx_result.get("ok"):
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "ts_code": ts_code,
            "source": "tencent",
            "message": f"tencent failed: {tx_result.get('error')}; no other source had data either",
        }
    if not ak_result.get("ok"):
        return {
            "ok": True,
            "updated": 0,
            "skipped": True,
            "ts_code": ts_code,
            "source": "akshare",
            "message": f"akshare failed: {ak_error}; no other source had data either",
        }
    return {
        "ok": True,
        "updated": 0,
        "skipped": True,
        "ts_code": ts_code,
        "source": "tencent",
        "message": "no source delivered new bars",
    }


def sync_hk_daily_full() -> dict[str, Any]:
    """
    Full sync for HK stocks with source-priority fallback
    (tencent → akshare → yfinance → tushare):
    - If today's run already succeeded: skip.
    - If today's run failed: resume from the ts_code after last_ts_code.
    - Per-stock logic is incremental (only fetches rows newer than cached last_trade_date).
    """
    run = get_today_run(JOB_TYPE)
    if run and run.get("success"):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    ts_codes = fetch_ts_codes_by_market("HK")
    if not ts_codes:
        return {"ok": True, "updated": 0, "message": "no HK stock list"}

    start_index = 0
    if run and run.get("success") is False and run.get("last_ts_code"):
        try:
            idx = ts_codes.index(run["last_ts_code"])
            start_index = idx + 1
        except ValueError:
            pass

    total_rows = 0
    skipped_count = 0
    failed_count = 0
    source_counts: dict[str, int] = {"tencent": 0, "akshare": 0, "yfinance": 0, "tushare": 0}
    remaining = len(ts_codes) - start_index
    logger.info(
        "hk_daily_full_sync start: total=%s resuming_from=%s remaining=%s",
        len(ts_codes),
        start_index,
        remaining,
    )

    for i in range(start_index, len(ts_codes)):
        ts_code = ts_codes[i]
        try:
            result = _sync_one_with_fallback(ts_code)
            updated = int(result.get("updated") or 0) if result.get("ok") else 0
            source = result.get("source") or "unknown"
            if updated == 0:
                skipped_count += 1
            else:
                total_rows += updated
                source_counts[source] = source_counts.get(source, 0) + 1
        except Exception as exc:  # noqa: BLE001
            failed_count += 1
            logger.warning("hk_daily_full_sync %s exception: %s", ts_code, exc)
            # Continue instead of aborting: a single bad ticker should not
            # block the whole batch. Resume next day picks up where we left.

        done = i + 1 - start_index
        if done % _PROGRESS_EVERY == 0 or done == remaining:
            logger.info(
                "hk_daily_full_sync progress: %s/%s updated=%s skipped=%s failed=%s sources=%s",
                done,
                remaining,
                total_rows,
                skipped_count,
                failed_count,
                source_counts,
            )

        if _AK_DELAY_SECONDS > 0 and i < len(ts_codes) - 1:
            time.sleep(_AK_DELAY_SECONDS)

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    logger.info(
        "hk_daily_full_sync done: total=%s updated=%s skipped=%s failed=%s sources=%s",
        remaining,
        total_rows,
        skipped_count,
        failed_count,
        source_counts,
    )
    return {
        "ok": True,
        "updated": total_rows,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "sources": source_counts,
    }


def _tushare_sync_one(ts_code: str) -> dict[str, Any]:
    """Single-ticker tushare fallback used by sync_hk_daily_full.

    First-time sync pulls 5 years of history; subsequent calls are
    incremental from the cached last_trade_date.
    """
    code = (ts_code or "").strip().upper()
    if not code:
        return {"ok": False, "error": "ts_code is required"}
    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    last_date = get_last_trade_date(code)
    if last_date is None:
        start_date = _backfill_start_yyyymmdd()
    else:
        start_date = _date_to_yyyymmdd(last_date + timedelta(days=1))
    end_date = _today_yyyymmdd()
    if start_date > end_date:
        return {"ok": True, "updated": 0, "skipped": True, "ts_code": code}

    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        df: pd.DataFrame = pro.hk_daily(
            ts_code=code,
            start_date=start_date,
            end_date=end_date,
            fields=",".join(DAILY_FIELDS),
        )
        updated = 0
        if df is not None and not df.empty:
            updated = upsert_from_dataframe(df)
        return {"ok": True, "updated": updated, "ts_code": code, "source": "tushare"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e), "ts_code": code}


def get_hk_daily_sync_status() -> dict[str, Any]:
    """Return today's run record for hk_daily_full if any."""
    run = get_today_run(JOB_TYPE)
    if run is None:
        return {"job_type": JOB_TYPE, "today_run": None}
    return {"job_type": JOB_TYPE, "today_run": run}


def sync_hk_daily_for_ts_code(ts_code: str) -> dict[str, Any]:
    """
    Single-ticker HK sync using the source-priority chain
    (tencent → akshare → yfinance → tushare). Used by market_bars for the
    hot-path ``bars?force=true`` refresh.
    """
    return _sync_one_with_fallback(ts_code)