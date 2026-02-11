"""Close-time sync: use trade calendar to sync by trade_date (market-wide), not per-stock loops."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd  # type: ignore[import-not-found]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.daily import upsert_from_dataframe as upsert_daily
from data_sync_service.db.daily import update_adj_factor_from_dataframe
from data_sync_service.db.sync_job_record import get_last_success, get_today_run, insert_record
from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day
from data_sync_service.service.trade_calendar import sync_trade_calendar

JOB_TYPE = "stock_close_sync"

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


def _cn_today() -> date:
    return datetime.now(ZoneInfo("Asia/Shanghai")).date()

def _cn_now() -> datetime:
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def _parse_yyyymmdd(s: str) -> date:
    return date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:8]}")


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _fetch_paged_daily(pro, trade_date: str, limit: int = 5000) -> int:
    offset = 0
    total = 0
    while True:
        df: pd.DataFrame = pro.daily(
            trade_date=trade_date,
            limit=limit,
            offset=offset,
            fields=",".join(DAILY_FIELDS),
        )
        if df is None or df.empty:
            break
        total += upsert_daily(df)
        if len(df) < limit:
            break
        offset += limit
    return total


def _fetch_paged_adj_factor(pro, trade_date: str, limit: int = 5000) -> int:
    offset = 0
    total = 0
    while True:
        df: pd.DataFrame = pro.adj_factor(
            trade_date=trade_date,
            limit=limit,
            offset=offset,
        )
        if df is None or df.empty:
            break
        total += update_adj_factor_from_dataframe(df)
        if len(df) < limit:
            break
        offset += limit
    return total


def sync_close(exchange: str = "SSE", *, force: bool = False) -> dict:
    """
    Close-time sync:
    - Requires trade calendar to be present.
    - If today is not a trading day: skip.
    - If today's run already succeeded: skip.
    - If today's run failed: resume from the next trading date after last_ts_code (stored as YYYYMMDD marker).
    - Otherwise: sync from the next trading day after last successful sync time (usually 1 day).
    - For each trading day: pull market-wide daily bars (paged) and adj_factor (paged).
    """
    today_run = get_today_run(JOB_TYPE)
    if today_run and today_run.get("success") and not bool(force):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    today = _cn_today()
    now_cn = _cn_now()
    open_flag = is_trading_day(exchange, today)
    trade_cal_auto: dict | None = None
    if open_flag is None:
        # Auto-heal: sync trade calendar first (include today).
        # We fetch a window large enough to cover common gaps and near-future dates.
        start = today - timedelta(days=400)
        end = today + timedelta(days=30)
        trade_cal_auto = sync_trade_calendar(
            exchange=exchange,
            start_date=_to_yyyymmdd(start),
            end_date=_to_yyyymmdd(end),
        )
        if not trade_cal_auto.get("ok"):
            return {
                "ok": False,
                "error": (
                    "trade calendar missing for today; auto sync trade_cal failed: "
                    + str(trade_cal_auto.get("error") or "unknown error")
                ),
            }
        open_flag = is_trading_day(exchange, today)
        if open_flag is None:
            return {"ok": False, "error": "trade calendar still missing for today after trade_cal sync"}
    if open_flag is False:
        out = {"ok": True, "skipped": True, "message": "not a trading day"}
        if trade_cal_auto is not None:
            out["trade_cal"] = {"autoSynced": True, "result": trade_cal_auto}
        return out

    # Determine start date by resume marker or last success time.
    start_date = today
    if today_run and today_run.get("success") is False and today_run.get("last_ts_code"):
        marker = str(today_run["last_ts_code"])
        start_date = _parse_yyyymmdd(marker) + timedelta(days=1)
    else:
        last_ok = get_last_success(JOB_TYPE)
        if last_ok and last_ok.get("sync_at"):
            # sync_at is ISO; use its date in Asia/Shanghai as a conservative baseline
            sync_at = datetime.fromisoformat(str(last_ok["sync_at"]))
            start_date = sync_at.astimezone(ZoneInfo("Asia/Shanghai")).date() + timedelta(days=1)

    if start_date > today:
        return {"ok": True, "skipped": True, "message": "already up to date"}

    # Guard: avoid marking today's close as synced before market close.
    # If user clicks during trading hours, tushare may return empty/partial rows.
    # We allow catching up previous days before close, but never process *today* before close-ready.
    close_ready_minutes = 17 * 60 + 5  # after market close + data settle buffer
    now_minutes = now_cn.hour * 60 + now_cn.minute
    end_date = today
    if now_cn.date() == today and now_minutes < close_ready_minutes:
        if start_date >= today:
            out0 = {
                "ok": True,
                "skipped": True,
                "message": "too early; market close sync is available after 17:05 Asia/Shanghai",
            }
            if trade_cal_auto is not None:
                out0["trade_cal"] = {"autoSynced": True, "result": trade_cal_auto}
            return out0
        # Catch up only up to yesterday; do NOT mark today's close as done.
        end_date = today - timedelta(days=1)

    settings = get_settings()
    if not settings.tu_share_api_key:
        out2 = {"ok": False, "error": "TU_SHARE_API_KEY is not set"}
        if trade_cal_auto is not None:
            out2["trade_cal"] = {"autoSynced": True, "result": trade_cal_auto}
        return out2
    pro = ts.pro_api(settings.tu_share_api_key)

    trade_dates = get_open_dates(exchange=exchange, start_date=start_date, end_date=end_date)
    if not trade_dates:
        return {"ok": True, "updated": 0, "message": "no trading dates in range"}

    total_daily = 0
    total_factor = 0
    last_completed: str | None = None

    for d in trade_dates:
        td = _to_yyyymmdd(d)
        try:
            n_daily = _fetch_paged_daily(pro, td)
            n_factor = _fetch_paged_adj_factor(pro, td)
            # If today's daily is empty, treat as a transient upstream delay and allow retry later.
            if d == today and n_daily <= 0:
                raise RuntimeError("tushare daily returned empty for today; try again later")
            total_daily += n_daily
            total_factor += n_factor
            last_completed = td
        except Exception as e:  # noqa: BLE001
            insert_record(JOB_TYPE, success=False, last_ts_code=last_completed, error_message=str(e))
            return {"ok": False, "error": str(e), "last_marker": last_completed}

    # Only mark the close job as "done today" when we actually processed today's trade_date.
    if end_date == today:
        insert_record(JOB_TYPE, success=True, last_ts_code=last_completed, error_message=None)
    out3 = {
        "ok": True,
        "updated_daily_rows": total_daily,
        "updated_adj_factor_rows": total_factor,
        "trade_dates": [d.isoformat() for d in trade_dates],
    }
    if end_date != today:
        out3["partial"] = True
        out3["message"] = "pre-close catchup: synced until yesterday; will sync today after close"
    if trade_cal_auto is not None:
        out3["trade_cal"] = {"autoSynced": True, "result": trade_cal_auto}
    return out3


def get_close_sync_status() -> dict:
    return {
        "job_type": JOB_TYPE,
        "today_run": get_today_run(JOB_TYPE),
        "last_success": get_last_success(JOB_TYPE),
    }

