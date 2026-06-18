"""ETF fund-flow sync via Tushare fund_daily + fund_share for dashboard watchlist."""

from __future__ import annotations

import random
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]
import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db.etf_fund_flow import (
    ensure_table,
    fetch_row,
    fetch_rows_for_codes,
    get_last_trade_date,
    get_latest_date,
    upsert_daily_rows,
)
from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.trade_calendar import get_open_dates

JOB_TYPE = "etf_fund_flow_watchlist"
FULL_START_DATE = "20230101"
SECTOR_MOMENTUM_3D_THRESHOLD = 1e9  # 10亿 CNY

ETF_WATCHLIST: list[dict[str, str]] = [
    {"symbol": "510300", "ts_code": "510300.SH", "name": "沪深300 ETF", "category": "broad"},
    {"symbol": "510050", "ts_code": "510050.SH", "name": "上证50 ETF", "category": "broad"},
    {"symbol": "510500", "ts_code": "510500.SH", "name": "中证500 ETF", "category": "broad"},
    {"symbol": "512480", "ts_code": "512480.SH", "name": "半导体 ETF", "category": "sector"},
    {"symbol": "515880", "ts_code": "515880.SH", "name": "通信 ETF", "category": "sector"},
    {"symbol": "159819", "ts_code": "159819.SZ", "name": "人工智能 ETF", "category": "sector"},
]

FUND_DAILY_FIELDS = "ts_code,trade_date,close,vol,amount"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_yyyymmdd() -> str:
    return datetime.now(tz=UTC).strftime("%Y%m%d")


def _date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _yyyymmdd_to_iso(s: str) -> str:
    s2 = str(s).strip()
    if len(s2) == 8 and s2.isdigit():
        return f"{s2[:4]}-{s2[4:6]}-{s2[6:8]}"
    return s2


def _with_retry(fn, *, tries: int = 3, base_sleep_s: float = 0.5, max_sleep_s: float = 3.0):
    tries2 = max(1, min(int(tries), 5))
    last: Exception | None = None
    for i in range(tries2):
        try:
            return fn()
        except Exception as e:
            last = e
            if i >= tries2 - 1:
                raise
            sleep_s = min(float(max_sleep_s), float(base_sleep_s) * (2**i))
            sleep_s = sleep_s * (0.7 + random.random() * 0.6)
            time.sleep(max(0.0, sleep_s))
    if last is not None:
        raise last
    raise RuntimeError("Retry wrapper failed unexpectedly.")


def compute_avg_price(*, close: float | None, vol: float | None, amount: float | None) -> float | None:
    """VWAP from fund_daily amount (千元) and vol (手); fallback to close."""
    try:
        v = float(vol or 0.0)
        a = float(amount or 0.0)
        if v > 0 and a > 0:
            return (a * 1000.0) / (v * 100.0)
    except (TypeError, ValueError):
        pass
    if close is not None:
        try:
            c = float(close)
            if c == c:
                return c
        except (TypeError, ValueError):
            pass
    return None


def compute_net_inflow_1d(
    *,
    fd_share_today: float | None,
    fd_share_prev: float | None,
    avg_price: float | None,
) -> float | None:
    """Net inflow CNY = delta fd_share (万份) * 10000 * avg_price."""
    if fd_share_today is None or fd_share_prev is None or avg_price is None:
        return None
    try:
        delta = float(fd_share_today) - float(fd_share_prev)
        price = float(avg_price)
        if price != price:
            return None
        return delta * 10_000.0 * price
    except (TypeError, ValueError):
        return None


def classify_signal(
    *,
    category: str,
    net_flow_1d: float | None,
    net_flow_3d: float | None,
) -> str:
    cat = str(category or "").strip().lower()
    d1 = net_flow_1d
    d3 = net_flow_3d
    if d1 is None or d3 is None:
        return "Neutral"

    if cat == "broad":
        if d1 > 0 and d3 > 0:
            return "National Team Buy"
        if d1 < 0 and d3 < 0:
            return "National Team Outflow"
        return "Neutral"

    if cat == "sector":
        if d3 > SECTOR_MOMENTUM_3D_THRESHOLD:
            return "Sector Momentum"
        if d1 < 0 and d3 < 0:
            return "Inst Outflow"
        return "Neutral"

    return "Neutral"


def signal_display(signal: str) -> str:
    mapping = {
        "National Team Buy": "🛡️ National Team Buy",
        "National Team Outflow": "⚠️ National Team Outflow",
        "Sector Momentum": "📈 Sector Momentum",
        "Inst Outflow": "⚠️ Inst Outflow",
        "Neutral": "➖ Neutral",
    }
    return mapping.get(signal, f"➖ {signal}")


def _prev_open_date(exchange: str, d: date) -> date | None:
    start = d - timedelta(days=30)
    open_dates = get_open_dates(exchange, start, d)
    prior = [x for x in open_dates if x < d]
    return prior[-1] if prior else None


def _exchange_for_ts_code(ts_code: str) -> str:
    return "SSE" if str(ts_code).endswith(".SH") else "SZSE"


def _recompute_net_inflows_for_code(ts_code: str, *, updated_at: str) -> int:
    """Recompute net_inflow for all rows of one ETF after share/price upsert."""
    rows = fetch_rows_for_codes([ts_code])
    if not rows:
        return 0
    exchange = _exchange_for_ts_code(ts_code)
    by_date: dict[str, dict[str, Any]] = {}
    for row in rows:
        td = str(row.get("trade_date") or "")
        if td:
            by_date[td] = dict(row)

    updates: list[dict[str, Any]] = []
    for td, row in sorted(by_date.items()):
        try:
            d = date.fromisoformat(td)
        except ValueError:
            continue
        prev_d = _prev_open_date(exchange, d)
        if prev_d is None:
            continue
        prev_row = by_date.get(prev_d.isoformat())
        if not prev_row:
            prev_row = fetch_row(ts_code, prev_d.isoformat())
        net = compute_net_inflow_1d(
            fd_share_today=row.get("fd_share"),
            fd_share_prev=(prev_row or {}).get("fd_share"),
            avg_price=row.get("avg_price"),
        )
        updates.append(
            {
                "ts_code": ts_code,
                "trade_date": td,
                "fd_share": row.get("fd_share"),
                "close": row.get("close"),
                "avg_price": row.get("avg_price"),
                "net_inflow": net,
                "updated_at": updated_at,
            }
        )
    return upsert_daily_rows(updates)


def _merge_tushare_frames(
    ts_code: str,
    *,
    share_df: pd.DataFrame | None,
    daily_df: pd.DataFrame | None,
    updated_at: str,
) -> list[dict[str, Any]]:
    share_by_date: dict[str, float] = {}
    if share_df is not None and not share_df.empty:
        for _, row in share_df.iterrows():
            td = _yyyymmdd_to_iso(str(row.get("trade_date") or ""))
            if not td:
                continue
            try:
                share_by_date[td] = float(row.get("fd_share"))
            except (TypeError, ValueError):
                continue

    daily_by_date: dict[str, dict[str, float | None]] = {}
    if daily_df is not None and not daily_df.empty:
        for _, row in daily_df.iterrows():
            td = _yyyymmdd_to_iso(str(row.get("trade_date") or ""))
            if not td:
                continue
            close = row.get("close")
            vol = row.get("vol")
            amount = row.get("amount")
            try:
                close_f = float(close) if close is not None and close == close else None
            except (TypeError, ValueError):
                close_f = None
            try:
                vol_f = float(vol) if vol is not None and vol == vol else None
            except (TypeError, ValueError):
                vol_f = None
            try:
                amount_f = float(amount) if amount is not None and amount == amount else None
            except (TypeError, ValueError):
                amount_f = None
            daily_by_date[td] = {
                "close": close_f,
                "vol": vol_f,
                "amount": amount_f,
                "avg_price": compute_avg_price(close=close_f, vol=vol_f, amount=amount_f),
            }

    all_dates = sorted(set(share_by_date.keys()) | set(daily_by_date.keys()))
    out: list[dict[str, Any]] = []
    for td in all_dates:
        daily = daily_by_date.get(td, {})
        out.append(
            {
                "ts_code": ts_code,
                "trade_date": td,
                "fd_share": share_by_date.get(td),
                "close": daily.get("close"),
                "avg_price": daily.get("avg_price"),
                "net_inflow": None,
                "updated_at": updated_at,
            }
        )
    return out


def sync_etf_fund_flow_watchlist(*, force: bool = False) -> dict[str, Any]:
    """
    Incremental sync for hardcoded ETF watchlist via fund_share + fund_daily.
    Skip if today's job already succeeded unless force=True.
    """
    if not force:
        run = get_today_run(JOB_TYPE)
        if run and run.get("success"):
            return {"ok": True, "skipped": True, "message": "already synced today"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    ensure_table()
    pro = ts.pro_api(settings.tu_share_api_key)
    end_date = _today_yyyymmdd()
    updated_at = _now_iso()
    total_rows = 0
    last_successful_ts_code: str | None = None

    start_index = 0
    if not force:
        run = get_today_run(JOB_TYPE)
        if run and run.get("success") is False and run.get("last_ts_code"):
            codes = [w["ts_code"] for w in ETF_WATCHLIST]
            try:
                start_index = codes.index(run["last_ts_code"]) + 1
            except ValueError:
                pass

    for spec in ETF_WATCHLIST[start_index:]:
        ts_code = spec["ts_code"]
        try:
            last_date = get_last_trade_date(ts_code)
            if last_date is None:
                start_date = FULL_START_DATE
            else:
                start_date = _date_to_yyyymmdd(last_date + timedelta(days=1))

            if start_date > end_date:
                last_successful_ts_code = ts_code
                continue

            share_df = _with_retry(
                lambda: pro.fund_share(ts_code=ts_code, start_date=start_date, end_date=end_date)
            )
            daily_df = _with_retry(
                lambda: pro.fund_daily(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=FUND_DAILY_FIELDS,
                )
            )

            merged = _merge_tushare_frames(
                ts_code,
                share_df=share_df,
                daily_df=daily_df,
                updated_at=updated_at,
            )
            if merged:
                total_rows += upsert_daily_rows(merged)
            total_rows += _recompute_net_inflows_for_code(ts_code, updated_at=updated_at)

            last_successful_ts_code = ts_code
            time.sleep(0.35)
        except Exception as e:  # noqa: BLE001
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=last_successful_ts_code,
                error_message=str(e),
            )
            return {"ok": False, "error": str(e), "last_ts_code": last_successful_ts_code}

    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total_rows}


def _sum_net_inflow_for_dates(
    rows_by_date: dict[str, dict[str, Any]],
    dates: list[str],
) -> float | None:
    vals: list[float] = []
    for d in dates:
        row = rows_by_date.get(d)
        if not row:
            return None
        v = row.get("net_inflow")
        if v is None:
            return None
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            return None
    return sum(vals) if vals else None


def build_etf_fund_flow_bundle(*, as_of_date: str) -> dict[str, Any]:
    """Build dashboard etfFundFlow block from cached DB rows."""
    ensure_table()
    as_of = str(as_of_date or "").strip() or (get_latest_date() or "")
    if not as_of:
        return {"asOfDate": "", "shareLag": False, "items": []}

    try:
        as_of_d = date.fromisoformat(as_of)
    except ValueError:
        return {"asOfDate": as_of, "shareLag": False, "items": []}

    start = as_of_d - timedelta(days=30)
    open_dates = get_open_dates("SSE", start, as_of_d)
    if not open_dates:
        open_dates = get_open_dates("SZSE", start, as_of_d)
    open_iso = [d.isoformat() for d in open_dates]
    if as_of not in open_iso:
        open_iso = [d for d in open_iso if d <= as_of]
    last_3 = open_iso[-3:] if open_iso else []

    ts_codes = [w["ts_code"] for w in ETF_WATCHLIST]
    rows = fetch_rows_for_codes(ts_codes, end_date=as_of)
    by_code_date: dict[str, dict[str, dict[str, Any]]] = {c: {} for c in ts_codes}
    for row in rows:
        code = str(row.get("ts_code") or "")
        td = str(row.get("trade_date") or "")
        if code and td:
            by_code_date.setdefault(code, {})[td] = row

    share_lag = False
    items: list[dict[str, Any]] = []
    for spec in ETF_WATCHLIST:
        code = spec["ts_code"]
        rows_by_date = by_code_date.get(code, {})
        as_of_row = rows_by_date.get(as_of)
        if as_of_row is None or as_of_row.get("fd_share") is None:
            share_lag = True

        net_1d = None
        if as_of_row is not None:
            net_1d = as_of_row.get("net_inflow")
            if net_1d is not None:
                try:
                    net_1d = float(net_1d)
                except (TypeError, ValueError):
                    net_1d = None

        net_3d = _sum_net_inflow_for_dates(rows_by_date, last_3) if last_3 else None
        signal = classify_signal(
            category=spec["category"],
            net_flow_1d=net_1d,
            net_flow_3d=net_3d,
        )
        items.append(
            {
                "name": spec["name"],
                "symbol": spec["symbol"],
                "tsCode": code,
                "category": spec["category"],
                "netFlow1d": net_1d,
                "netFlow3d": net_3d,
                "signal": signal,
                "signalDisplay": signal_display(signal),
            }
        )

    return {"asOfDate": as_of, "shareLag": share_lag, "items": items}
