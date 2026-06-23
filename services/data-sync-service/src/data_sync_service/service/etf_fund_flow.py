"""ETF fund-flow sync via East Money realtime flows for dashboard watchlist."""

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
from data_sync_service.service.etf_fund_flow_em import (
    EM_ETF_FLOW_SOURCE,
    fetch_em_etf_realtime_flow_for_symbols,
    fetch_em_etf_spot_for_symbols,
    get_last_em_etf_fetch_error,
)
from data_sync_service.db.sync_job_record import get_today_run, insert_record
from data_sync_service.db.trade_calendar import get_open_dates
from data_sync_service.service.market_regime import _is_shanghai_sync_window
from data_sync_service.service.trade_calendar_utils import last_open_date_on_or_before, shanghai_today

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


def _shanghai_today_yyyymmdd() -> str:
    return shanghai_today().strftime("%Y%m%d")


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
        "Data Lag": "⏳ Data Lag",
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
        source = str(row.get("source") or "")
        if source == EM_ETF_FLOW_SOURCE and row.get("main_net_inflow") is not None:
            net = row.get("main_net_inflow")
        else:
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
                "source": row.get("source"),
                "trade_time": row.get("trade_time"),
                "main_net_inflow": row.get("main_net_inflow"),
                "super_large_net_inflow": row.get("super_large_net_inflow"),
                "large_net_inflow": row.get("large_net_inflow"),
                "medium_net_inflow": row.get("medium_net_inflow"),
                "small_net_inflow": row.get("small_net_inflow"),
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


def _should_skip_etf_sync_today(*, force: bool) -> bool:
    """Skip when a successful realtime snapshot already exists outside the active sync window."""
    if force:
        return False
    run = get_today_run(JOB_TYPE)
    if not run or not run.get("success"):
        return False
    return not _is_shanghai_sync_window()


def _em_flow_trade_date(flow: dict[str, Any], *, fallback_iso: str) -> str:
    data_date = str(flow.get("dataDate") or "").strip()
    if data_date:
        try:
            return date.fromisoformat(data_date[:10]).isoformat()
        except ValueError:
            pass
    return fallback_iso


def _is_current_realtime_trade_date(trade_date_iso: str, *, fallback_iso: str) -> bool:
    try:
        trade_date = date.fromisoformat(str(trade_date_iso)[:10])
        fallback_date = date.fromisoformat(str(fallback_iso)[:10])
    except ValueError:
        return trade_date_iso == fallback_iso
    last_open = last_open_date_on_or_before(fallback_date) or fallback_date
    return trade_date >= last_open


def _em_flow_to_daily_row(
    *,
    ts_code: str,
    trade_date_iso: str,
    flow: dict[str, Any],
    updated_at: str,
) -> dict[str, Any] | None:
    main_net = flow.get("mainNetInflow")
    if main_net is None:
        return None
    return {
        "ts_code": ts_code,
        "trade_date": trade_date_iso,
        "fd_share": flow.get("fdShareWan"),
        "close": flow.get("latestPrice"),
        "avg_price": flow.get("latestPrice"),
        "net_inflow": main_net,
        "updated_at": updated_at,
        "source": flow.get("source") or EM_ETF_FLOW_SOURCE,
        "trade_time": flow.get("tradeTime"),
        "main_net_inflow": main_net,
        "super_large_net_inflow": flow.get("superLargeNetInflow"),
        "large_net_inflow": flow.get("largeNetInflow"),
        "medium_net_inflow": flow.get("mediumNetInflow"),
        "small_net_inflow": flow.get("smallNetInflow"),
    }


def _sync_tushare_history_if_available(
    *,
    ts_code: str,
    end_date: str,
    updated_at: str,
) -> int:
    settings = get_settings()
    if not settings.tu_share_api_key:
        return 0
    pro = ts.pro_api(settings.tu_share_api_key)
    last_date = get_last_trade_date(ts_code)
    row_count = len(fetch_rows_for_codes([ts_code]))
    if last_date is None or row_count < 5:
        start_date = FULL_START_DATE
    else:
        start_date = _date_to_yyyymmdd(last_date + timedelta(days=1))
    if start_date > end_date:
        return 0
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
    updated = upsert_daily_rows(merged) if merged else 0
    return updated + _recompute_net_inflows_for_code(ts_code, updated_at=updated_at)


def sync_etf_fund_flow_watchlist(*, force: bool = False) -> dict[str, Any]:
    """Sync hardcoded ETF watchlist with East Money realtime fund-flow as the primary source."""
    if _should_skip_etf_sync_today(force=force):
        return {"ok": True, "skipped": True, "message": "already synced today"}

    ensure_table()
    end_date = _shanghai_today_yyyymmdd()
    end_iso = _yyyymmdd_to_iso(end_date)
    updated_at = _now_iso()
    symbols = [w["symbol"] for w in ETF_WATCHLIST]
    realtime = fetch_em_etf_realtime_flow_for_symbols(symbols)
    fetch_error = get_last_em_etf_fetch_error()
    rows: list[dict[str, Any]] = []
    missing_symbols: list[str] = []
    stale_symbols: list[str] = []

    for spec in ETF_WATCHLIST:
        symbol = spec["symbol"]
        flow = realtime.get(symbol)
        if not flow:
            missing_symbols.append(symbol)
            continue
        trade_date_iso = _em_flow_trade_date(flow, fallback_iso=end_iso)
        if not _is_current_realtime_trade_date(trade_date_iso, fallback_iso=end_iso):
            stale_symbols.append(symbol)
            missing_symbols.append(symbol)
            continue
        row = _em_flow_to_daily_row(
            ts_code=spec["ts_code"],
            trade_date_iso=trade_date_iso,
            flow=flow,
            updated_at=updated_at,
        )
        if row is None:
            missing_symbols.append(symbol)
            continue
        rows.append(row)

    history_rows = 0
    history_error: str | None = None
    try:
        for spec in ETF_WATCHLIST:
            history_rows += _sync_tushare_history_if_available(
                ts_code=spec["ts_code"],
                end_date=end_date,
                updated_at=updated_at,
            )
    except Exception as e:  # noqa: BLE001
        history_error = str(e)

    total_rows = upsert_daily_rows(rows)
    success = bool(rows)
    error_message = None if success else (fetch_error or "no realtime ETF flow rows from East Money")
    insert_record(
        job_type=JOB_TYPE,
        success=success,
        last_ts_code=None,
        error_message=error_message,
    )
    if not success:
        return {
            "ok": False,
            "error": error_message,
            "missingSymbols": missing_symbols,
            "staleSymbols": stale_symbols,
            "historyUpdated": history_rows,
            "historyError": history_error,
        }
    return {
        "ok": True,
        "updated": total_rows,
        "source": EM_ETF_FLOW_SOURCE,
        "missingSymbols": missing_symbols,
        "staleSymbols": stale_symbols,
        "fetchError": fetch_error,
        "historyUpdated": history_rows,
        "historyError": history_error,
    }


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


def _apply_em_spot_fallback(
    *,
    ts_code: str,
    symbol: str,
    trade_date_iso: str,
    rows_by_date: dict[str, dict[str, Any]],
    updated_at: str,
) -> bool:
    """Fill missing fd_share / net_inflow for trade_date_iso from East Money spot."""
    em = fetch_em_etf_spot_for_symbols([symbol]).get(symbol)
    if not em:
        return False
    fd_share_wan = em.get("fdShareWan")
    main_net = em.get("mainNetInflow")
    if fd_share_wan is None and main_net is None:
        return False

    row = dict(rows_by_date.get(trade_date_iso) or {})
    row["ts_code"] = ts_code
    row["trade_date"] = trade_date_iso
    row["updated_at"] = updated_at
    if fd_share_wan is not None:
        row["fd_share"] = fd_share_wan
    if main_net is not None and row.get("net_inflow") is None:
        row["net_inflow"] = main_net
        row["emMainNetInflow"] = True
    rows_by_date[trade_date_iso] = row
    upsert_daily_rows([row])
    return True


def _latest_net_inflow_row(
    rows_by_date: dict[str, dict[str, Any]],
    open_iso: list[str],
    *,
    up_to: str,
) -> tuple[str | None, float | None]:
    """Walk open dates backward from up_to and return latest date with net_inflow."""
    if up_to in open_iso:
        idx = open_iso.index(up_to)
        candidates = open_iso[: idx + 1]
    else:
        candidates = [d for d in open_iso if d <= up_to]
    for td in reversed(candidates):
        row = rows_by_date.get(td)
        if not row:
            continue
        v = row.get("net_inflow")
        if v is None:
            continue
        try:
            return td, float(v)
        except (TypeError, ValueError):
            continue
    return None, None


def _prev_open_iso(open_iso: list[str], as_of: str) -> str | None:
    prior = [d for d in open_iso if d < as_of]
    return prior[-1] if prior else None


def _estimate_net_1d_from_em(
    *,
    symbol: str,
    as_of: str,
    rows_by_date: dict[str, dict[str, Any]],
    open_iso: list[str],
    em_spot: dict[str, dict[str, Any]] | None,
) -> float | None:
    """Best-effort same-day net inflow from East Money spot (read path, no DB write)."""
    em = (em_spot or {}).get(symbol)
    if not em:
        return None
    data_date = em.get("dataDate")
    if data_date and str(data_date) != as_of:
        return None
    main_net = em.get("mainNetInflow")
    if main_net is not None:
        try:
            return float(main_net)
        except (TypeError, ValueError):
            pass
    fd_share_wan = em.get("fdShareWan")
    if fd_share_wan is None:
        return None
    prev_iso = _prev_open_iso(open_iso, as_of)
    if not prev_iso:
        return None
    prev_row = rows_by_date.get(prev_iso) or {}
    as_of_row = rows_by_date.get(as_of) or {}
    avg_price = as_of_row.get("avg_price") or as_of_row.get("close") or prev_row.get("avg_price")
    return compute_net_inflow_1d(
        fd_share_today=fd_share_wan,
        fd_share_prev=prev_row.get("fd_share"),
        avg_price=avg_price,
    )


def build_etf_fund_flow_bundle(*, as_of_date: str) -> dict[str, Any]:
    """Build dashboard etfFundFlow block from cached DB rows."""
    ensure_table()
    as_of = str(as_of_date or "").strip() or (get_latest_date() or "")
    if not as_of:
        return {"asOfDate": "", "shareLag": False, "intradaySafe": True, "items": []}

    try:
        as_of_d = date.fromisoformat(as_of)
    except ValueError:
        return {"asOfDate": as_of, "shareLag": False, "intradaySafe": True, "items": []}

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
    em_spot: dict[str, dict[str, Any]] | None = None
    items: list[dict[str, Any]] = []
    for spec in ETF_WATCHLIST:
        code = spec["ts_code"]
        symbol = spec["symbol"]
        rows_by_date = by_code_date.get(code, {})
        as_of_row = rows_by_date.get(as_of)
        row_source = str((as_of_row or {}).get("source") or "")
        has_realtime_flow = row_source == EM_ETF_FLOW_SOURCE and (as_of_row or {}).get("net_inflow") is not None
        if (as_of_row is None or as_of_row.get("fd_share") is None) and not has_realtime_flow:
            share_lag = True

        net_1d = None
        flow_as_of = as_of
        em_realtime_row: dict[str, Any] | None = None
        if as_of_row is not None:
            net_1d = as_of_row.get("net_inflow")
            if net_1d is not None:
                try:
                    net_1d = float(net_1d)
                except (TypeError, ValueError):
                    net_1d = None

        net_1d_lagged: float | None = None
        if net_1d is None:
            lag_date, lag_net = _latest_net_inflow_row(rows_by_date, open_iso, up_to=as_of)
            if lag_net is not None and lag_date and lag_date != as_of:
                net_1d_lagged = lag_net
                flow_as_of = lag_date

        if net_1d is None and (as_of_row is None or as_of_row.get("fd_share") is None):
            if em_spot is None and _is_shanghai_sync_window():
                symbols = [w["symbol"] for w in ETF_WATCHLIST]
                em_spot = fetch_em_etf_spot_for_symbols(symbols)
            em_net = _estimate_net_1d_from_em(
                symbol=symbol,
                as_of=as_of,
                rows_by_date=rows_by_date,
                open_iso=open_iso,
                em_spot=em_spot,
            )
            if em_net is not None:
                net_1d = em_net
                flow_as_of = as_of
                row_source = EM_ETF_FLOW_SOURCE
                em_realtime_row = (em_spot or {}).get(symbol)

        net_3d = _sum_net_inflow_for_dates(rows_by_date, last_3) if last_3 else None
        if net_3d is None and last_3:
            vals: list[float] = []
            for d in last_3:
                _, v = _latest_net_inflow_row(rows_by_date, open_iso, up_to=d)
                if v is None:
                    vals = []
                    break
                vals.append(v)
            net_3d = sum(vals) if vals else None

        stale_flow = flow_as_of != as_of or (
            as_of_row is not None
            and as_of_row.get("fd_share") is None
            and net_1d is None
            and not has_realtime_flow
        )
        if row_source == EM_ETF_FLOW_SOURCE or (
            net_1d is not None and flow_as_of == as_of and em_spot and em_spot.get(symbol)
        ):
            data_source = EM_ETF_FLOW_SOURCE
        elif as_of_row and as_of_row.get("fd_share") is not None:
            data_source = "tushare"
        elif as_of_row and as_of_row.get("net_inflow") is not None and as_of_row.get("fd_share") is None:
            data_source = "eastmoney"
        elif stale_flow:
            data_source = "tushare"
        else:
            data_source = "mixed" if net_1d is not None else "tushare"

        if net_1d is None:
            signal = "Data Lag"
        else:
            signal = classify_signal(
                category=spec["category"],
                net_flow_1d=net_1d,
                net_flow_3d=net_3d,
            )
        source = data_source
        items.append(
            {
                "name": spec["name"],
                "symbol": symbol,
                "tsCode": code,
                "category": spec["category"],
                "netFlow1d": net_1d,
                "netFlow1dLagged": net_1d_lagged,
                "netFlow3d": net_3d,
                "flowAsOfDate": flow_as_of if flow_as_of != as_of else None,
                "source": source,
                "tradeTime": (as_of_row or {}).get("trade_time") or (em_realtime_row or {}).get("tradeTime"),
                "mainNetInflow": (as_of_row or {}).get("main_net_inflow")
                if (as_of_row or {}).get("main_net_inflow") is not None
                else (em_realtime_row or {}).get("mainNetInflow"),
                "superLargeNetInflow": (as_of_row or {}).get("super_large_net_inflow")
                if (as_of_row or {}).get("super_large_net_inflow") is not None
                else (em_realtime_row or {}).get("superLargeNetInflow"),
                "largeNetInflow": (as_of_row or {}).get("large_net_inflow")
                if (as_of_row or {}).get("large_net_inflow") is not None
                else (em_realtime_row or {}).get("largeNetInflow"),
                "mediumNetInflow": (as_of_row or {}).get("medium_net_inflow")
                if (as_of_row or {}).get("medium_net_inflow") is not None
                else (em_realtime_row or {}).get("mediumNetInflow"),
                "smallNetInflow": (as_of_row or {}).get("small_net_inflow")
                if (as_of_row or {}).get("small_net_inflow") is not None
                else (em_realtime_row or {}).get("smallNetInflow"),
                "signal": signal,
                "signalDisplay": signal_display(signal),
            }
        )

    intraday_safe = not any(str(it.get("signal") or "") == "Data Lag" for it in items)
    return {
        "asOfDate": as_of,
        "shareLag": share_lag,
        "intradaySafe": intraday_safe,
        "items": items,
    }
