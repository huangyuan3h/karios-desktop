from __future__ import annotations

import json
import time
from collections.abc import Generator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import UTC, date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db.industry_fund_flow import ensure_table as ensure_industry
from data_sync_service.db.industry_fund_flow import get_dates_upto, get_rows_for_dates
from data_sync_service.db.industry_fund_flow import get_latest_date as get_latest_industry_date
from data_sync_service.db.market_sentiment import (
    get_dates_upto as get_sentiment_dates_upto,
)
from data_sync_service.db.market_sentiment import get_latest_date as get_latest_sentiment_date
from data_sync_service.db.market_sentiment import (
    list_days_for_dates as list_sentiment_days_for_dates,
)
from data_sync_service.db.news import ensure_tables as ensure_news_tables
from data_sync_service.db.news import fetch_items
from data_sync_service.service.etf_fund_flow import (
    build_etf_flow_signal,
    build_etf_fund_flow_bundle,
    sync_etf_fund_flow_watchlist,
)
from data_sync_service.service.execution_gate import compute_execution_gate
from data_sync_service.service.industry_fund_flow import (
    sync_cn_industry_fund_flow,
)
from data_sync_service.service.industry_fund_flow_read import (
    build_dashboard_industry_bundle,
    max_net_inflow_for_date,
    top_by_date_from_rows,
)
from data_sync_service.service.macro_daily import sync_macro_daily_full
from data_sync_service.service.macro_snapshot import build_macro_snapshot
from data_sync_service.service.market_environment_zh import format_market_environment_zh
from data_sync_service.service.market_regime import (
    _is_shanghai_sync_window,
    get_index_signals,
)
from data_sync_service.service.market_sentiment import (
    apply_breadth_panic_index_signals,
    apply_breadth_panic_sentiment_items,
    sync_cn_sentiment,
)
from data_sync_service.service.news import fetch_all_sources
from data_sync_service.service.option_iv import sync_option_iv_daily
from data_sync_service.service.sector_rotation_index import compute_srv_index
from data_sync_service.service.top_inst_flow import sync_top_inst_watchlist
from data_sync_service.service.trade_calendar_utils import (
    compute_market_status,
    previous_open_date,
    resolve_effective_as_of,
    shanghai_today_iso,
    trade_dates_upto,
)

TV_SCREENER_SYNC_MAX_WORKERS = 2


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_iso_date() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _build_industry_bundle(*, as_of_date: str) -> dict[str, Any]:
    """Industry fund-flow block from one batch DB read + in-memory aggregation."""
    ensure_industry()
    dates = trade_dates_upto(as_of_date, 5, fallback_dates_fn=get_dates_upto)
    rows = get_rows_for_dates(dates)
    return build_dashboard_industry_bundle(as_of_date=as_of_date, dates=dates, rows=rows)


def _industry_top_by_date(*, as_of_date: str, days: int = 5) -> list[dict[str, Any]]:
    """Lightweight Top-K-by-date read for SRV (no full industry bundle)."""
    ensure_industry()
    dates = trade_dates_upto(as_of_date, days, fallback_dates_fn=get_dates_upto)
    rows = get_rows_for_dates(dates)
    return top_by_date_from_rows(rows, dates, top_k=5)


def _build_market_sentiment_bundle(
    *,
    as_of_date: str,
    use_realtime_index: bool,
    index_signals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    trade_dates = trade_dates_upto(
        as_of_date,
        5,
        fallback_dates_fn=get_sentiment_dates_upto,
    )
    sentiment_items = list_sentiment_days_for_dates(trade_dates)
    latest = sentiment_items[-1] if sentiment_items else {}
    down_count = int((latest or {}).get("downCount") or 0)
    sentiment_items = apply_breadth_panic_sentiment_items(sentiment_items, down_count)
    if index_signals is None:
        index_as_of = None if use_realtime_index else as_of_date
        index_signals = get_index_signals(as_of_date=index_as_of, include_breadth=False)
    index_signals = apply_breadth_panic_index_signals(index_signals, down_count)
    etf_fund_flow = build_etf_fund_flow_bundle(as_of_date=as_of_date)
    etf_flow_signal = build_etf_flow_signal(as_of_date=as_of_date)
    srv_index = compute_srv_index(
        top_by_date=_industry_top_by_date(as_of_date=as_of_date, days=5),
        as_of_date=as_of_date,
    )
    # V6.3 overflow inputs: max 1D SW L1 inflow + upCount
    ensure_industry()
    flow_dates = trade_dates_upto(as_of_date, 1, fallback_dates_fn=get_dates_upto)
    flow_rows = get_rows_for_dates(flow_dates) if flow_dates else []
    flow_as_of = flow_dates[-1] if flow_dates else as_of_date
    max_inflow_cny, overflow_sector = max_net_inflow_for_date(flow_rows, flow_as_of)
    # Re-read latest after breadth-panic mutation
    latest_after = sentiment_items[-1] if sentiment_items else {}
    up_count = int((latest_after or {}).get("upCount") or (latest or {}).get("upCount") or 0)
    execution_gate = compute_execution_gate(
        index_signals=index_signals,
        down_count=int((latest_after or {}).get("downCount") or down_count or 0),
        risk_mode=str((latest_after or {}).get("riskMode") or "") or None,
        srv_index=srv_index,
        up_count=up_count,
        max_sector_inflow_cny=max_inflow_cny,
        overflow_sector=overflow_sector,
        now=datetime.now(tz=ZoneInfo("Asia/Shanghai")),
        etf_flow_signal=etf_flow_signal,
    )
    return {
        "asOfDate": as_of_date,
        "days": 5,
        "items": sentiment_items,
        "indexSignals": index_signals,
        "etfFundFlow": etf_fund_flow,
        "etfFlowSignal": etf_flow_signal,
        "srvIndex": srv_index,
        "executionGate": execution_gate,
    }


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def _index_signal_items(*, as_of_date: str | None) -> list[dict[str, Any]]:
    """
    Build index traffic-light signals for selected indices using MA20/MA5.
    During sync window, try to use realtime quotes from tushare.
    """
    return get_index_signals(as_of_date=as_of_date, include_breadth=False)


def _news_items(hours: int = 24, limit: int = 50) -> dict[str, Any]:
    """
    Fetch recent news items for the dashboard, sorted by investment relevance.

    Ordering rules (decision-useful first):
      1. Enriched items with importance >= 1, sorted by relevance_score DESC
         (relevance_score already folds in importance + watchlist boost).
      2. Unenriched items (enrichment hasn't run yet) — fallback order by time.
      3. Items with importance = 0 are noise — skipped entirely.

    Returns score fields (`importance`, `relevanceScore`, `actionability`,
    `tickers`) so the frontend can mark watchlist hits and decide which
    to surface as actionable.
    """
    ensure_news_tables()
    total, items = fetch_items(limit=limit * 2, hours=hours)

    enriched: list[dict[str, Any]] = []
    unenriched: list[dict[str, Any]] = []
    for item in items:
        importance = item.get("importance")
        if importance is None:
            unenriched.append(item)
        elif importance >= 1:
            enriched.append(item)
        # importance == 0 → noise, drop

    # Enriched: relevance desc, then importance desc (relevance already
    # includes importance × 15 so this is mostly a tie-breaker).
    enriched.sort(
        key=lambda i: (
            -(int(i.get("relevanceScore") or 0)),
            -(int(i.get("importance") or 0)),
        )
    )

    # Unenriched: backend already returned them newest-first; preserve order.
    combined = (enriched + unenriched)[:limit]

    return {
        "hours": hours,
        "total": total,
        "items": [
            {
                "id": item["id"],
                "sourceId": item["sourceId"],
                "title": item["title"],
                "link": item["link"],
                "publishedAt": item["publishedAt"],
                "importance": item.get("importance"),
                "relevanceScore": item.get("relevanceScore"),
                "actionability": item.get("actionability"),
                "tickers": item.get("tickers") or [],
                "aiSummary": item.get("aiSummary"),
            }
            for item in combined
        ],
    }


def dashboard_summary(
    *,
    include_macro: bool = True,
    include_sentiment: bool = True,
    include_news: bool = True,
    include_industry: bool = True,
) -> dict[str, Any]:
    """
    Minimal Dashboard summary for UI:
      - asOfDate
      - industryFundFlow: {dates, topByDate, flow5d}
      - marketSentiment: {asOfDate, days, items, indexSignals}
      - news: list
      - marketEnvironmentZh: text
      - macroSnapshot: {cnIndexSignals, macro}
    """
    # Prefer sentiment latest date (clamped to last open day), otherwise Shanghai today.
    raw_as_of = get_latest_sentiment_date() or get_latest_industry_date() or shanghai_today_iso()
    as_of = resolve_effective_as_of(raw_as_of)
    market_status = compute_market_status()
    # Pre-market on a trading day: no intraday data exists for today yet, so clamp
    # as_of back to the previous open day to avoid an empty/duplicated "today" column.
    if market_status.get("isPreMarket") and as_of == shanghai_today_iso():
        try:
            today_d = date.fromisoformat(as_of)
            prev = previous_open_date(today_d)
            if prev is not None:
                as_of = prev.isoformat()
        except ValueError:
            pass
    in_sync_window = _is_shanghai_sync_window()
    use_realtime_index = as_of == shanghai_today_iso() and in_sync_window

    if use_realtime_index:
        shared_index_signals = get_index_signals(as_of_date=None, include_breadth=False)
        sentiment_signals_in = shared_index_signals
        macro_signals_in = shared_index_signals
    elif include_sentiment or include_macro:
        shared_index_signals = get_index_signals(as_of_date=as_of, include_breadth=False)
        sentiment_signals_in = shared_index_signals
        macro_signals_in = shared_index_signals
    else:
        sentiment_signals_in = []
        macro_signals_in = []

    industry: dict[str, Any] = {}
    market_sentiment: dict[str, Any] = {}
    news: dict[str, Any] = {"hours": 24, "total": 0, "items": []}
    macro_snapshot = None
    market_env_zh = ""

    with ThreadPoolExecutor(max_workers=5) as executor:
        f_industry = (
            executor.submit(_build_industry_bundle, as_of_date=as_of) if include_industry else None
        )
        f_sentiment = (
            executor.submit(
                _build_market_sentiment_bundle,
                as_of_date=as_of,
                use_realtime_index=use_realtime_index,
                index_signals=sentiment_signals_in,
            )
            if include_sentiment
            else None
        )
        f_news = executor.submit(_news_items, 24, 50) if include_news else None
        f_macro = (
            executor.submit(build_macro_snapshot, cn_index_signals=macro_signals_in)
            if include_macro
            else None
        )

        if f_industry is not None:
            industry = f_industry.result()
        if f_sentiment is not None:
            market_sentiment = f_sentiment.result()
        if f_news is not None:
            news = f_news.result()
        if f_macro is not None:
            try:
                macro_snapshot = f_macro.result()
                market_env_zh = format_market_environment_zh(macro_snapshot)
            except Exception:
                market_env_zh = ""

    return {
        "asOfDate": as_of,
        "industryFundFlow": industry,
        "marketSentiment": market_sentiment,
        "news": news,
        "marketEnvironmentZh": market_env_zh,
        "macroSnapshot": macro_snapshot,
        "meta": {
            "inSyncWindow": in_sync_window,
            "useRealtimeIndex": use_realtime_index,
            "marketStatus": market_status,
        },
    }


def _run_step(name: str, fn: callable) -> dict[str, Any]:
    st = time.perf_counter()
    ok = True
    msg: str | None = None
    meta: dict[str, Any] = {}
    try:
        out = fn()
        if isinstance(out, dict):
            meta = out
    except Exception as exc:
        ok = False
        msg = str(exc)
    dur = int((time.perf_counter() - st) * 1000)
    return {"name": name, "ok": ok, "durationMs": dur, "message": msg, "meta": meta}


def _sync_macro_step() -> dict[str, Any]:
    out = sync_macro_daily_full()
    return out if isinstance(out, dict) else {"ok": True}


def _sync_industry_step(*, force: bool = False) -> dict[str, Any]:
    out = sync_cn_industry_fund_flow(days=10, top_n=10, force=bool(force))
    return out if isinstance(out, dict) else {"ok": True}


def _sync_sentiment_step(*, force: bool) -> dict[str, Any]:
    d = shanghai_today_iso()
    out = sync_cn_sentiment(date_str=d, force=bool(force))
    etf_out = sync_etf_fund_flow_watchlist(force=bool(force))
    top_inst_out = sync_top_inst_watchlist(force=bool(force))
    option_iv_out = sync_option_iv_daily(force=bool(force))
    items = out.get("items") if isinstance(out, dict) else []
    last = items[-1] if isinstance(items, list) and items else {}
    return {
        "asOfDate": out.get("asOfDate") if isinstance(out, dict) else d,
        "riskMode": str((last or {}).get("riskMode") or ""),
        "premium": (last or {}).get("yesterdayLimitUpPremium"),
        "failedRate": (last or {}).get("failedLimitUpRate"),
        "etfFundFlow": etf_out,
        "topInstWatchlist": top_inst_out,
        "optionIvDaily": option_iv_out,
    }


def _progress_from_job(job: dict[str, Any], *, name: str) -> dict[str, Any]:
    sid = str(job.get("screenerId") or "")
    status = str(job.get("status") or "")
    if status == "done":
        rc = int(job.get("rowCount") or 0)
        mapped = "ok" if rc > 0 else "missing"
    elif status == "failed":
        mapped = "failed"
    elif status == "running":
        mapped = "running"
    else:
        mapped = "queued"
    return {
        "id": sid,
        "name": name,
        "status": mapped,
        "ok": mapped in {"ok", "queued", "running"},
        "rowCount": int(job.get("rowCount") or 0) if job.get("rowCount") is not None else 0,
        "durationMs": 0,
        "error": job.get("error"),
        "jobId": job.get("jobId"),
        "jobStatus": status,
    }


def _sync_news_step() -> dict[str, Any]:
    results = fetch_all_sources()
    total = sum(v for v in results.values() if v > 0)
    failed = sum(1 for v in results.values() if v < 0)
    return {"total": total, "failed": failed, "sources": len(results)}


def dashboard_sync(*, force: bool = True) -> dict[str, Any]:
    started_at = _now_iso()
    steps: list[dict[str, Any]] = []
    steps.append(_run_step("industryFundFlow", lambda: _sync_industry_step(force=force)))
    steps.append(_run_step("marketSentiment", lambda: _sync_sentiment_step(force=force)))
    steps.append(_run_step("macroDaily", _sync_macro_step))
    steps.append(_run_step("news", _sync_news_step))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    return {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
    }


def dashboard_sync_parallel(*, force: bool = True) -> dict[str, Any]:
    started_at = _now_iso()
    step_fns = {
        "industryFundFlow": lambda: _sync_industry_step(force=force),
        "marketSentiment": lambda: _sync_sentiment_step(force=force),
        "macroDaily": _sync_macro_step,
        "news": _sync_news_step,
    }
    steps: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_run_step, name, fn): name for name, fn in step_fns.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                steps.append(result)
            except Exception as exc:
                steps.append({"name": name, "ok": False, "durationMs": 0, "message": str(exc), "meta": {}})
    step_order = ["industryFundFlow", "marketSentiment", "macroDaily", "news"]
    steps.sort(key=lambda s: step_order.index(s.get("name", "")))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    return {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
    }


def dashboard_sync_stream(*, force: bool = True) -> Generator[str]:
    started_at = _now_iso()
    yield json.dumps({"type": "start", "startedAt": started_at}) + "\n"
    step_fns = {
        "industryFundFlow": lambda: _sync_industry_step(force=force),
        "marketSentiment": lambda: _sync_sentiment_step(force=force),
        "macroDaily": _sync_macro_step,
        "news": _sync_news_step,
    }
    steps: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures: dict[Any, str] = {
            executor.submit(_run_step, name, fn): name for name, fn in step_fns.items()
        }
        pending = set(futures.keys())
        while pending:
            done, pending = wait(pending, timeout=0.3, return_when=FIRST_COMPLETED)
            for future in done:
                name = futures[future]
                try:
                    result = future.result()
                    steps.append(result)
                    yield json.dumps({"type": "step", "step": result}) + "\n"
                except Exception as exc:
                    result = {"name": name, "ok": False, "durationMs": 0, "message": str(exc), "meta": {}}
                    steps.append(result)
                    yield json.dumps({"type": "step", "step": result}) + "\n"

    step_order = ["industryFundFlow", "marketSentiment", "macroDaily", "news"]
    steps.sort(key=lambda s: step_order.index(s.get("name", "")))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    final = {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
    }
    summary_data: dict[str, Any] = {}
    try:
        summary_data = dashboard_summary()
    except Exception:
        pass
    yield json.dumps({"type": "done", "result": final, "summary": summary_data}) + "\n"
