from __future__ import annotations

import json
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import get_connection
from data_sync_service.db.industry_fund_flow import ensure_table as ensure_industry
from data_sync_service.db.market_sentiment import get_latest_date as get_latest_sentiment_date
from data_sync_service.db.market_sentiment import list_days as list_sentiment_days
from data_sync_service.db.news import ensure_tables as ensure_news_tables
from data_sync_service.db.news import fetch_items
from data_sync_service.db.tv import list_snapshots_for_screener_full
from data_sync_service.service.industry_fund_flow import (
    sync_cn_industry_fund_flow,
)
from data_sync_service.service.macro_snapshot import build_macro_snapshot
from data_sync_service.service.market_environment_zh import format_market_environment_zh
from data_sync_service.service.market_regime import (
    _is_shanghai_sync_window,
    get_index_signals,
)
from data_sync_service.service.market_sentiment import sync_cn_sentiment
from data_sync_service.service.news import fetch_all_sources
from data_sync_service.service.tv import list_screeners, sync_screener


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_iso_date() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _industry_top_by_date(*, as_of_date: str, days: int = 5, top_k: int = 5) -> dict[str, Any]:
    """
    Return TopK industry names per date for the last N days (<= as_of_date).

    Shape:
      { asOfDate, days, topK, dates, topByDate: [{date, top:[name...]}] }
    """
    ensure_industry()
    days2 = max(1, min(int(days), 30))
    topk2 = max(1, min(int(top_k), 20))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH base AS (
                  SELECT date, industry_name, net_inflow
                  FROM market_cn_industry_fund_flow_daily
                  WHERE date <= %s
                ),
                ranked AS (
                  SELECT
                    date,
                    industry_name,
                    ROW_NUMBER() OVER (PARTITION BY date ORDER BY net_inflow DESC) AS rn
                  FROM base
                ),
                dates AS (
                  SELECT DISTINCT date
                  FROM base
                  ORDER BY date DESC
                  LIMIT %s
                )
                SELECT r.date, r.rn, r.industry_name
                FROM ranked r
                JOIN dates d ON d.date = r.date
                WHERE r.rn <= %s
                ORDER BY r.date ASC, r.rn ASC
                """,
                (as_of_date, days2, topk2),
            )
            rows = cur.fetchall()

    by_date: dict[str, list[str]] = {}
    for r in rows:
        d = str(r[0])
        name = str(r[2] or "")
        if not name:
            continue
        by_date.setdefault(d, []).append(name)
    dates_sorted = sorted(by_date.keys())
    top_by_date = [{"date": d, "top": by_date.get(d, [])[:topk2]} for d in dates_sorted]
    return {
        "asOfDate": as_of_date,
        "days": days2,
        "topK": topk2,
        "dates": dates_sorted,
        "topByDate": top_by_date,
    }


def _industry_flow_5d_items(*, as_of_date: str) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Compute 5D aggregated flow items from DB for the last 5 cached dates (<= as_of_date).
    """
    ensure_industry()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH dates AS (
                  SELECT DISTINCT date
                  FROM market_cn_industry_fund_flow_daily
                  WHERE date <= %s
                  ORDER BY date DESC
                  LIMIT 5
                )
                SELECT d.date, b.industry_code, b.industry_name, b.net_inflow
                FROM market_cn_industry_fund_flow_daily b
                JOIN dates d ON d.date = b.date
                ORDER BY d.date ASC
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()

    dates_sorted: list[str] = sorted({str(r[0]) for r in rows if r and r[0]})
    if not dates_sorted:
        return [], []

    last_date = dates_sorted[-1]
    by_code: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = str(r[0] or "")
        code = str(r[1] or "")
        name = str(r[2] or "")
        try:
            v = float(r[3] or 0.0)
        except Exception:
            v = 0.0
        if not code:
            continue
        rec = by_code.setdefault(code, {"industryCode": code, "industryName": name, "perDate": {}})
        if name and not rec.get("industryName"):
            rec["industryName"] = name
        rec["perDate"][d] = v

    items: list[dict[str, Any]] = []
    for code, rec in by_code.items():
        per: dict[str, float] = rec.get("perDate") or {}
        series = [{"date": d, "netInflow": float(per.get(d, 0.0) or 0.0)} for d in dates_sorted]
        sum5d = 0.0
        for p in series:
            net = p.get("netInflow")
            if isinstance(net, (int, float, str)):
                try:
                    sum5d += float(net)
                except Exception:
                    sum5d += 0.0
            else:
                sum5d += 0.0
        items.append(
            {
                "industryCode": code,
                "industryName": str(rec.get("industryName") or ""),
                "sum5d": sum5d,
                "netInflow": float(per.get(last_date, 0.0) or 0.0),
                "series": series,
            }
        )
    return dates_sorted, items


def _industry_flow_5d(*, as_of_date: str) -> dict[str, Any]:
    """
    Numeric 5D inflow block used by Dashboard under industryFundFlow.flow5d.
    """
    dates_sorted, items = _industry_flow_5d_items(as_of_date=as_of_date)
    if not dates_sorted:
        return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": [], "top": []}
    top_in = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0), reverse=True)[:10]
    return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_in}


def _industry_flow_5d_out(*, as_of_date: str) -> dict[str, Any]:
    """
    5D outflow block used by Dashboard under industryFundFlow.flow5dOut.
    """
    dates_sorted, items = _industry_flow_5d_items(as_of_date=as_of_date)
    if not dates_sorted:
        return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": [], "top": []}
    top_out = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0))[:10]
    return {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_out}


def _build_industry_bundle(*, as_of_date: str) -> dict[str, Any]:
    """Industry fund-flow block; one 5D query for both inflow/outflow tops."""
    industry_daily = _industry_top_by_date(as_of_date=as_of_date, days=5, top_k=5)
    dates_sorted, items = _industry_flow_5d_items(as_of_date=as_of_date)
    if not dates_sorted:
        empty = {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": [], "top": []}
        return {**industry_daily, "flow5d": empty, "flow5dOut": empty}
    top_in = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0), reverse=True)[:10]
    top_out = sorted(items, key=lambda x: float(x.get("sum5d") or 0.0))[:10]
    flow5d = {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_in}
    flow5d_out = {"asOfDate": as_of_date, "days": 5, "topN": 10, "dates": dates_sorted, "top": top_out}
    return {**industry_daily, "flow5d": flow5d, "flow5dOut": flow5d_out}


def _build_market_sentiment_bundle(*, as_of_date: str, use_realtime_index: bool) -> dict[str, Any]:
    sentiment_items = list_sentiment_days(as_of_date=as_of_date, days=5)
    index_as_of = None if use_realtime_index else as_of_date
    return {
        "asOfDate": as_of_date,
        "days": 5,
        "items": sentiment_items,
        "indexSignals": get_index_signals(as_of_date=index_as_of, include_breadth=False),
    }


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def _screeners_status(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return enabled screeners + latest snapshot meta.
    """
    scr = list_screeners()
    items = scr.get("items") if isinstance(scr, dict) else []
    rows: list[dict[str, Any]] = []
    for it in (items if isinstance(items, list) else [])[: max(1, min(int(limit), 200))]:
        if not isinstance(it, dict):
            continue
        if not bool(it.get("enabled")):
            continue
        sid = str(it.get("id") or "").strip()
        if not sid:
            continue
        latest = list_snapshots_for_screener_full(sid, limit=1)
        meta = latest[0] if latest else {}
        filters = meta.get("filters") if isinstance(meta, dict) else []
        filters_count = len(filters) if isinstance(filters, list) else 0
        rows.append(
            {
                "id": sid,
                "name": str(it.get("name") or sid),
                "enabled": True,
                "updatedAt": it.get("updatedAt"),
                "capturedAt": meta.get("capturedAt") if isinstance(meta, dict) else None,
                "rowCount": int(meta.get("rowCount") or 0) if isinstance(meta, dict) else 0,
                "filtersCount": int(filters_count),
            }
        )
    return rows


def _index_signal_items(*, as_of_date: str | None) -> list[dict[str, Any]]:
    """
    Build index traffic-light signals for selected indices using MA20/MA5.
    During sync window, try to use realtime quotes from tushare.
    """
    return get_index_signals(as_of_date=as_of_date, include_breadth=False)


def _news_items(hours: int = 24, limit: int = 50) -> dict[str, Any]:
    """
    Fetch recent news items for the dashboard.
    """
    ensure_news_tables()
    total, items = fetch_items(limit=limit, hours=hours)
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
            }
            for item in items
        ],
    }


def dashboard_summary(*, include_macro: bool = True) -> dict[str, Any]:
    """
    Minimal Dashboard summary for UI:
      - asOfDate
      - industryFundFlow: {dates, topByDate, flow5d}
      - marketSentiment: {asOfDate, days, items, indexSignals}
      - screeners: list
      - news: list
      - marketEnvironmentZh: text
      - macroSnapshot: {cnIndexSignals, macro}
    """
    # Prefer sentiment latest date as asOfDate, otherwise today.
    as_of = get_latest_sentiment_date() or _today_iso_date()
    in_sync_window = _is_shanghai_sync_window()
    use_realtime_index = as_of == _today_iso_date() and in_sync_window

    industry: dict[str, Any] = {}
    market_sentiment: dict[str, Any] = {}
    screeners: list[dict[str, Any]] = []
    news: dict[str, Any] = {"hours": 24, "total": 0, "items": []}
    macro_snapshot = None
    market_env_zh = ""

    with ThreadPoolExecutor(max_workers=5) as executor:
        f_industry = executor.submit(_build_industry_bundle, as_of_date=as_of)
        f_sentiment = executor.submit(
            _build_market_sentiment_bundle,
            as_of_date=as_of,
            use_realtime_index=use_realtime_index,
        )
        f_screeners = executor.submit(_screeners_status, 50)
        f_news = executor.submit(_news_items, 24, 50)
        f_macro = executor.submit(build_macro_snapshot) if include_macro else None

        industry = f_industry.result()
        market_sentiment = f_sentiment.result()
        screeners = f_screeners.result()
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
        "screeners": screeners,
        "news": news,
        "marketEnvironmentZh": market_env_zh,
        "macroSnapshot": macro_snapshot,
        "meta": {
            "inSyncWindow": in_sync_window,
            "useRealtimeIndex": use_realtime_index,
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


def _sync_industry_step() -> dict[str, Any]:
    out = sync_cn_industry_fund_flow(days=10, top_n=10)
    return out if isinstance(out, dict) else {"ok": True}


def _sync_sentiment_step(*, force: bool) -> dict[str, Any]:
    d = datetime.now(tz=UTC).date().isoformat()
    out = sync_cn_sentiment(date_str=d, force=bool(force))
    items = out.get("items") if isinstance(out, dict) else []
    last = items[-1] if isinstance(items, list) and items else {}
    return {
        "asOfDate": out.get("asOfDate") if isinstance(out, dict) else d,
        "riskMode": str((last or {}).get("riskMode") or ""),
        "premium": (last or {}).get("yesterdayLimitUpPremium"),
        "failedRate": (last or {}).get("failedLimitUpRate"),
    }


def _sync_screeners_step(*, screeners_enabled: bool) -> dict[str, Any]:
    screener_failed: list[str] = []
    screener_missing: list[str] = []
    screener_skipped: list[str] = []
    scr = list_screeners()
    items = scr.get("items") if isinstance(scr, dict) else []
    items_list = items if isinstance(items, list) else []
    enabled = [x for x in items_list if isinstance(x, dict) and bool(x.get("enabled"))]
    if not bool(screeners_enabled):
        return {"enabled": len(enabled), "skipped": True, "failed": 0, "missing": 0}
    skip_after_close = not _is_shanghai_sync_window()
    today_sh = _shanghai_today_iso()
    for sc in enabled:
        sid = str(sc.get("id") or "").strip()
        if not sid:
            continue
        if skip_after_close:
            latest = list_snapshots_for_screener_full(sid, limit=1)
            meta = latest[0] if latest else {}
            captured = str(meta.get("capturedAt") or "")[:10]
            row_count = int(meta.get("rowCount") or 0) if isinstance(meta, dict) else 0
            if captured == today_sh and row_count > 0:
                screener_skipped.append(sid)
                continue
        try:
            res = sync_screener(screener_id=sid)
            rc = int(res.get("rowCount") or 0) if isinstance(res, dict) else 0
            if rc <= 0:
                screener_missing.append(sid)
        except Exception:
            screener_failed.append(sid)
    return {
        "enabled": len(enabled),
        "skipped": False,
        "failed": len(screener_failed),
        "missing": len(screener_missing),
        "skippedIds": screener_skipped,
        "failedIds": screener_failed,
        "missingIds": screener_missing,
    }


def _sync_news_step() -> dict[str, Any]:
    results = fetch_all_sources()
    total = sum(v for v in results.values() if v > 0)
    failed = sum(1 for v in results.values() if v < 0)
    return {"total": total, "failed": failed, "sources": len(results)}


def dashboard_sync(*, force: bool = True, screeners: bool = True) -> dict[str, Any]:
    started_at = _now_iso()
    steps: list[dict[str, Any]] = []
    steps.append(_run_step("industryFundFlow", _sync_industry_step))
    steps.append(_run_step("marketSentiment", lambda: _sync_sentiment_step(force=force)))
    screener_result = _run_step("screeners", lambda: _sync_screeners_step(screeners_enabled=screeners))
    steps.append(screener_result)
    steps.append(_run_step("news", _sync_news_step))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    screener_meta = screener_result.get("meta") or {}
    return {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
        "screener": {
            "failed": screener_meta.get("failedIds", []),
            "missing": screener_meta.get("missingIds", []),
        },
    }


def dashboard_sync_parallel(*, force: bool = True, screeners: bool = True) -> dict[str, Any]:
    started_at = _now_iso()
    step_fns = {
        "industryFundFlow": _sync_industry_step,
        "marketSentiment": lambda: _sync_sentiment_step(force=force),
        "screeners": lambda: _sync_screeners_step(screeners_enabled=screeners),
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
    step_order = ["industryFundFlow", "marketSentiment", "screeners", "news"]
    steps.sort(key=lambda s: step_order.index(s.get("name", "")))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    screener_step = next((s for s in steps if s.get("name") == "screeners"), {})
    screener_meta = screener_step.get("meta") or {}
    return {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
        "screener": {
            "failed": screener_meta.get("failedIds", []),
            "missing": screener_meta.get("missingIds", []),
        },
    }


def dashboard_sync_stream(
    *, force: bool = True, screeners: bool = True
) -> Generator[str]:
    started_at = _now_iso()
    yield json.dumps({"type": "start", "startedAt": started_at}) + "\n"
    step_fns = {
        "industryFundFlow": _sync_industry_step,
        "marketSentiment": lambda: _sync_sentiment_step(force=force),
        "screeners": lambda: _sync_screeners_step(screeners_enabled=screeners),
        "news": _sync_news_step,
    }
    steps: list[dict[str, Any]] = []
    screener_meta: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_run_step, name, fn): name for name, fn in step_fns.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                result = future.result()
                steps.append(result)
                yield json.dumps({"type": "step", "step": result}) + "\n"
                if name == "screeners":
                    screener_meta = result.get("meta") or {}
            except Exception as exc:
                result = {"name": name, "ok": False, "durationMs": 0, "message": str(exc), "meta": {}}
                steps.append(result)
                yield json.dumps({"type": "step", "step": result}) + "\n"
    step_order = ["industryFundFlow", "marketSentiment", "screeners", "news"]
    steps.sort(key=lambda s: step_order.index(s.get("name", "")))
    finished_at = _now_iso()
    ok = all(bool(s.get("ok")) for s in steps)
    final = {
        "ok": ok,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "steps": steps,
        "screener": {
            "failed": screener_meta.get("failedIds", []),
            "missing": screener_meta.get("missingIds", []),
        },
    }
    summary_data: dict[str, Any] = {}
    try:
        summary_data = dashboard_summary()
    except Exception:
        pass
    yield json.dumps({"type": "done", "result": final, "summary": summary_data}) + "\n"
