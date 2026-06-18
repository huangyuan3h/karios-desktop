from __future__ import annotations

import json
import queue
import time
from collections.abc import Callable, Generator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from data_sync_service.db import get_connection
from data_sync_service.db.industry_fund_flow import ensure_table as ensure_industry
from data_sync_service.db.industry_fund_flow import get_dates_upto, get_rows_for_dates
from data_sync_service.db.market_sentiment import get_latest_date as get_latest_sentiment_date
from data_sync_service.db.market_sentiment import list_days as list_sentiment_days
from data_sync_service.db.news import ensure_tables as ensure_news_tables
from data_sync_service.db.news import fetch_items
from data_sync_service.db.tv import list_latest_snapshots_for_screeners
from data_sync_service.service.industry_fund_flow import (
    sync_cn_industry_fund_flow,
)
from data_sync_service.service.industry_fund_flow_read import build_dashboard_industry_bundle
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
from data_sync_service.service.tv import (
    CAPTURE_JOB_DEFAULT_TIMEOUT_S,
    enqueue_screener_capture,
    list_screeners,
    wait_for_capture_jobs,
)

TV_SCREENER_SYNC_MAX_WORKERS = 2


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _today_iso_date() -> str:
    return datetime.now(tz=UTC).date().isoformat()


def _build_industry_bundle(*, as_of_date: str) -> dict[str, Any]:
    """Industry fund-flow block from one batch DB read + in-memory aggregation."""
    ensure_industry()
    dates = get_dates_upto(as_of_date, 5)
    rows = get_rows_for_dates(dates)
    return build_dashboard_industry_bundle(as_of_date=as_of_date, dates=dates, rows=rows)


def _build_market_sentiment_bundle(
    *,
    as_of_date: str,
    use_realtime_index: bool,
    index_signals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    sentiment_items = list_sentiment_days(as_of_date=as_of_date, days=5)
    latest = sentiment_items[-1] if sentiment_items else {}
    down_count = int((latest or {}).get("downCount") or 0)
    sentiment_items = apply_breadth_panic_sentiment_items(sentiment_items, down_count)
    if index_signals is None:
        index_as_of = None if use_realtime_index else as_of_date
        index_signals = get_index_signals(as_of_date=index_as_of, include_breadth=False)
    index_signals = apply_breadth_panic_index_signals(index_signals, down_count)
    return {
        "asOfDate": as_of_date,
        "days": 5,
        "items": sentiment_items,
        "indexSignals": index_signals,
    }


def _shanghai_today_iso() -> str:
    return datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()


def _screeners_status(limit: int = 50) -> list[dict[str, Any]]:
    """
    Return enabled screeners + latest snapshot meta.
    """
    scr = list_screeners()
    items = scr.get("items") if isinstance(scr, dict) else []
    enabled_items: list[tuple[dict[str, Any], str]] = []
    for it in (items if isinstance(items, list) else [])[: max(1, min(int(limit), 200))]:
        if not isinstance(it, dict):
            continue
        if not bool(it.get("enabled")):
            continue
        sid = str(it.get("id") or "").strip()
        if not sid:
            continue
        enabled_items.append((it, sid))

    latest_by_sid = list_latest_snapshots_for_screeners([sid for _, sid in enabled_items])
    rows: list[dict[str, Any]] = []
    for it, sid in enabled_items:
        meta = latest_by_sid.get(sid, {})
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


def dashboard_summary(
    *,
    include_macro: bool = True,
    include_sentiment: bool = True,
    include_news: bool = True,
    include_industry: bool = True,
    include_screeners: bool = True,
) -> dict[str, Any]:
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
    screeners: list[dict[str, Any]] = []
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
        f_screeners = executor.submit(_screeners_status, 50) if include_screeners else None
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
        if f_screeners is not None:
            screeners = f_screeners.result()
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


def _skip_screener_after_close_from_meta(meta: dict[str, Any], today_sh: str) -> tuple[bool, int]:
    captured = str(meta.get("capturedAt") or "")[:10]
    row_count = int(meta.get("rowCount") or 0) if isinstance(meta, dict) else 0
    return captured == today_sh and row_count > 0, row_count


def _should_skip_screener_after_close(*, sid: str, today_sh: str) -> tuple[bool, int]:
    latest_by_sid = list_latest_snapshots_for_screeners([sid])
    return _skip_screener_after_close_from_meta(latest_by_sid.get(sid, {}), today_sh)


def _job_to_screener_result(
    job: dict[str, Any],
    *,
    name: str,
    duration_ms: int,
) -> dict[str, Any]:
    sid = str(job.get("screenerId") or "")
    status = str(job.get("status") or "")
    rc = int(job.get("rowCount") or 0) if job.get("rowCount") is not None else 0
    if status == "done" and rc > 0:
        return {
            "id": sid,
            "name": name,
            "status": "ok",
            "ok": True,
            "rowCount": rc,
            "durationMs": duration_ms,
            "error": None,
            "jobId": job.get("jobId"),
            "jobStatus": status,
        }
    if status == "done":
        return {
            "id": sid,
            "name": name,
            "status": "missing",
            "ok": False,
            "rowCount": rc,
            "durationMs": duration_ms,
            "error": None,
            "jobId": job.get("jobId"),
            "jobStatus": status,
        }
    return {
        "id": sid,
        "name": name,
        "status": "failed",
        "ok": False,
        "rowCount": 0,
        "durationMs": duration_ms,
        "error": str(job.get("error") or status or "capture failed"),
        "jobId": job.get("jobId"),
        "jobStatus": status,
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


def _sync_one_screener(
    sc: dict[str, Any],
    *,
    skip_after_close: bool,
    today_sh: str,
    on_screener_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    sid = str(sc.get("id") or "").strip()
    name = str(sc.get("name") or sid).strip()
    if not sid:
        return {
            "id": "",
            "name": name,
            "status": "failed",
            "ok": False,
            "rowCount": 0,
            "durationMs": 0,
            "error": "missing screener id",
        }

    if skip_after_close:
        should_skip, row_count = _should_skip_screener_after_close(sid=sid, today_sh=today_sh)
        if should_skip:
            return {
                "id": sid,
                "name": name,
                "status": "skipped",
                "ok": True,
                "rowCount": row_count,
                "durationMs": 0,
                "error": None,
            }

    st = time.perf_counter()
    try:
        enqueued = enqueue_screener_capture(screener_id=sid, trigger="dashboard")
        job_id = str(enqueued.get("jobId") or "")
        _emit_screener_progress(
            on_screener_progress,
            _progress_from_job(enqueued, name=name),
        )

        def on_job(job: dict[str, Any]) -> None:
            _emit_screener_progress(on_screener_progress, _progress_from_job(job, name=name))

        jobs = wait_for_capture_jobs(
            [job_id],
            timeout_s=CAPTURE_JOB_DEFAULT_TIMEOUT_S,
            poll_s=1.0,
            on_update=on_job,
        )
        job = jobs[0] if jobs else enqueued
        dur = int((time.perf_counter() - st) * 1000)
        return _job_to_screener_result(job, name=name, duration_ms=dur)
    except HTTPException as exc:
        dur = int((time.perf_counter() - st) * 1000)
        err = str(exc.detail) if exc.detail is not None else str(exc)
        return {
            "id": sid,
            "name": name,
            "status": "failed",
            "ok": False,
            "rowCount": 0,
            "durationMs": dur,
            "error": err,
            "jobStatus": "failed",
        }
    except Exception as exc:
        dur = int((time.perf_counter() - st) * 1000)
        return {
            "id": sid,
            "name": name,
            "status": "failed",
            "ok": False,
            "rowCount": 0,
            "durationMs": dur,
            "error": str(exc) or exc.__class__.__name__,
            "jobStatus": "failed",
        }


def _emit_screener_progress(
    on_screener_progress: Callable[[dict[str, Any]], None] | None,
    result: dict[str, Any],
) -> None:
    if on_screener_progress is not None:
        on_screener_progress(result)


def _sync_screeners_step(
    *,
    screeners_enabled: bool,
    on_screener_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    screener_failed: list[str] = []
    screener_missing: list[str] = []
    screener_skipped: list[str] = []
    screener_results: list[dict[str, Any]] = []
    scr = list_screeners()
    items = scr.get("items") if isinstance(scr, dict) else []
    items_list = items if isinstance(items, list) else []
    enabled = [x for x in items_list if isinstance(x, dict) and bool(x.get("enabled"))]
    if not bool(screeners_enabled):
        return {"enabled": len(enabled), "skipped": True, "failed": 0, "missing": 0}
    skip_after_close = not _is_shanghai_sync_window()
    today_sh = _shanghai_today_iso()
    enabled_sids = [str(sc.get("id") or "").strip() for sc in enabled]
    enabled_sids = [sid for sid in enabled_sids if sid]
    latest_by_sid = (
        list_latest_snapshots_for_screeners(enabled_sids) if skip_after_close else {}
    )
    to_sync: list[dict[str, Any]] = []
    for sc in enabled:
        sid = str(sc.get("id") or "").strip()
        if not sid:
            continue
        if skip_after_close:
            should_skip, row_count = _skip_screener_after_close_from_meta(
                latest_by_sid.get(sid, {}),
                today_sh,
            )
            if should_skip:
                screener_skipped.append(sid)
                result = {
                    "id": sid,
                    "name": str(sc.get("name") or sid).strip(),
                    "status": "skipped",
                    "ok": True,
                    "rowCount": row_count,
                    "durationMs": 0,
                    "error": None,
                }
                screener_results.append(result)
                _emit_screener_progress(on_screener_progress, result)
                continue
        to_sync.append(sc)

    if to_sync:
        with ThreadPoolExecutor(max_workers=TV_SCREENER_SYNC_MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    _sync_one_screener,
                    sc,
                    skip_after_close=False,
                    today_sh=today_sh,
                    on_screener_progress=on_screener_progress,
                )
                for sc in to_sync
            ]
            for future in as_completed(futures):
                result = future.result()
                screener_results.append(result)
                _emit_screener_progress(on_screener_progress, result)
                sid = str(result.get("id") or "").strip()
                status = str(result.get("status") or "")
                if status == "failed" and sid:
                    screener_failed.append(sid)
                elif status == "missing" and sid:
                    screener_missing.append(sid)

    return {
        "enabled": len(enabled),
        "skipped": False,
        "failed": len(screener_failed),
        "missing": len(screener_missing),
        "skippedIds": screener_skipped,
        "failedIds": screener_failed,
        "missingIds": screener_missing,
        "screenerResults": screener_results,
    }


def _run_screeners_step_with_progress(
    *,
    screeners_enabled: bool,
    progress_queue: queue.Queue[dict[str, Any]],
) -> dict[str, Any]:
    def on_progress(evt: dict[str, Any]) -> None:
        progress_queue.put(evt)

    return _run_step(
        "screeners",
        lambda: _sync_screeners_step(
            screeners_enabled=screeners_enabled,
            on_screener_progress=on_progress,
        ),
    )


def _drain_screener_progress_events(q: queue.Queue[dict[str, Any]]) -> Generator[str, None, None]:
    while True:
        try:
            evt = q.get_nowait()
        except queue.Empty:
            break
        yield json.dumps({"type": "screener", "screener": evt}) + "\n"


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
        "news": _sync_news_step,
    }
    steps: list[dict[str, Any]] = []
    screener_meta: dict[str, Any] = {}
    progress_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures: dict[Any, str] = {
            executor.submit(_run_step, name, fn): name for name, fn in step_fns.items()
        }
        if screeners:
            futures[
                executor.submit(
                    _run_screeners_step_with_progress,
                    screeners_enabled=True,
                    progress_queue=progress_queue,
                )
            ] = "screeners"
        else:
            futures[
                executor.submit(
                    _run_step,
                    "screeners",
                    lambda: _sync_screeners_step(screeners_enabled=False),
                )
            ] = "screeners"

        pending = set(futures.keys())
        while pending:
            yield from _drain_screener_progress_events(progress_queue)
            done, pending = wait(pending, timeout=0.3, return_when=FIRST_COMPLETED)
            for future in done:
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

        yield from _drain_screener_progress_events(progress_queue)
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
