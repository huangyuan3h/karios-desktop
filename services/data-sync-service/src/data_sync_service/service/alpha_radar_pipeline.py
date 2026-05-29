"""Alpha Radar 12h full pipeline: RSS -> filter -> fulltext -> LLM -> A-share map."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from data_sync_service.db.alpha_radar import (
    delete_trends_before,
    delete_trends_since,
    fetch_trends,
    get_meta,
    set_meta,
)
from data_sync_service.db.sync_job_record import get_last_success, get_today_run, insert_record
from data_sync_service.service.alpha_radar_ingest import add_default_sources, fetch_all_sources
from data_sync_service.service.alpha_radar_process import process_pending_documents

JOB_TYPE = "alpha_radar_pipeline"
META_LAST_RUN_AT = "last_run_at"
META_LAST_BATCH_STARTED_AT = "last_batch_started_at"
META_LAST_TREND_COUNT = "last_trend_count"
META_LAST_INGEST_STATS = "last_ingest_stats"
COOLDOWN_HOURS = 12


def max_batch_rounds() -> int:
    raw = os.getenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "3").strip()
    try:
        return max(1, min(int(raw), 10))
    except ValueError:
        return 3


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _within_cooldown(last_run_at: str | None) -> bool:
    last = _parse_iso(last_run_at)
    if not last:
        return False
    return datetime.now(timezone.utc) - last < timedelta(hours=COOLDOWN_HOURS)


def _load_ingest_stats() -> dict[str, Any] | None:
    raw = get_meta(META_LAST_INGEST_STATS)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def pipeline_status() -> dict[str, Any]:
    last_at = get_meta(META_LAST_RUN_AT)
    batch_started_at = get_meta(META_LAST_BATCH_STARTED_AT)
    last_trend_count_raw = get_meta(META_LAST_TREND_COUNT)
    try:
        last_trend_count = int(last_trend_count_raw) if last_trend_count_raw else 0
    except ValueError:
        last_trend_count = 0

    trend_total = last_trend_count
    if batch_started_at:
        trend_total, _ = fetch_trends(limit=1, since=batch_started_at)

    return {
        "lastRunAt": last_at,
        "lastBatchStartedAt": batch_started_at,
        "lastTrendCount": last_trend_count,
        "currentTrendCount": trend_total,
        "lastIngestStats": _load_ingest_stats(),
        "withinCooldown": _within_cooldown(last_at),
        "cooldownHours": COOLDOWN_HOURS,
        "jobType": JOB_TYPE,
        "todayRun": get_today_run(JOB_TYPE),
        "lastSuccess": get_last_success(JOB_TYPE),
    }


def run_alpha_radar_pipeline(*, force: bool = False, trigger: str = "manual") -> dict[str, Any]:
    last_at = get_meta(META_LAST_RUN_AT)
    if not force and _within_cooldown(last_at):
        _, trends = fetch_trends(limit=50, since=get_meta(META_LAST_BATCH_STARTED_AT))
        return {
            "skipped": True,
            "trigger": trigger,
            "lastRunAt": last_at,
            "trendCount": len(trends),
            "trends": trends,
            "ingest": None,
            "ingestStats": _load_ingest_stats(),
            "processedHeadlines": 0,
            "errors": [],
            "keptPreviousTrends": True,
        }

    batch_started_at = datetime.now(timezone.utc).isoformat()
    set_meta(META_LAST_BATCH_STARTED_AT, batch_started_at)

    add_default_sources()
    ingest_result = fetch_all_sources(enrich_fulltext=None, apply_filter=True)
    ingest_stats = ingest_result.get("ingestStats") or {}
    stored = int(ingest_stats.get("stored") or 0)

    if stored == 0:
        msg = "No documents stored after RSS sync/filter"
        source_errors = ingest_result.get("sourceErrors") or {}
        if source_errors:
            msg += f"; source errors: {', '.join(source_errors.keys())}"
        insert_record(JOB_TYPE, success=False, error_message=msg)
        _, trends = fetch_trends(limit=50)
        return {
            "skipped": False,
            "ok": False,
            "trigger": trigger,
            "lastRunAt": last_at,
            "trendCount": len(trends),
            "trends": trends,
            "ingest": ingest_result,
            "ingestStats": ingest_stats,
            "processedHeadlines": 0,
            "errors": [{"error": msg, "sourceErrors": source_errors}],
            "keptPreviousTrends": True,
        }

    total_processed = 0
    saved_trends: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for _ in range(max_batch_rounds()):
        try:
            batch = process_pending_documents(limit=10, map_cn=True, mode="batch")
        except Exception as exc:
            errors.append({"error": str(exc)})
            break

        processed = int(batch.get("processed") or 0)
        trends = batch.get("trends") or []
        total_processed += processed
        saved_trends.extend(trends)

        for err in batch.get("errors") or []:
            if isinstance(err, dict):
                errors.append(err)
            else:
                errors.append({"error": str(err)})

        if processed < 2:
            break

    _, new_trends = fetch_trends(limit=50, since=batch_started_at)
    trend_count = len(new_trends)

    if trend_count > 0:
        removed_old = delete_trends_before(batch_started_at)
        kept_previous = False
        success = True
        error_message = None
    else:
        delete_trends_since(batch_started_at)
        _, new_trends = fetch_trends(limit=50)
        trend_count = len(new_trends)
        kept_previous = True
        removed_old = 0
        success = False
        if errors:
            error_message = str(errors[0].get("error") or "LLM produced 0 trends; kept previous cards")
            for err in errors:
                msg = str(err.get("error") or "")
                if "ai-service" in msg or "LLM returned" in msg:
                    error_message = msg
                    break
        else:
            error_message = "LLM produced 0 trends; kept previous cards"
            errors.append({"error": error_message})

    now = datetime.now(timezone.utc).isoformat()
    set_meta(META_LAST_RUN_AT, now)
    set_meta(META_LAST_TREND_COUNT, str(trend_count))
    set_meta(META_LAST_INGEST_STATS, json.dumps(ingest_stats, ensure_ascii=False))

    insert_record(
        JOB_TYPE,
        success=success,
        error_message=error_message,
        last_ts_code=str(trend_count),
    )

    return {
        "skipped": False,
        "ok": success,
        "trigger": trigger,
        "lastRunAt": now,
        "lastBatchStartedAt": batch_started_at,
        "trendCount": trend_count,
        "trends": new_trends if trend_count else saved_trends,
        "ingest": ingest_result,
        "ingestStats": ingest_stats,
        "processedHeadlines": total_processed,
        "removedOldTrends": removed_old,
        "errors": errors,
        "keptPreviousTrends": kept_previous,
    }


# Backward-compatible alias used by older imports.
def run_daily_generation(*, force: bool = False, enrich_fulltext: bool = True) -> dict[str, Any]:
    del enrich_fulltext
    return run_alpha_radar_pipeline(force=force, trigger="manual")


def daily_status() -> dict[str, Any]:
    status = pipeline_status()
    return {
        **status,
        "generatedToday": bool(status.get("lastRunAt")),
    }
