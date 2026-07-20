"""Alpha Radar pipeline: RSS ingest, LLM process, and 12h full batch."""

from __future__ import annotations

import json
import math
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from data_sync_service.db.alpha_radar import (
    count_documents_by_status,
    count_trends_total,
    delete_trends_older_than_days,
    delete_trends_since,
    fetch_trends,
    get_meta,
    set_meta,
)
from data_sync_service.db.sync_job_record import get_last_success, get_today_run, insert_record
from data_sync_service.service.alpha_radar_ingest import add_default_sources, fetch_all_sources
from data_sync_service.service.alpha_radar_process import process_pending_documents

JOB_TYPE = "alpha_radar_pipeline"
JOB_TYPE_INGEST = "alpha_radar_ingest"
JOB_TYPE_PROCESS = "alpha_radar_process"
META_LAST_RUN_AT = "last_run_at"
META_LAST_INGEST_AT = "last_ingest_at"
META_LAST_PROCESS_AT = "last_process_at"
META_LAST_BATCH_STARTED_AT = "last_batch_started_at"
META_LAST_TREND_COUNT = "last_trend_count"
META_LAST_INGEST_STATS = "last_ingest_stats"

DEFAULT_COOLDOWN_HOURS = 12
DEFAULT_PROCESS_BATCH_SIZE = 10
DEFAULT_PROCESS_MAX_ROUNDS = 8


def pipeline_cooldown_hours() -> int:
    raw = os.getenv("ALPHA_RADAR_PIPELINE_COOLDOWN_HOURS", str(DEFAULT_COOLDOWN_HOURS)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_COOLDOWN_HOURS


def max_batch_rounds() -> int:
    raw = os.getenv("ALPHA_RADAR_DAILY_BATCH_ROUNDS", "3").strip()
    try:
        return max(1, min(int(raw), 10))
    except ValueError:
        return 3


def process_max_rounds() -> int:
    raw = os.getenv("ALPHA_RADAR_PROCESS_MAX_ROUNDS", str(DEFAULT_PROCESS_MAX_ROUNDS)).strip()
    try:
        return max(1, min(int(raw), 20))
    except ValueError:
        return DEFAULT_PROCESS_MAX_ROUNDS


def process_batch_size() -> int:
    raw = os.getenv("ALPHA_RADAR_PROCESS_BATCH_SIZE", str(DEFAULT_PROCESS_BATCH_SIZE)).strip()
    try:
        return max(2, min(int(raw), 15))
    except ValueError:
        return DEFAULT_PROCESS_BATCH_SIZE


def trend_retention_days() -> int:
    """0 = keep trends forever (default). >0 = optional ops prune after pipeline success."""
    raw = os.getenv("ALPHA_RADAR_TREND_RETENTION_DAYS", "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt
    except ValueError:
        return None


def _within_cooldown(last_run_at: str | None) -> bool:
    last = _parse_iso(last_run_at)
    if not last:
        return False
    return datetime.now(UTC) - last < timedelta(hours=pipeline_cooldown_hours())


def _load_ingest_stats() -> dict[str, Any] | None:
    raw = get_meta(META_LAST_INGEST_STATS)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _rounds_for_raw_backlog(*, raw_count: int, max_rounds: int | None = None) -> int:
    cap = max_rounds if max_rounds is not None else process_max_rounds()
    batch = process_batch_size()
    if raw_count <= 0:
        return 1
    needed = max(1, math.ceil(raw_count / batch))
    return min(cap, needed)


def _run_process_loops(
    *,
    max_rounds: int,
    batch_limit: int | None = None,
) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
    limit = batch_limit or process_batch_size()
    total_processed = 0
    saved_trends: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for _ in range(max_rounds):
        if count_documents_by_status("raw") == 0:
            break
        try:
            batch = process_pending_documents(limit=limit, map_cn=True, mode="batch")
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

    return total_processed, saved_trends, errors


def pipeline_status() -> dict[str, Any]:
    last_at = get_meta(META_LAST_RUN_AT)
    batch_started_at = get_meta(META_LAST_BATCH_STARTED_AT)
    last_trend_count_raw = get_meta(META_LAST_TREND_COUNT)
    try:
        last_trend_count = int(last_trend_count_raw) if last_trend_count_raw else 0
    except ValueError:
        last_trend_count = 0

    current_batch_count = last_trend_count
    if batch_started_at:
        current_batch_count, _ = fetch_trends(limit=1, since=batch_started_at)

    return {
        "lastRunAt": last_at,
        "lastIngestAt": get_meta(META_LAST_INGEST_AT),
        "lastProcessAt": get_meta(META_LAST_PROCESS_AT),
        "lastBatchStartedAt": batch_started_at,
        "lastTrendCount": last_trend_count,
        "currentTrendCount": current_batch_count,
        "accumulatedTrendCount": count_trends_total(),
        "rawBacklogCount": count_documents_by_status("raw"),
        "trendRetentionDays": trend_retention_days(),
        "lastIngestStats": _load_ingest_stats(),
        "withinCooldown": _within_cooldown(last_at),
        "cooldownHours": pipeline_cooldown_hours(),
        "jobType": JOB_TYPE,
        "todayRun": get_today_run(JOB_TYPE),
        "lastSuccess": get_last_success(JOB_TYPE),
    }


def run_alpha_radar_ingest(*, trigger: str = "manual", force_reprocess: bool = False) -> dict[str, Any]:
    add_default_sources()
    ingest_result = fetch_all_sources(
        enrich_fulltext=None,
        apply_filter=True,
        force_reprocess=force_reprocess,
    )
    ingest_stats = ingest_result.get("ingestStats") or {}
    now = datetime.now(UTC).isoformat()
    set_meta(META_LAST_INGEST_AT, now)
    set_meta(META_LAST_INGEST_STATS, json.dumps(ingest_stats, ensure_ascii=False))

    stored = int(ingest_stats.get("stored") or 0)
    source_errors = ingest_result.get("sourceErrors") or {}
    success = stored > 0 or not source_errors
    error_message = None
    if not success and source_errors:
        error_message = f"RSS source errors: {', '.join(source_errors.keys())}"

    insert_record(
        JOB_TYPE_INGEST,
        success=success,
        error_message=error_message,
        last_ts_code=str(stored),
    )

    return {
        "ok": success,
        "trigger": trigger,
        "lastIngestAt": now,
        "ingest": ingest_result,
        "ingestStats": ingest_stats,
        "rawBacklogCount": count_documents_by_status("raw"),
        "errors": [{"error": error_message, "sourceErrors": source_errors}] if error_message else [],
    }


def run_alpha_radar_process(
    *,
    trigger: str = "manual",
    max_rounds: int | None = None,
) -> dict[str, Any]:
    raw_before = count_documents_by_status("raw")
    rounds = _rounds_for_raw_backlog(raw_count=raw_before, max_rounds=max_rounds)
    total_processed, saved_trends, errors = _run_process_loops(max_rounds=rounds)

    now = datetime.now(UTC).isoformat()
    set_meta(META_LAST_PROCESS_AT, now)
    raw_after = count_documents_by_status("raw")

    insert_record(
        JOB_TYPE_PROCESS,
        success=len(errors) == 0,
        error_message=str(errors[0].get("error")) if errors else None,
        last_ts_code=str(total_processed),
    )

    return {
        "ok": len(errors) == 0,
        "trigger": trigger,
        "lastProcessAt": now,
        "processedHeadlines": total_processed,
        "trendsProduced": len(saved_trends),
        "processRounds": rounds,
        "rawBacklogBefore": raw_before,
        "rawBacklogCount": raw_after,
        "errors": errors,
        "accumulatedTrendCount": count_trends_total(),
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
            "accumulatedTrendCount": count_trends_total(),
            "rawBacklogCount": count_documents_by_status("raw"),
        }

    batch_started_at = datetime.now(UTC).isoformat()
    previous_batch_started_at = get_meta(META_LAST_BATCH_STARTED_AT)

    ingest_out = run_alpha_radar_ingest(trigger=trigger)
    ingest_result = ingest_out.get("ingest") or {}
    ingest_stats = ingest_out.get("ingestStats") or {}
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
            "accumulatedTrendCount": count_trends_total(),
            "rawBacklogCount": count_documents_by_status("raw"),
        }

    total_processed, saved_trends, errors = _run_process_loops(max_rounds=max_batch_rounds())

    _, new_trends = fetch_trends(limit=50, since=batch_started_at)
    trend_count = len(new_trends)
    pruned_old = 0

    if trend_count > 0:
        set_meta(META_LAST_BATCH_STARTED_AT, batch_started_at)
        kept_previous = False
        success = True
        error_message = None
        retention = trend_retention_days()
        if retention > 0:
            pruned_old = delete_trends_older_than_days(retention)
    else:
        delete_trends_since(batch_started_at)
        _, new_trends = fetch_trends(
            limit=50,
            since=previous_batch_started_at,
        )
        trend_count = 0
        kept_previous = True
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

    now = datetime.now(UTC).isoformat()
    set_meta(META_LAST_RUN_AT, now)
    set_meta(META_LAST_TREND_COUNT, str(trend_count))

    insert_record(
        JOB_TYPE,
        success=success,
        error_message=error_message,
        last_ts_code=str(trend_count),
    )

    accumulated = count_trends_total()

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
        "prunedOldTrends": pruned_old,
        "accumulatedTrendCount": accumulated,
        "rawBacklogCount": count_documents_by_status("raw"),
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
