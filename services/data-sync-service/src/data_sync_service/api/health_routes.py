"""Data freshness health endpoint (TIP-013).

Reports per-source last-sync freshness so the Copy All payload can carry
visible timestamps instead of relying on faith.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/api/health", tags=["health"])

# (source, job_type, data_table, data_table_ts_column, threshold_minutes)
_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "source": "market",
        "label": "行情",
        "jobType": "stock_close_sync",
        "table": "stock_daily",
        "tableTsColumn": "trade_date",
        "thresholdMinutes": 24 * 60,
    },
    {
        "source": "news",
        "label": "新闻",
        "jobType": "news_fetch_job",
        "table": "news_items",
        "tableTsColumn": "fetched_at",
        "thresholdMinutes": 6 * 60,
    },
    {
        "source": "research",
        "label": "研报",
        "jobType": "research_report_sync",
        "table": "research_reports",
        "tableTsColumn": "created_at",
        "thresholdMinutes": 24 * 60,
    },
    {
        "source": "watchlist",
        "label": "Watchlist 评分",
        "jobType": "watchlist_automation",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "macro",
        "label": "宏观",
        "jobType": "macro_daily_full",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "alpha_radar",
        "label": "Alpha Radar",
        "jobType": "alpha_radar_pipeline",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 24 * 60,
    },
)


def _last_table_timestamp(table: str, ts_column: str) -> str | None:
    if not table or not ts_column:
        return None
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX({ts_column}) FROM {table}"
                )
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        ts = row[0]
        return ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
    except Exception:  # noqa: BLE001
        return None


def _job_last_success(job_type: str) -> str | None:
    from data_sync_service.db.sync_job_record import get_last_success

    rec = get_last_success(job_type)
    if not rec:
        return None
    sync_at = rec.get("sync_at")
    return sync_at.isoformat() if hasattr(sync_at, "isoformat") else sync_at


def _alpha_radar_last_at() -> str | None:
    try:
        from data_sync_service.service.alpha_radar_pipeline import pipeline_status

        status = pipeline_status()
        for key in ("lastFetchAt", "lastProcessedAt", "lastRunAt", "last_sync_at"):
            if status and status.get(key):
                ts = status[key]
                return ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
    except Exception:  # noqa: BLE001
        pass
    return None


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts
    except ValueError:
        return None


def datasource_freshness() -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    sources: list[dict[str, Any]] = []
    for spec in _SOURCES:
        job_at = _job_last_success(spec["jobType"])
        table_at = _last_table_timestamp(spec["table"], spec["tableTsColumn"])
        candidate = max(
            [t for t in (job_at, table_at) if _parse_dt(t) is not None],
            key=lambda t: _parse_dt(t) or now,
            default=None,
        )
        if spec["source"] == "alpha_radar":
            radar_at = _alpha_radar_last_at()
            if _parse_dt(radar_at) and (
                not candidate or (_parse_dt(radar_at) or now) > (_parse_dt(candidate) or now)
            ):
                candidate = radar_at
        last = _parse_dt(candidate)
        age_minutes = int((now - last).total_seconds() / 60) if last else None
        threshold = spec["thresholdMinutes"]
        sources.append(
            {
                "source": spec["source"],
                "label": spec["label"],
                "lastSyncedAt": candidate,
                "ageMinutes": age_minutes,
                "thresholdMinutes": threshold,
                "stale": age_minutes is None or age_minutes > threshold,
            }
        )
    return sources


@router.get("/datasources")
def datasources_endpoint() -> dict[str, Any]:
    """Per-source data freshness for Copy All header (TIP-013)."""
    return {
        "ok": True,
        "generatedAt": datetime.now(UTC).isoformat(),
        "sources": datasource_freshness(),
    }
