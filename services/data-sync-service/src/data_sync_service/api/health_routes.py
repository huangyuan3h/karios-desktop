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
        "weekendTolerant": True,
        "label": "行情",
        "jobType": "stock_close_sync",
        "table": "stock_daily",
        "tableTsColumn": "trade_date",
        "thresholdMinutes": 24 * 60,
    },
    {
        "source": "daily_basic",
        "weekendTolerant": True,
        "group": "twin_star",
        "label": "双子星 · 市值 dailybasic",
        "jobType": "stock_daily_basic_sync",
        "table": "stock_dailybasic",
        "tableTsColumn": "trade_date",
        "thresholdMinutes": 48 * 60,
    },
    {
        # Keep ts_code list in sync with etf_daily.SLEEVE_ETF_TS_CODES.
        "source": "twin_star_etf",
        "weekendTolerant": True,
        "group": "twin_star",
        "label": "双子星 · 核心ETF日线",
        "jobType": "sleeve_etf_daily_sync",
        "table": "daily",
        "tableTsColumn": "trade_date",
        "whereSql": "ts_code IN ('518880.SH','513350.SH','513110.SH','513100.SH','511260.SH')",
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "twin_star_intraday",
        "weekendTolerant": True,
        "group": "twin_star",
        "label": "双子星 · 盘中快照",
        "jobType": "twin_star_intraday",
        "table": None,
        "tableTsColumn": None,
        "resolver": "intraday_snapshot",
        "thresholdMinutes": 20,
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
        "weekendTolerant": True,
        "label": "Watchlist 评分",
        "jobType": "watchlist_automation",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "macro",
        "weekendTolerant": True,
        "label": "宏观",
        "jobType": "macro_daily_full",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "alpha_radar",
        "weekendTolerant": True,
        "label": "Alpha Radar",
        "jobType": "alpha_radar_pipeline",
        "table": None,
        "tableTsColumn": None,
        "thresholdMinutes": 24 * 60,
    },
    # 2026-08-10 (P1-3 staleness): HK strategy-line freshness — HK daily
    # bars, HSI/HSTECH macro, HK score snapshots and CN mainline scores.
    # These lagged for days while uvicorn cached an empty tushare key
    # (hk_basic_sync / macro_daily "false-success"); surfacing them here
    # turns that silent drift into a visible alert.
    {
        "source": "hk_daily",
        "weekendTolerant": True,
        "label": "HK 日线",
        "jobType": "hk_daily_full",
        "table": "daily",
        "tableTsColumn": "trade_date",
        "whereSql": "ts_code LIKE '%.HK'",
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "hk_macro",
        "weekendTolerant": True,
        "label": "HK 指数（HSI/HSTECH）",
        "jobType": "macro_daily_full",
        "table": "macro_daily",
        "tableTsColumn": "trade_date",
        "whereSql": "series_id IN ('HSI','HSTECH')",
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "hk_score",
        "weekendTolerant": True,
        "label": "HK 评分",
        "jobType": "watchlist_automation",
        "table": "watchlist_score_daily",
        "tableTsColumn": "trade_date",
        "whereSql": "symbol LIKE 'HK:%'",
        "thresholdMinutes": 48 * 60,
    },
    {
        "source": "mainline",
        "weekendTolerant": True,
        "label": "主线评分",
        "jobType": "cn_industry_post_close_sync",
        "table": "market_cn_industry_mainline_scores_daily",
        "tableTsColumn": "date",
        "thresholdMinutes": 48 * 60,
    },
)


def _last_table_timestamp(
    table: str | None,
    ts_column: str | None,
    where_sql: str | None = None,
) -> str | None:
    if not table or not ts_column:
        return None
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                where = f" WHERE {where_sql}" if where_sql else ""
                cur.execute(
                    f"SELECT MAX({ts_column}) FROM {table}{where}",
                    None,
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
    # Weekend (Shanghai) has no new data until Monday's close; relax the
    # thresholds so the alert does not cry wolf every Saturday/Sunday.
    weekend_extra_hours = 48 if _is_shanghai_weekend(now) else 0
    sources: list[dict[str, Any]] = []
    for spec in _SOURCES:
        if spec.get("resolver") == "intraday_snapshot":
            sources.append(_intraday_snapshot_source(spec))
            continue
        job_at = _job_last_success(spec["jobType"])
        table_at = _last_table_timestamp(
            spec.get("table"),
            spec.get("tableTsColumn"),
            spec.get("whereSql"),
        )
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
        weekend_bonus = weekend_extra_hours * 60 if spec.get("weekendTolerant") else 0
        threshold = spec["thresholdMinutes"] + weekend_bonus
        sources.append(
            {
                "source": spec["source"],
                "label": spec["label"],
                "group": spec.get("group"),
                "lastSyncedAt": candidate,
                "ageMinutes": age_minutes,
                "thresholdMinutes": threshold,
                "stale": age_minutes is None or age_minutes > threshold,
            }
        )
    return sources


def _intraday_snapshot_source(spec: dict[str, Any]) -> dict[str, Any]:
    """East Money session file: stale only after 12:30 on a trading day."""
    from data_sync_service.service.twin_star_intraday import intraday_snapshot_status

    try:
        status = intraday_snapshot_status()
    except Exception:  # noqa: BLE001
        status = {
            "ok": True,
            "snapshotAt": None,
            "ageSeconds": None,
        }
    age_sec = status.get("ageSeconds")
    return {
        "source": spec["source"],
        "label": spec["label"],
        "group": spec.get("group"),
        "lastSyncedAt": status.get("snapshotAt"),
        "ageMinutes": int(age_sec / 60) if isinstance(age_sec, (int, float)) else None,
        "thresholdMinutes": spec["thresholdMinutes"],
        "stale": not bool(status.get("ok", True)),
    }


def _is_shanghai_weekend(now: datetime) -> bool:
    """True on Sat/Sun OR a calendar non-trading day (holiday-aware freshness relax)."""
    from datetime import timedelta, timezone

    from data_sync_service.service.trade_calendar_utils import is_non_trading_day

    sh = now.astimezone(timezone(timedelta(hours=8)))
    if sh.weekday() >= 5:
        return True
    return is_non_trading_day(sh.date())


def recent_job_failures(hours: int = 24) -> dict[str, Any]:
    """Aggregate sync job failures from the last `hours` (R5 job-failure alerts)."""
    from data_sync_service.db.sync_job_record import list_recent_failures

    recs = list_recent_failures(hours=hours)
    latest_by_job: dict[str, dict[str, Any]] = {}
    for r in recs:
        jt = str(r.get("job_type") or "unknown")
        if jt not in latest_by_job:
            latest_by_job[jt] = r
    failures = [
        {
            "jobType": jt,
            "syncedAt": rec.get("sync_at"),
            "lastTsCode": rec.get("last_ts_code"),
            "errorMessage": rec.get("error_message"),
            # Cap the per-job count: high-frequency manual retries (e.g. an
            # option-IV source with no data) would otherwise flood the alert.
            "failures24h": min(
                sum(1 for r in recs if str(r.get("job_type") or "unknown") == jt),
                10,
            ),
        }
        for jt, rec in latest_by_job.items()
    ]
    return {
        "ok": len(failures) == 0,
        "hours": hours,
        "count": len(failures),
        "failures": failures,
    }


@router.get("/job-failures")
def job_failures_endpoint(hours: int = 24) -> dict[str, Any]:
    """Recent sync job failures (last 24h by default) — surfaced for desktop alerts."""
    return recent_job_failures(hours=hours)


@router.get("/system-events")
def system_events_endpoint(limit: int = 100, include_resolved: bool = False) -> dict[str, Any]:
    from data_sync_service.db.system_events import list_events

    return {"ok": True, "events": list_events(limit=limit, include_resolved=include_resolved)}


@router.post("/system-events/{event_id}/resolve")
def system_events_resolve(event_id: int) -> dict[str, Any]:
    from data_sync_service.db.system_events import resolve_event

    ok = resolve_event(event_id)
    return {"ok": ok}


@router.get("/datasources")
def datasources_endpoint() -> dict[str, Any]:
    """Per-source data freshness for Copy All header (TIP-013)."""
    return {
        "ok": True,
        "generatedAt": datetime.now(UTC).isoformat(),
        "sources": datasource_freshness(),
    }
