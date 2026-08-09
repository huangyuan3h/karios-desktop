"""Funnel health monitor — Screener → Watchlist pullback gate watch.

Runs post-close (18:10 Asia/Shanghai, weekdays). Reproduces the FE import
funnel offline from the latest TV snapshots + DB K-lines:

    tv_hit = deduped candidates from enabled screeners' latest snapshots
    pass_pullback = candidates inside the 52W pullback window (K-line based)

Anomaly = the pullback gate produced zero pass-through while TV still hit
candidates (data break like the 2026-08-02+ `High.Interval52Week` empty
column, or an extreme market regime). When the anomaly streak reaches
``ANOMALY_CONSECUTIVE_DAYS`` (3), the run is recorded as a failure so it
surfaces in ``GET /api/health/job-failures``.

Run metrics are stored in the job record (``last_ts_code`` = streak count,
``error_message`` = ``key=value`` metrics) so streaks can be recomputed
offline across days.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from data_sync_service.db.sync_job_record import insert_record, list_recent_runs

logger = logging.getLogger(__name__)

JOB_TYPE = "watchlist_funnel_health"
ANOMALY_CONSECUTIVE_DAYS = 3

_METRIC_RE = re.compile(r"([A-Za-z]+)=(\d+)")


def collect_funnel_metrics() -> dict[str, Any]:
    """Reproduce the FE import funnel from DB state (no browser needed)."""
    from data_sync_service.db.tv import fetch_latest_snapshot_rows, fetch_screeners
    from data_sync_service.service.watchlist_automation import filter_pullback_window

    enabled = [s for s in fetch_screeners() if s.get("enabled")]
    ids = [str(s.get("id") or "").strip() for s in enabled if str(s.get("id") or "").strip()]
    snapshot_rows = fetch_latest_snapshot_rows(ids) if ids else {}

    candidates: list[str] = []
    for sid in ids:
        for r in snapshot_rows.get(sid) or []:
            sym = _normalize_screener_symbol(r.get("Ticker") or r.get("Symbol"))
            if sym:
                candidates.append(sym)
    candidates = list(dict.fromkeys(candidates))
    tv_hit = len(candidates)

    if not candidates:
        return {
            "tvHit": 0,
            "passPullback": 0,
            "missing": 0,
            "fallbackWouldTrigger": True,
            "snapshotCount": len(snapshot_rows),
        }

    res = filter_pullback_window(candidates)
    pass_pullback = sum(1 for r in res.get("results") or [] if r.get("inWindow"))
    missing = sum(1 for r in res.get("results") or [] if r.get("missing"))
    return {
        "tvHit": tv_hit,
        "passPullback": pass_pullback,
        "missing": missing,
        "fallbackWouldTrigger": pass_pullback == 0,
        "snapshotCount": len(snapshot_rows),
    }


def _normalize_screener_symbol(raw: Any) -> str | None:
    s = str(raw or "").strip().upper()
    if not s:
        return None
    if re.fullmatch(r"\d{6}", s):
        return f"CN:{s}"
    if re.fullmatch(r"\d{4,5}", s):
        return f"HK:{s}"
    return None


def _is_anomaly(metrics: dict[str, Any]) -> bool:
    """Zero pullback pass-through while TV still had candidates (or zero hits)."""
    return int(metrics.get("passPullback") or 0) == 0


def _metrics_to_str(metrics: dict[str, Any]) -> str:
    return " ".join(f"{k}={v}" for k, v in metrics.items())


def _metrics_from_str(text: str | None) -> dict[str, int]:
    out: dict[str, int] = {}
    if not text:
        return out
    for k, v in _METRIC_RE.findall(text):
        out[k] = int(v)
    return out


def _streak_from_history(records: list[dict[str, Any]], today_metrics: dict[str, Any]) -> int:
    """Consecutive anomaly days including today (latest record first).

    Records are deduped per calendar day (UTC) so multiple runs of the same
    trading day (scheduled 18:10 + manual) never inflate the streak. Records
    without parseable metrics (e.g. collect errors) break the chain.
    """
    seen_dates: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for rec in records:
        raw = rec.get("sync_at")
        if isinstance(raw, str):
            d = raw[:10]
        elif hasattr(raw, "isoformat"):
            d = str(raw)[:10]
        else:
            continue
        if d in seen_dates:
            continue
        seen_dates.add(d)
        deduped.append(rec)

    streak = 1 if _is_anomaly(today_metrics) else 0
    for rec in deduped:
        if rec.get("success") is False and "metrics:" not in (rec.get("error_message") or ""):
            # Failure without embedded metrics (e.g. collect_error) breaks the chain.
            break
        metrics = _metrics_from_str(rec.get("error_message"))
        if not metrics:
            break
        if _is_anomaly(metrics):
            streak += 1
        else:
            break
    return streak


def check_funnel_health() -> dict[str, Any]:
    """Run today's funnel check, persist a job record, fail on 3+ anomaly days."""
    try:
        metrics = collect_funnel_metrics()
    except Exception as exc:  # noqa: BLE001
        logger.warning("funnel health collect failed: %s", exc)
        insert_record(JOB_TYPE, success=False, error_message=f"collect_error={exc}")
        return {"ok": False, "metrics": {}, "streak": 0, "error": str(exc)}

    history = list_recent_runs(JOB_TYPE, limit=14)
    streak = _streak_from_history(history, metrics)
    message = _metrics_to_str(metrics)
    if streak >= ANOMALY_CONSECUTIVE_DAYS:
        insert_record(
            JOB_TYPE,
            success=False,
            last_ts_code=f"streak:{streak}",
            error_message=(
                f"funnel anomaly {ANOMALY_CONSECUTIVE_DAYS}+ days ({streak}): "
                f"TV hit {metrics['tvHit']} but pullback gate 0 — data break or "
                f"extreme market regime. metrics: {message}"
            ),
        )
        return {
            "ok": False,
            "metrics": metrics,
            "streak": streak,
            "error": f"consecutive anomaly {streak} days",
        }

    insert_record(
        JOB_TYPE,
        success=True,
        last_ts_code=f"streak:{streak}",
        error_message=message,
    )
    return {"ok": True, "metrics": metrics, "streak": streak}
