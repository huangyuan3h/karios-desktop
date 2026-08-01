"""TradingView screener config and snapshot queries (DB-only; no capture)."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from data_sync_service.config import ROOT_ENV_PATH
from data_sync_service.db import tv as tvdb
from data_sync_service.db import tv_capture_jobs as jobdb
from data_sync_service.service import tv_chrome
from data_sync_service.tv import scanner_api
from data_sync_service.tv.capture import capture_screener_over_cdp_sync

CAPTURE_JOB_DEFAULT_TIMEOUT_S = 180
CAPTURE_VIA_API = "api"
CAPTURE_VIA_EGO_LITE = "ego_lite"
CAPTURE_VIA_CHROME = "chrome"


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def ensure_seeded() -> None:
    """
    Seed default screeners if the table is empty.
    Keep ids stable for compatibility with existing UI/tests.
    """
    if tvdb.count_screeners() > 0:
        return
    ts = _now_iso()
    tvdb.upsert_screener(
        screener_id="falcon",
        name="Legacy Falcon (momentum)",
        url="https://www.tradingview.com/screener/TMcms1mM/",
        enabled=False,
        created_at=ts,
        updated_at=ts,
        mode="chrome",
    )
    tvdb.upsert_screener(
        screener_id="blackhorse",
        name="Legacy Black Horse",
        url="https://www.tradingview.com/screener/kBuKODpK/",
        enabled=False,
        created_at=ts,
        updated_at=ts,
        mode="chrome",
    )


def list_screeners() -> dict[str, Any]:
    ensure_seeded()
    return {"items": tvdb.fetch_screeners()}


def create_screener(
    *,
    name: str,
    url: str = "",
    enabled: bool = True,
    mode: str = "chrome",
    market: str | None = None,
    filter_json: dict[str, Any] | None = None,
    api_columns: list[str] | None = None,
) -> dict[str, str]:
    ensure_seeded()
    screener_id = str(uuid.uuid4())
    ts = _now_iso()
    tvdb.upsert_screener(
        screener_id=screener_id,
        name=(name or "").strip() or "Untitled",
        url=(url or "").strip(),
        enabled=bool(enabled),
        created_at=ts,
        updated_at=ts,
        mode=mode,
        market=market,
        filter_json=filter_json,
        api_columns=api_columns,
    )
    return {"id": screener_id}


def create_screener_from_template(
    *,
    template_id: str,
    enabled: bool = True,
) -> dict[str, str]:
    """Register a screener pre-populated from a built-in template.

    Used by SettingsPage's Template mode: picks the template, derives
    name + market + filter_json + api_columns, and creates a fresh
    screener with mode='api'.
    """
    from data_sync_service.tv.templates import get_template

    ensure_seeded()
    template = get_template(template_id)
    if template is None:
        raise HTTPException(status_code=400, detail=f"unknown template: {template_id}")
    return create_screener(
        name=template.display_name,
        url="",  # API mode doesn't need a URL
        enabled=enabled,
        mode="api",
        market=template.market,
        filter_json=dict(template.filter_json),
        api_columns=list(template.api_columns),
    )


def list_screener_templates() -> dict[str, Any]:
    """Expose built-in templates to the frontend (SettingsPage)."""
    from data_sync_service.tv.templates import list_templates

    items = [
        {
            "templateId": t.template_id,
            "displayName": t.display_name,
            "market": t.market,
            "description": t.description,
            "nestedFilterValidated": t.nested_filter_validated,
            "screenTitleSubstr": t.screen_title_substr,
        }
        for t in list_templates()
    ]
    return {"items": items}


def update_screener(
    *,
    screener_id: str,
    name: str,
    url: str,
    enabled: bool,
    mode: str = "chrome",
    market: str | None = None,
    filter_json: dict[str, Any] | None = None,
    api_columns: list[str] | None = None,
) -> dict[str, bool]:
    ensure_seeded()
    ok = tvdb.update_screener(
        screener_id=(screener_id or "").strip(),
        name=(name or "").strip() or "Untitled",
        url=(url or "").strip(),
        enabled=bool(enabled),
        updated_at=_now_iso(),
        mode=mode,
        market=market,
        filter_json=filter_json,
        api_columns=api_columns,
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Not found")
    return {"ok": True}


def delete_screener(*, screener_id: str) -> dict[str, bool]:
    ensure_seeded()
    ok = tvdb.delete_screener((screener_id or "").strip())
    if not ok:
        raise HTTPException(status_code=404, detail="Not found")
    return {"ok": True}


def list_snapshots(*, screener_id: str, limit: int = 10) -> dict[str, Any]:
    ensure_seeded()
    sid = (screener_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="screener_id is required")
    if tvdb.fetch_screener_by_id(sid) is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    return {"items": tvdb.list_snapshots_for_screener(sid, limit=limit)}


def get_snapshot(*, snapshot_id: str) -> dict[str, Any]:
    ensure_seeded()
    out = tvdb.fetch_snapshot_detail(snapshot_id)
    if out is None:
        raise HTTPException(status_code=404, detail="Not found")
    return out


def list_latest_snapshots_batch(*, screener_ids: list[str]) -> dict[str, Any]:
    ensure_seeded()
    details = tvdb.list_latest_snapshot_details_for_screeners(screener_ids)
    return {"items": details}


def _parse_iso_datetime(value: str) -> datetime | None:
    s = (value or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _tv_local_date_and_slot(captured_at: str) -> tuple[str, str]:
    dt = _parse_iso_datetime(captured_at)
    if dt is None:
        return datetime.now(tz=UTC).date().isoformat(), "unknown"
    try:
        dt2 = dt.astimezone(ZoneInfo("Asia/Shanghai"))
    except Exception:
        dt2 = dt.astimezone(UTC)
    slot = "am" if dt2.hour < 12 else "pm"
    return dt2.date().isoformat(), slot


def screener_history(*, screener_id: str, days: int = 10) -> dict[str, Any]:
    ensure_seeded()
    sid = (screener_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="screener_id is required")
    s = tvdb.fetch_screener_by_id(sid)
    if s is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    days2 = max(1, min(int(days), 30))

    items = tvdb.list_snapshots_for_screener_full(sid, limit=200)
    # Pick latest N distinct local dates present in data.
    dates_desc: list[str] = []
    seen_dates: set[str] = set()
    for it in items:
        local_date, _slot = _tv_local_date_and_slot(str(it.get("capturedAt") or ""))
        if not local_date or local_date in seen_dates:
            continue
        seen_dates.add(local_date)
        dates_desc.append(local_date)
        if len(dates_desc) >= days2:
            break
    if not dates_desc:
        today = datetime.now(tz=UTC).date().isoformat()
        dates_desc = [today]
    keep_dates: set[str] = set(dates_desc)

    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for it in items:
        captured_at = str(it.get("capturedAt") or "")
        local_date, slot = _tv_local_date_and_slot(captured_at)
        if local_date not in keep_dates:
            continue
        by_date.setdefault(local_date, {})[slot] = {
            "snapshotId": str(it.get("snapshotId") or ""),
            "capturedAt": captured_at,
            "rowCount": int(it.get("rowCount") or 0),
            "screenTitle": it.get("screenTitle"),
            "filters": it.get("filters") or [],
        }

    # Keep ascending date in response (consistent UI table).
    dates = sorted(list(keep_dates))
    rows_out: list[dict[str, Any]] = []
    for d in dates:
        cells = by_date.get(d) or {}
        rows_out.append({"date": d, "am": cells.get("am"), "pm": cells.get("pm")})

    return {
        "screenerId": str(s["id"]),
        "screenerName": str(s["name"]),
        "days": days2,
        "rows": rows_out,
    }


def _default_quant_sqlite_path() -> Path:
    # repo root is the parent of .env
    root = ROOT_ENV_PATH.parent
    return root / "services" / "quant-service" / "karios.sqlite3"


def migrate_from_sqlite(*, sqlite_path: str | None = None) -> dict[str, Any]:
    """
    One-shot migration: read quant-service SQLite and upsert into Postgres.
    Idempotent: can be called multiple times.
    """
    ensure_seeded()
    p = Path(sqlite_path).expanduser() if sqlite_path else _default_quant_sqlite_path()
    if not p.exists():
        raise HTTPException(status_code=404, detail=f"SQLite file not found: {p}")

    screener_rows: list[tuple] = []
    snapshot_rows: list[tuple] = []
    with sqlite3.connect(str(p)) as conn:
        try:
            screener_rows = conn.execute(
                """
                SELECT id, name, url, enabled, created_at, updated_at
                FROM tv_screeners
                """,
            ).fetchall()
        except Exception:
            screener_rows = []
        try:
            snapshot_rows = conn.execute(
                """
                SELECT id, screener_id, captured_at, row_count, rows_json
                FROM tv_screener_snapshots
                """,
            ).fetchall()
        except Exception:
            snapshot_rows = []

    upserted_screeners = 0
    for r in screener_rows:
        sid = str(r[0])
        tvdb.upsert_screener(
            screener_id=sid,
            name=str(r[1] or "").strip() or "Untitled",
            url=str(r[2] or "").strip(),
            enabled=bool(int(r[3])) if r[3] is not None else True,
            created_at=str(r[4] or _now_iso()),
            updated_at=str(r[5] or _now_iso()),
        )
        upserted_screeners += 1

    upserted_snapshots = 0
    for r in snapshot_rows:
        snap_id = str(r[0])
        screener_id = str(r[1])
        captured_at = str(r[2] or "")
        row_count = int(r[3] or 0)
        payload_raw = str(r[4] or "{}")
        try:
            payload = json.loads(payload_raw)
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        tvdb.upsert_snapshot(
            snapshot_id=snap_id,
            screener_id=screener_id,
            captured_at=captured_at,
            row_count=row_count,
            payload=payload,
        )
        upserted_snapshots += 1

    return {
        "ok": True,
        "sqlitePath": str(p),
        "screenersUpserted": upserted_screeners,
        "snapshotsUpserted": upserted_snapshots,
    }


def _validate_screener_for_capture(screener_id: str) -> dict[str, Any]:
    ensure_seeded()
    sid = (screener_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="screener_id is required")
    screener = tvdb.fetch_screener_by_id(sid)
    if screener is None:
        raise HTTPException(status_code=404, detail="Screener not found")
    if not screener.get("enabled"):
        raise HTTPException(status_code=409, detail="Screener is disabled")
    url = str(screener.get("url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="Screener URL is empty")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="Screener URL must start with http(s)://")
    return screener


def _ensure_cdp_ready() -> str:
    st = tv_chrome.status()
    if not st.cdpOk:
        src_ud = (tv_chrome.get_setting("tv_bootstrap_src_user_data_dir") or "").strip()
        src_profile = (tv_chrome.get_setting("tv_bootstrap_src_profile_dir") or "").strip()
        desired_profile_dir = src_profile or tv_chrome.TV_PROFILE_DIR_DEFAULT
        tv_chrome.start(
            port=st.port,
            userDataDir=st.userDataDir,
            profileDirectory=desired_profile_dir,
            chromeBin=(tv_chrome.get_setting("tv_chrome_bin") or tv_chrome.TV_CHROME_BIN_DEFAULT),
            headless=True,
            bootstrapFromChromeUserDataDir=src_ud or None,
            bootstrapFromProfileDirectory=src_profile or None,
            forceBootstrap=False,
        )
        st = tv_chrome.status()
        if not st.cdpOk:
            raise HTTPException(
                status_code=409,
                detail=(
                    "CDP is not available. Auto-start failed. "
                    "Please ensure Chrome profile is logged in to TradingView, "
                    "or configure the bootstrap paths in Settings."
                ),
            )
    return f"http://{st.host}:{st.port}"


def _capture_and_persist_screener(*, screener_id: str) -> dict[str, Any]:
    screener = _validate_screener_for_capture(screener_id)
    sid = str(screener.get("id") or "").strip()
    url = str(screener.get("url") or "").strip()
    mode = str(screener.get("mode") or CAPTURE_VIA_CHROME).strip().lower()
    filter_json = screener.get("filterJson")
    api_columns = screener.get("apiColumns")
    market = str(screener.get("market") or "").strip().lower() or None

    result, captured_via = _dispatch_capture(
        mode=mode,
        url=url,
        filter_json=filter_json if isinstance(filter_json, dict) else None,
        api_columns=api_columns if isinstance(api_columns, list) else None,
    )

    snapshot_id = str(uuid.uuid4())
    payload = {
        "screenTitle": result.screen_title,
        "filters": [str(x) for x in (result.filters or []) if str(x).strip()],
        "url": result.url,
        "headers": result.headers,
        "rows": result.rows,
        "capturedVia": captured_via,
    }
    tvdb.upsert_snapshot(
        snapshot_id=snapshot_id,
        screener_id=sid,
        captured_at=result.captured_at,
        row_count=len(result.rows),
        payload=payload,
    )
    return {
        "snapshotId": snapshot_id,
        "capturedAt": result.captured_at,
        "rowCount": len(result.rows),
        "screenerId": sid,
        "capturedVia": captured_via,
    }


def _capture_via_api(
    *,
    filter_json: dict[str, Any],
    api_columns: list[str],
    url: str,
    screen_title: str | None,
    filters: list[str] | None = None,
) -> tuple[Any, str]:
    """Run Scanner API and wrap as a Chrome-CDP-compatible CaptureResult."""
    columns = api_columns or scanner_api.default_columns()
    api_result = scanner_api.fetch_screener_via_api(
        filter_payload=filter_json,
        columns=columns,
    )
    # Map internal column names back to friendly headers (matches
    # ScreenerPage pickColumns contract).
    friendly_headers, friendly_rows = scanner_api.internal_to_friendly_rows(
        api_result.headers, api_result.raw_rows
    )
    title = screen_title or _infer_screen_title_from_url(url) or "TradingView Scanner"
    from data_sync_service.tv.capture import CaptureResult

    cap = CaptureResult(
        url=url,
        captured_at=api_result.captured_at,
        screen_title=title,
        filters=filters or _filters_from_filter_json(filter_json),
        headers=friendly_headers,
        rows=friendly_rows,
    )
    return cap, CAPTURE_VIA_API


def _capture_via_ego_lite(*, url: str) -> tuple[Any, str]:
    """ego-lite fallback (headless chromium, no profile)."""
    from data_sync_service.tv import ego_lite

    try:
        cap = ego_lite.capture_screener_ego_lite_sync(url=url)
        return cap, CAPTURE_VIA_EGO_LITE
    except ego_lite.EgoLiteUnavailable as e:
        # Playwright not installed — re-raise as transient so caller
        # can decide whether to fall back to chrome.
        raise scanner_api.TransientApiError(f"ego_lite_unavailable:{e}") from e


def _capture_via_chrome(*, url: str) -> tuple[Any, str]:
    """Chrome CDP (legacy fallback path)."""
    cdp_url = _ensure_cdp_ready()
    cap = capture_screener_over_cdp_sync(cdp_url=cdp_url, url=url)
    return cap, CAPTURE_VIA_CHROME


def _dispatch_capture(
    *,
    mode: str,
    url: str,
    filter_json: dict[str, Any] | None,
    api_columns: list[str] | None,
) -> tuple[Any, str]:
    """Dispatch a screener capture across the three tracks (OPT-057).

    Fallback chain: api → ego_lite → chrome.
    - `mode='api'`: try Scanner API first; on TransientApiError, fall back
      to ego_lite; on PermanentApiError, surface immediately.
    - `mode='chrome'`: try chrome first; on RuntimeError, surface.
      (Chrome errors are not transient in our model — restart handled by
      `_ensure_cdp_ready` inside `_capture_via_chrome`.)
    """
    if mode == CAPTURE_VIA_API:
        if not filter_json:
            # Backstop: legacy api-mode screener registered without filter.
            raise HTTPException(
                status_code=409,
                detail="Screener mode='api' but filter_json is empty. Pick a template or set filter_json.",
            )
        if not url:
            # API-mode screeners don't need a URL, but downstream code may
            # still expect one; synthesise a stable identifier.
            url = f"scanner_api://{filter_json.get('market') or 'unknown'}"
        try:
            return _capture_via_api(
                filter_json=filter_json,
                api_columns=api_columns or [],
                url=url,
                screen_title=None,
            )
        except scanner_api.TransientApiError as e:
            # Mid-tier fallback: try ego_lite if playwright is available.
            try:
                return _capture_via_ego_lite(url=url)
            except scanner_api.TransientApiError:
                # ego_lite unavailable / also failed → bubble up original error
                raise HTTPException(status_code=502, detail=f"api+ego_lite_failed:{e}") from e
        except scanner_api.PermanentApiError as e:
            raise HTTPException(status_code=422, detail=f"api_permanent:{e}") from e

    # mode='chrome' (default for legacy rows)
    if not url:
        raise HTTPException(
            status_code=409,
            detail="Screener mode='chrome' requires a URL.",
        )
    try:
        return _capture_via_chrome(url=url)
    except Exception as e:  # noqa: BLE001
        msg = str(e) or e.__class__.__name__
        if "Cannot locate screener grid/table" in msg or "TradingView login required" in msg:
            raise HTTPException(status_code=409, detail=msg) from e
        raise HTTPException(status_code=500, detail=msg) from e


def _infer_screen_title_from_url(url: str) -> str | None:
    """Best-effort screen title when one isn't supplied (API mode)."""
    if not url:
        return None
    if url.startswith("scanner_api://"):
        return None
    # TV screener URLs look like https://www.tradingview.com/screener/<id>/
    # The friendly title isn't in the URL; downstream code falls back to
    # screener.name anyway. This helper is mostly for audit logs.
    return None


def _filters_from_filter_json(filter_json: dict[str, Any]) -> list[str]:
    """Convert filter JSON into a human-readable filter pill list (UI display).

    Used by `payload.filters` so the existing UI (ScreenerPage, history view)
    doesn't need to know about the new field. The conversion is intentionally
    lossy — it's for display, not for recomputing the filter.

    The TV Scanner API filter format is `{"and": [<pred>, <pred>, ...]}` or
    `{"or": [...]}`, with the root key being the boolean operator (no
    `operation` field at the root). Predicate nodes have `{left, operation,
    right}` shape.
    """
    out: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for child in node:
                walk(child)
            return
        if not isinstance(node, dict):
            return
        # Root keys: "and" / "or" map to compound operators.
        if "and" in node and isinstance(node["and"], list):
            for child in node["and"]:
                walk(child)
            return
        if "or" in node and isinstance(node["or"], list):
            for child in node["or"]:
                walk(child)
            return
        op = node.get("operation")
        if op in {"greater", "less", "equal", "in_range", "egreater", "eless"}:
            left = node.get("left")
            right = node.get("right")
            if isinstance(right, dict):
                right = _op_str(right)
            out.append(f"{left} {op} {right}")

    walk(filter_json)
    return out


def _op_str(node: dict[str, Any]) -> str:
    op = node.get("operation")
    if op == "mult":
        return f"{node.get('left')}*{node.get('right')}"
    if op == "subtract":
        return f"{node.get('left')}-{node.get('right')}"
    return str(node.get("right"))


def job_to_api(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "jobId": str(job.get("id") or ""),
        "screenerId": str(job.get("screener_id") or ""),
        "status": str(job.get("status") or ""),
        "trigger": str(job.get("trigger_source") or ""),
        "createdAt": job.get("created_at"),
        "startedAt": job.get("started_at"),
        "finishedAt": job.get("finished_at"),
        "snapshotId": job.get("snapshot_id"),
        "rowCount": job.get("row_count"),
        "error": job.get("error_message"),
    }


def enqueue_screener_capture(*, screener_id: str, trigger: str = "api") -> dict[str, Any]:
    _validate_screener_for_capture(screener_id)
    job = jobdb.enqueue_or_get_active(screener_id=screener_id, trigger_source=trigger)
    from data_sync_service.service.tv_capture_worker import wake_tv_capture_worker

    wake_tv_capture_worker()
    return job_to_api(job)


def get_capture_job(job_id: str) -> dict[str, Any]:
    job = jobdb.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Capture job not found")
    return job_to_api(job)


def list_capture_jobs(*, screener_id: str | None = None, limit: int = 20) -> dict[str, Any]:
    items = jobdb.list_jobs(screener_id=screener_id, limit=limit)
    return {"items": [job_to_api(j) for j in items]}


def process_capture_job(job_id: str) -> dict[str, Any]:
    job = jobdb.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Capture job not found")
    sid = str(job.get("screener_id") or "").strip()
    try:
        result = _capture_and_persist_screener(screener_id=sid)
        jobdb.mark_done(
            job_id=job_id,
            snapshot_id=str(result.get("snapshotId") or ""),
            row_count=int(result.get("rowCount") or 0),
        )
        return result
    except HTTPException as exc:
        err = str(exc.detail) if exc.detail is not None else str(exc)
        jobdb.mark_failed(job_id=job_id, error_message=err)
        raise
    except Exception as exc:
        jobdb.mark_failed(job_id=job_id, error_message=str(exc) or exc.__class__.__name__)
        raise


def wait_for_capture_jobs(
    job_ids: list[str],
    *,
    timeout_s: float | None = None,
    poll_s: float = 1.0,
    on_update: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    ids = [str(j or "").strip() for j in job_ids if str(j or "").strip()]
    if not ids:
        return []
    timeout = float(
        timeout_s if timeout_s is not None else CAPTURE_JOB_DEFAULT_TIMEOUT_S * max(len(ids), 1)
    )
    deadline = time.time() + timeout
    last_status: dict[str, str] = {}
    while time.time() < deadline:
        jobs: list[dict[str, Any]] = []
        all_terminal = True
        for jid in ids:
            raw = jobdb.get_job(jid)
            if raw is None:
                raise HTTPException(status_code=404, detail=f"Capture job not found: {jid}")
            api = job_to_api(raw)
            jobs.append(api)
            status = str(api.get("status") or "")
            if on_update is not None and last_status.get(jid) != status:
                last_status[jid] = status
                on_update(api)
            if status not in jobdb.TERMINAL_STATUSES:
                all_terminal = False
        if all_terminal:
            return jobs
        time.sleep(max(0.2, float(poll_s)))
    raise HTTPException(status_code=504, detail="Capture job wait timed out")


def sync_screener(*, screener_id: str) -> dict[str, Any]:
    """Blocking helper for legacy callers; prefer enqueue + wait."""
    out = enqueue_screener_capture(screener_id=screener_id, trigger="api")
    jobs = wait_for_capture_jobs([str(out.get("jobId") or "")], timeout_s=CAPTURE_JOB_DEFAULT_TIMEOUT_S)
    job = jobs[0] if jobs else out
    if str(job.get("status") or "") == "failed":
        raise HTTPException(status_code=500, detail=str(job.get("error") or "capture failed"))
    if str(job.get("status") or "") != "done":
        raise HTTPException(status_code=504, detail="capture did not complete")
    return {
        "snapshotId": job.get("snapshotId"),
        "capturedAt": job.get("finishedAt"),
        "rowCount": int(job.get("rowCount") or 0),
    }

