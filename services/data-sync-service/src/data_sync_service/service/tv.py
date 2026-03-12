"""TradingView screener config and snapshot queries (DB-only; no capture)."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from data_sync_service.config import ROOT_ENV_PATH
from data_sync_service.db import tv as tvdb
from data_sync_service.service import tv_chrome
from data_sync_service.tv.capture import capture_screener_over_cdp_sync


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
        name="Swing Falcon Filter",
        url="https://www.tradingview.com/screener/TMcms1mM/",
        enabled=True,
        created_at=ts,
        updated_at=ts,
    )
    tvdb.upsert_screener(
        screener_id="blackhorse",
        name="Black Horse Filter",
        url="https://www.tradingview.com/screener/kBuKODpK/",
        enabled=True,
        created_at=ts,
        updated_at=ts,
    )


def list_screeners() -> dict[str, Any]:
    ensure_seeded()
    return {"items": tvdb.fetch_screeners()}


def create_screener(*, name: str, url: str, enabled: bool = True) -> dict[str, str]:
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
    )
    return {"id": screener_id}


def update_screener(*, screener_id: str, name: str, url: str, enabled: bool) -> dict[str, bool]:
    ensure_seeded()
    ok = tvdb.update_screener(
        screener_id=(screener_id or "").strip(),
        name=(name or "").strip() or "Untitled",
        url=(url or "").strip(),
        enabled=bool(enabled),
        updated_at=_now_iso(),
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


def sync_screener(*, screener_id: str) -> dict[str, Any]:
    """
    Capture a fresh snapshot for a screener via CDP-attached Chrome and persist to Postgres.
    """
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

    st = tv_chrome.status()
    if not st.cdpOk:
        # Auto-start a headless Chrome for silent sync (same behavior as quant-service).
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

    cdp_url = f"http://{st.host}:{st.port}"
    try:
        result = capture_screener_over_cdp_sync(cdp_url=cdp_url, url=url)
    except Exception as e:  # noqa: BLE001
        # Avoid unhandled exceptions (which bypass CORS due to ServerErrorMiddleware).
        msg = str(e) or e.__class__.__name__
        if "Cannot locate screener grid/table" in msg or "TradingView login required" in msg:
            raise HTTPException(status_code=409, detail=msg) from e
        raise HTTPException(status_code=500, detail=msg) from e

    snapshot_id = str(uuid.uuid4())
    payload = {
        "screenTitle": result.screen_title,
        "filters": [str(x) for x in (result.filters or []) if str(x).strip()],
        "url": result.url,
        "headers": result.headers,
        "rows": result.rows,
    }
    tvdb.upsert_snapshot(
        snapshot_id=snapshot_id,
        screener_id=sid,
        captured_at=result.captured_at,
        row_count=len(result.rows),
        payload=payload,
    )
    return {"snapshotId": snapshot_id, "capturedAt": result.captured_at, "rowCount": len(result.rows)}

