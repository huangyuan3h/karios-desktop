from __future__ import annotations

import json
import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.db import tv as tvdb  # type: ignore[import-not-found]
from data_sync_service.main import app  # type: ignore[import-not-found]
from data_sync_service.service import dashboard as dashboard_svc  # type: ignore[import-not-found]

pytestmark = pytest.mark.requires_postgres

def _enabled_screeners(*ids: str) -> dict:
    return {
        "items": [
            {
                "id": sid,
                "name": f"Screener {sid}",
                "enabled": True,
                "url": f"https://www.tradingview.com/screener/{sid}/",
            }
            for sid in ids
        ]
    }


def _job(screener_id: str, *, status: str = "done", row_count: int = 10, error: str | None = None) -> dict:
    return {
        "jobId": screener_id,
        "screenerId": screener_id,
        "status": status,
        "rowCount": row_count,
        "snapshotId": "s",
        "error": error,
    }


def test_sync_screeners_parallel_faster_than_serial(monkeypatch) -> None:
    lock = threading.Lock()
    active = {"count": 0, "max": 0}

    def fake_enqueue(*, screener_id: str, trigger: str = "api") -> dict:
        return _job(screener_id, status="queued", row_count=0)

    def fake_wait(
        job_ids: list[str],
        *,
        timeout_s: float | None = None,
        poll_s: float = 1.0,
        on_update=None,
    ) -> list[dict]:
        _ = timeout_s, poll_s, on_update
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        try:
            time.sleep(0.15)
            return [_job(jid, status="done", row_count=10) for jid in job_ids]
        finally:
            with lock:
                active["count"] -= 1

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("a", "b", "c"))
    monkeypatch.setattr(dashboard_svc, "enqueue_screener_capture", fake_enqueue)
    monkeypatch.setattr(dashboard_svc, "wait_for_capture_jobs", fake_wait)
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: True)

    start = time.perf_counter()
    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    elapsed = time.perf_counter() - start

    assert out["failed"] == 0
    assert out["missing"] == 0
    assert len(out["screenerResults"]) == 3
    assert elapsed < 0.42
    assert active["max"] >= 2


def test_sync_screeners_failure_isolation(monkeypatch) -> None:
    def fake_enqueue(*, screener_id: str, trigger: str = "api") -> dict:
        return _job(screener_id, status="queued")

    def fake_wait(
        job_ids: list[str],
        *,
        timeout_s: float | None = None,
        poll_s: float = 1.0,
        on_update=None,
    ) -> list[dict]:
        _ = timeout_s, poll_s, on_update
        out: list[dict] = []
        for jid in job_ids:
            if jid == "bad":
                out.append(_job(jid, status="failed", row_count=0, error="capture failed"))
            else:
                out.append(_job(jid, status="done", row_count=5))
        return out

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("ok", "bad", "ok2"))
    monkeypatch.setattr(dashboard_svc, "enqueue_screener_capture", fake_enqueue)
    monkeypatch.setattr(dashboard_svc, "wait_for_capture_jobs", fake_wait)
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: True)

    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    assert out["failed"] == 1
    assert out["failedIds"] == ["bad"]
    assert out["missingIds"] == []
    statuses = {r["id"]: r["status"] for r in out["screenerResults"]}
    assert statuses["bad"] == "failed"
    assert statuses["ok"] == "ok"
    assert statuses["ok2"] == "ok"


def test_screeners_status_uses_batch_snapshot_read(monkeypatch) -> None:
    batch_calls: list[list[str]] = []

    def fake_list_screeners() -> dict:
        return _enabled_screeners("a", "b", "c")

    def fake_latest(screener_ids: list[str]) -> dict:
        batch_calls.append(list(screener_ids))
        return {
            "a": {
                "capturedAt": "2026-06-18T10:00:00",
                "rowCount": 3,
                "filters": ["f1", "f2"],
            },
            "b": {},
            "c": {
                "capturedAt": "2026-06-17T10:00:00",
                "rowCount": 1,
                "filters": ["x"],
            },
        }

    monkeypatch.setattr(dashboard_svc, "list_screeners", fake_list_screeners)
    monkeypatch.setattr(dashboard_svc, "list_latest_snapshots_for_screeners", fake_latest)

    rows = dashboard_svc._screeners_status(limit=50)
    assert len(batch_calls) == 1
    assert set(batch_calls[0]) == {"a", "b", "c"}
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["filtersCount"] == 2
    assert by_id["a"]["rowCount"] == 3
    assert by_id["a"]["capturedAt"] == "2026-06-18T10:00:00"
    assert by_id["b"]["filtersCount"] == 0
    assert by_id["c"]["filtersCount"] == 1


def test_list_latest_snapshots_for_screeners_empty() -> None:
    assert tvdb.list_latest_snapshots_for_screeners([]) == {}


def test_snapshot_meta_from_row_extracts_filters() -> None:
    meta = tvdb._snapshot_meta_from_row(
        ("snap-1", "falcon", "2026-06-18T12:00:00", 7, {"filters": ["a", " b ", ""], "screenTitle": "X"}),
    )
    assert meta["snapshotId"] == "snap-1"
    assert meta["screenerId"] == "falcon"
    assert meta["filters"] == ["a", " b "]
    assert meta["screenTitle"] == "X"


def test_sync_screeners_skip_after_close(monkeypatch) -> None:
    today = dashboard_svc._shanghai_today_iso()
    enqueue_calls: list[str] = []

    def fake_enqueue(*, screener_id: str, trigger: str = "api") -> dict:
        enqueue_calls.append(screener_id)
        return _job(screener_id, status="queued")

    def fake_latest(screener_ids: list[str]) -> dict:
        _ = screener_ids
        return {
            "skip-me": {"capturedAt": f"{today}T10:00:00", "rowCount": 5},
        }

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("skip-me", "run-me"))
    monkeypatch.setattr(dashboard_svc, "enqueue_screener_capture", fake_enqueue)
    monkeypatch.setattr(
        dashboard_svc,
        "wait_for_capture_jobs",
        lambda job_ids, **_: [_job(j, status="done", row_count=1) for j in job_ids],
    )
    monkeypatch.setattr(dashboard_svc, "list_latest_snapshots_for_screeners", fake_latest)
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: False)

    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    assert out["skippedIds"] == ["skip-me"]
    assert enqueue_calls == ["run-me"]
    skipped = next(r for r in out["screenerResults"] if r["id"] == "skip-me")
    assert skipped["status"] == "skipped"


def test_sync_screeners_missing_row_count(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("empty"))
    monkeypatch.setattr(
        dashboard_svc,
        "enqueue_screener_capture",
        lambda *, screener_id, trigger="api": _job(screener_id, status="queued"),
    )
    monkeypatch.setattr(
        dashboard_svc,
        "wait_for_capture_jobs",
        lambda job_ids, **_: [_job("empty", status="done", row_count=0)],
    )
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: True)

    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    assert out["missing"] == 1
    assert out["missingIds"] == ["empty"]
    assert out["screenerResults"][0]["status"] == "missing"


def test_dashboard_sync_stream_emits_screener_events(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_svc, "_sync_industry_step", lambda: {"ok": True})
    monkeypatch.setattr(dashboard_svc, "_sync_sentiment_step", lambda **_: {"ok": True})
    monkeypatch.setattr(dashboard_svc, "_sync_news_step", lambda: {"total": 0, "failed": 0, "sources": 0})
    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("falcon", "blackhorse"))
    monkeypatch.setattr(
        dashboard_svc,
        "enqueue_screener_capture",
        lambda *, screener_id, trigger="api": _job(screener_id, status="queued"),
    )
    monkeypatch.setattr(
        dashboard_svc,
        "wait_for_capture_jobs",
        lambda job_ids, **_: [_job(j, status="done", row_count=3) for j in job_ids],
    )
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(dashboard_svc, "dashboard_summary", lambda **_: {"asOfDate": "2026-06-18"})

    client = TestClient(app)
    events: list[dict] = []
    with client.stream("GET", "/dashboard/sync/stream?force=true&screeners=true") as resp:
        assert resp.status_code == 200
        for line in resp.iter_lines():
            if not line.startswith("data: "):
                continue
            events.append(json.loads(line[6:]))

    types = [e["type"] for e in events]
    assert types[0] == "start"
    assert types[-1] == "done"
    assert "screener" in types
    assert "step" in types
    screener_events = [e for e in events if e["type"] == "screener"]
    assert len(screener_events) >= 2
    final_status = {}
    for e in screener_events:
        final_status[e["screener"]["id"]] = e["screener"]["status"]
    assert final_status.get("falcon") == "ok"
    assert final_status.get("blackhorse") == "ok"


def test_tv_chrome_start_uses_lock() -> None:
    import data_sync_service.service.tv_chrome as tv_chrome  # type: ignore[import-not-found]

    assert isinstance(tv_chrome._start_lock, type(threading.Lock()))

    with patch.object(tv_chrome, "_start_unlocked", return_value=tv_chrome.status()) as unlocked:
        tv_chrome.start()
    unlocked.assert_called_once()
