from __future__ import annotations

import json
import threading
import time
from unittest.mock import patch

from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]
from data_sync_service.service import dashboard as dashboard_svc  # type: ignore[import-not-found]


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


def test_sync_screeners_parallel_faster_than_serial(monkeypatch) -> None:
    lock = threading.Lock()
    active = {"count": 0, "max": 0}

    def fake_sync(*, screener_id: str) -> dict:
        _ = screener_id
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        try:
            time.sleep(0.15)
            return {"rowCount": 10, "snapshotId": "s", "capturedAt": "2026-06-18"}
        finally:
            with lock:
                active["count"] -= 1

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("a", "b", "c"))
    monkeypatch.setattr(dashboard_svc, "sync_screener", fake_sync)
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
    def fake_sync(*, screener_id: str) -> dict:
        if screener_id == "bad":
            raise RuntimeError("capture failed")
        return {"rowCount": 5, "snapshotId": "s", "capturedAt": "2026-06-18"}

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("ok", "bad", "ok2"))
    monkeypatch.setattr(dashboard_svc, "sync_screener", fake_sync)
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: True)

    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    assert out["failed"] == 1
    assert out["failedIds"] == ["bad"]
    assert out["missingIds"] == []
    statuses = {r["id"]: r["status"] for r in out["screenerResults"]}
    assert statuses["bad"] == "failed"
    assert statuses["ok"] == "ok"
    assert statuses["ok2"] == "ok"


def test_sync_screeners_skip_after_close(monkeypatch) -> None:
    today = dashboard_svc._shanghai_today_iso()
    sync_calls: list[str] = []

    def fake_sync(*, screener_id: str) -> dict:
        sync_calls.append(screener_id)
        return {"rowCount": 1, "snapshotId": "s", "capturedAt": today}

    def fake_snapshots(sid: str, limit: int = 1) -> list[dict]:
        _ = limit
        if sid == "skip-me":
            return [{"capturedAt": f"{today}T10:00:00", "rowCount": 5}]
        return []

    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("skip-me", "run-me"))
    monkeypatch.setattr(dashboard_svc, "sync_screener", fake_sync)
    monkeypatch.setattr(dashboard_svc, "list_snapshots_for_screener_full", fake_snapshots)
    monkeypatch.setattr(dashboard_svc, "_is_shanghai_sync_window", lambda: False)

    out = dashboard_svc._sync_screeners_step(screeners_enabled=True)
    assert out["skippedIds"] == ["skip-me"]
    assert sync_calls == ["run-me"]
    skipped = next(r for r in out["screenerResults"] if r["id"] == "skip-me")
    assert skipped["status"] == "skipped"


def test_sync_screeners_missing_row_count(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_svc, "list_screeners", lambda: _enabled_screeners("empty"))
    monkeypatch.setattr(
        dashboard_svc,
        "sync_screener",
        lambda *, screener_id: {"rowCount": 0, "snapshotId": "s", "capturedAt": "2026-06-18"},
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
        "sync_screener",
        lambda *, screener_id: {
            "rowCount": 3,
            "snapshotId": "s",
            "capturedAt": "2026-06-18",
        },
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
    assert all(e["screener"]["status"] == "ok" for e in screener_events)


def test_tv_chrome_start_uses_lock() -> None:
    import data_sync_service.service.tv_chrome as tv_chrome  # type: ignore[import-not-found]

    assert isinstance(tv_chrome._start_lock, type(threading.Lock()))

    with patch.object(tv_chrome, "_start_unlocked", return_value=tv_chrome.status()) as unlocked:
        tv_chrome.start()
    unlocked.assert_called_once()
