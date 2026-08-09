"""job-failure alert endpoint (R5)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)


def test_job_failures_endpoint_ok_when_no_failures(monkeypatch) -> None:
    from data_sync_service.db import sync_job_record

    monkeypatch.setattr(sync_job_record, "list_recent_failures", lambda hours=24: [])
    resp = client.get("/api/health/job-failures")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["count"] == 0
    assert payload["hours"] == 24
    assert payload["failures"] == []


def test_job_failures_endpoint_aggregates_by_job(monkeypatch) -> None:
    from data_sync_service.db import sync_job_record

    recs = [
        {"job_type": "stock_close_sync", "sync_at": "2026-08-07T09:05:00+00:00", "last_ts_code": "CN:600519", "error_message": "tushare quota"},
        {"job_type": "stock_close_sync", "sync_at": "2026-08-07T08:00:00+00:00", "last_ts_code": "CN:000858", "error_message": "timeout"},
        {"job_type": "news_fetch_job", "sync_at": "2026-08-07T07:30:00+00:00", "last_ts_code": None, "error_message": "conn refused"},
    ]
    monkeypatch.setattr(sync_job_record, "list_recent_failures", lambda hours=24: recs)
    resp = client.get("/api/health/job-failures")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is False
    assert payload["count"] == 2
    by_job = {f["jobType"]: f for f in payload["failures"]}
    assert by_job["stock_close_sync"]["failures24h"] == 2
    assert by_job["stock_close_sync"]["syncedAt"] == "2026-08-07T09:05:00+00:00"
    assert by_job["stock_close_sync"]["lastTsCode"] == "CN:600519"
    assert by_job["news_fetch_job"]["failures24h"] == 1
    assert by_job["news_fetch_job"]["errorMessage"] == "conn refused"


def test_job_failures_endpoint_custom_hours(monkeypatch) -> None:
    from data_sync_service.db import sync_job_record

    captured: dict[str, int] = {}
    monkeypatch.setattr(
        sync_job_record,
        "list_recent_failures",
        lambda hours=24: (captured.setdefault("h", hours), [])[1],
    )
    client.get("/api/health/job-failures", params={"hours": 6})
    assert captured["h"] == 6
