"""twin_star_reminder_job tests (OPT-139) — records on every path, pure unit."""

from __future__ import annotations

import pytest

from data_sync_service.scheduler import twin_star_reminder_job as job


@pytest.fixture(autouse=True)
def _no_real_records(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda *a, **k: None,
    )


def _patch_job(monkeypatch: pytest.MonkeyPatch, *, sat="sat", payload="payload", emit=None):
    monkeypatch.setattr(job, "build_intraday_sat", lambda today: sat)
    monkeypatch.setattr(job, "cache_intraday_sat", lambda s, t: None)
    monkeypatch.setattr(job, "build_twin_star_reminder_payload", lambda today: payload)
    calls: list[dict] = []
    if emit is None:
        def emit(event_type, payload, dedupe_key):
            calls.append({"type": event_type, "key": dedupe_key})
            return True
    monkeypatch.setattr(job, "emit_event", emit)
    return calls


def _records(monkeypatch: pytest.MonkeyPatch):
    rows: list[tuple] = []
    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: rows.append(
            (jt, success, last_ts_code, error_message)
        ),
    )
    return rows


def test_emit_path_records_success(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.service.twin_star_daily import now_cn

    day = now_cn().date().isoformat()
    calls = _patch_job(
        monkeypatch,
        sat={"gateOpen": True, "candidates": [{"ts": "000001.SZ"}]},
        payload={"detail": "some detail"},
    )
    rows = _records(monkeypatch)
    job.run()
    assert len(calls) == 1 and calls[0]["type"] == "twin_star_reminder"
    assert rows == [("twin_star_reminder", True, day, None)]


def test_no_detail_records_success_without_emit(monkeypatch: pytest.MonkeyPatch) -> None:
    from data_sync_service.service.twin_star_daily import now_cn

    day = now_cn().date().isoformat()
    calls = _patch_job(monkeypatch, payload={"detail": ""})
    rows = _records(monkeypatch)
    job.run()
    assert calls == []
    assert rows == [("twin_star_reminder", True, f"{day}|no-detail", None)]


def test_emit_failure_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(event_type, payload, dedupe_key):
        raise RuntimeError("bark down")

    _patch_job(monkeypatch, payload={"detail": "x"}, emit=boom)
    rows = _records(monkeypatch)
    job.run()
    assert len(rows) == 1
    assert rows[0][0] == "twin_star_reminder" and rows[0][1] is False
    assert "bark down" in (rows[0][3] or "")


def test_payload_build_failure_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "build_intraday_sat", lambda today: None)
    monkeypatch.setattr(job, "cache_intraday_sat", lambda s, t: None)

    def boom(today):
        raise RuntimeError("db down")

    monkeypatch.setattr(job, "build_twin_star_reminder_payload", boom)
    rows = _records(monkeypatch)
    job.run()  # must not raise
    assert len(rows) == 1 and rows[0][1] is False
