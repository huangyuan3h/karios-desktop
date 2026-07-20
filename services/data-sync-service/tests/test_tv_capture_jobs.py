from __future__ import annotations

import os
import threading
import time

import pytest

from data_sync_service.db import tv_capture_jobs as jobdb  # type: ignore[import-not-found]
from data_sync_service.service import tv as tvsvc  # type: ignore[import-not-found]


def _postgres_available() -> bool:
    from data_sync_service.db import check_db  # type: ignore[import-not-found]

    if os.getenv("SKIP_DB_TESTS", "").lower() in {"1", "true", "yes"}:
        return False
    ok, _ = check_db()
    return ok


def test_job_to_api_shape() -> None:
    api = tvsvc.job_to_api(
        {
            "id": "j1",
            "screener_id": "falcon",
            "status": "queued",
            "trigger_source": "api",
            "created_at": "2026-06-18T00:00:00+00:00",
            "started_at": None,
            "finished_at": None,
            "snapshot_id": None,
            "row_count": None,
            "error_message": None,
        }
    )
    assert api["jobId"] == "j1"
    assert api["screenerId"] == "falcon"
    assert api["status"] == "queued"
    assert api["trigger"] == "api"


def test_enqueue_dedupes_active_job(monkeypatch) -> None:
    calls: list[str] = []

    def fake_find(sid: str) -> dict | None:
        if sid == "falcon":
            return {"id": "existing", "screener_id": sid, "status": "running", "trigger_source": "api"}
        return None

    def fake_insert(*, screener_id: str, trigger_source: str = "api") -> dict:
        calls.append(screener_id)
        return {"id": "new", "screener_id": screener_id, "status": "queued", "trigger_source": trigger_source}

    monkeypatch.setattr(tvsvc, "_validate_screener_for_capture", lambda sid: {"id": sid, "enabled": True, "url": "http://x"})
    monkeypatch.setattr(jobdb, "find_active_job_for_screener", fake_find)
    monkeypatch.setattr(jobdb, "insert_job", fake_insert)
    monkeypatch.setattr(
        "data_sync_service.service.tv_capture_worker.wake_tv_capture_worker",
        lambda: None,
    )

    out = tvsvc.enqueue_screener_capture(screener_id="falcon", trigger="api")
    assert out["jobId"] == "existing"
    assert calls == []


def test_process_capture_job_marks_done(monkeypatch) -> None:
    monkeypatch.setattr(
        jobdb,
        "get_job",
        lambda jid: {"id": jid, "screener_id": "falcon", "status": "running"},
    )
    monkeypatch.setattr(
        tvsvc,
        "_capture_and_persist_screener",
        lambda *, screener_id: {"snapshotId": "snap1", "rowCount": 7, "screenerId": screener_id},
    )
    marked: list[tuple[str, str, int]] = []

    def fake_mark_done(*, job_id: str, snapshot_id: str, row_count: int) -> None:
        marked.append((job_id, snapshot_id, row_count))

    monkeypatch.setattr(jobdb, "mark_done", fake_mark_done)
    out = tvsvc.process_capture_job("job-1")
    assert out["rowCount"] == 7
    assert marked == [("job-1", "snap1", 7)]


def test_process_capture_job_marks_failed(monkeypatch) -> None:
    from fastapi import HTTPException

    monkeypatch.setattr(
        jobdb,
        "get_job",
        lambda jid: {"id": jid, "screener_id": "falcon", "status": "running"},
    )
    monkeypatch.setattr(
        tvsvc,
        "_capture_and_persist_screener",
        lambda *, screener_id: (_ for _ in ()).throw(HTTPException(status_code=409, detail="CDP down")),
    )
    failed: list[tuple[str, str]] = []

    def fake_mark_failed(*, job_id: str, error_message: str) -> None:
        failed.append((job_id, error_message))

    monkeypatch.setattr(jobdb, "mark_failed", fake_mark_failed)
    with pytest.raises(HTTPException):
        tvsvc.process_capture_job("job-2")
    assert failed == [("job-2", "CDP down")]


def test_wait_for_capture_jobs_polls_until_terminal(monkeypatch) -> None:
    states = iter(
        [
            {"id": "j1", "screener_id": "falcon", "status": "queued", "trigger_source": "api"},
            {"id": "j1", "screener_id": "falcon", "status": "running", "trigger_source": "api"},
            {
                "id": "j1",
                "screener_id": "falcon",
                "status": "done",
                "trigger_source": "api",
                "snapshot_id": "s1",
                "row_count": 3,
            },
        ]
    )

    monkeypatch.setattr(jobdb, "get_job", lambda jid: next(states))
    updates: list[str] = []
    jobs = tvsvc.wait_for_capture_jobs(
        ["j1"],
        timeout_s=5,
        poll_s=0.01,
        on_update=lambda j: updates.append(str(j.get("status"))),
    )
    assert jobs[0]["status"] == "done"
    assert jobs[0]["rowCount"] == 3
    assert updates == ["queued", "running", "done"]


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not available")
def test_job_db_enqueue_claim_and_done() -> None:
    sid = f"test-screener-{int(time.time() * 1000)}"
    job = jobdb.insert_job(screener_id=sid, trigger_source="test")
    assert job["status"] == "queued"

    dup = jobdb.enqueue_or_get_active(screener_id=sid, trigger_source="test")
    assert dup["id"] == job["id"]

    claimed = jobdb.claim_next_jobs(limit=5)
    ids = {str(j["id"]) for j in claimed}
    assert job["id"] in ids

    jobdb.mark_done(job_id=job["id"], snapshot_id="snap-x", row_count=11)
    final = jobdb.get_job(job["id"])
    assert final is not None
    assert final["status"] == "done"
    assert final["row_count"] == 11


def test_worker_respects_max_concurrency(monkeypatch) -> None:
    import data_sync_service.service.tv_capture_worker as worker  # type: ignore[import-not-found]

    lock = threading.Lock()
    active = {"count": 0, "max": 0}

    def slow_process(job_id: str) -> dict:
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        try:
            time.sleep(0.1)
            return {"jobId": job_id}
        finally:
            with lock:
                active["count"] -= 1

    jobs = [{"id": f"j{i}", "screener_id": f"s{i}", "status": "queued"} for i in range(4)]
    idx = {"i": 0}

    def fake_claim(*, limit: int = 1) -> list[dict]:
        out: list[dict] = []
        while len(out) < limit and idx["i"] < len(jobs):
            out.append(jobs[idx["i"]])
            idx["i"] += 1
        return out

    monkeypatch.setattr(jobdb, "claim_next_jobs", fake_claim)
    monkeypatch.setattr(worker, "process_capture_job", slow_process)
    monkeypatch.setattr(worker, "POLL_INTERVAL_S", 0.02)

    worker.start_tv_capture_worker()
    try:
        deadline = time.time() + 2.0
        while time.time() < deadline and idx["i"] < len(jobs):
            time.sleep(0.05)
        assert active["max"] <= worker.MAX_CONCURRENT_CAPTURES
    finally:
        worker.stop_tv_capture_worker()
