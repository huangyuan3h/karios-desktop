"""OPT-144: peripheral job failures page only on a 3-streak.

High-severity jobs emit job_failed immediately; low-severity (option_iv,
news_fetch, ...) record to system_events + hub digest on every failure but
emit to the phone only after 3 consecutive failures. Requires Postgres
(verifies the real streak query + real emit gating).
"""

from __future__ import annotations

import uuid

import pytest

from data_sync_service.db import get_connection
from data_sync_service.db import sync_job_record as rec

pytestmark = pytest.mark.requires_postgres

_CREATED: set[str] = set()


def _job() -> str:
    jt = f"test_periph_{uuid.uuid4().hex[:8]}"
    _CREATED.add(jt)
    return jt


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    if not _CREATED:
        return
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sync_job_record WHERE job_type = ANY(%s)",
                (sorted(_CREATED),),
            )
            cur.execute(
                "DELETE FROM system_events WHERE dedupe_key LIKE 'job_failed:test_periph_%'",
            )
        conn.commit()
    _CREATED.clear()


def _emitted(monkeypatch) -> list:
    out: list = []

    def fake_emit(event_type, payload, dedupe_key=None, **kw):
        out.append({"type": event_type, "payload": payload, "key": dedupe_key})
        return True

    monkeypatch.setattr("data_sync_service.db.webhook.emit_event", fake_emit)
    return out


def test_high_severity_emits_immediately(monkeypatch) -> None:
    emitted = _emitted(monkeypatch)
    rec.insert_record("close_sync", success=False, error_message="boom")
    assert len(emitted) == 1
    assert emitted[0]["type"] == "job_failed"
    # cleanup the real close_sync row this test just wrote
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM sync_job_record WHERE job_type='close_sync' AND error_message='boom'"
            )
            cur.execute(
                "DELETE FROM system_events WHERE dedupe_key LIKE 'job_failed:close_sync:%'"
                " AND title LIKE '%boom%'"
            )
        conn.commit()


def test_low_severity_single_failure_silent(monkeypatch) -> None:
    emitted = _emitted(monkeypatch)
    jt = _job()
    rec.insert_record(jt, success=False, error_message="e1")
    assert emitted == []
    assert rec.consec_failures(jt) == 1


def test_low_severity_emits_on_third_streak(monkeypatch) -> None:
    emitted = _emitted(monkeypatch)
    jt = _job()
    rec.insert_record(jt, success=False, error_message="e1")
    rec.insert_record(jt, success=False, error_message="e2")
    assert emitted == []
    rec.insert_record(jt, success=False, error_message="e3")
    assert len(emitted) == 1
    assert emitted[0]["payload"]["streak"] == 3
    assert emitted[0]["payload"]["job_type"] == jt


def test_success_resets_streak(monkeypatch) -> None:
    emitted = _emitted(monkeypatch)
    jt = _job()
    rec.insert_record(jt, success=False, error_message="e1")
    rec.insert_record(jt, success=False, error_message="e2")
    rec.insert_record(jt, success=True)
    assert rec.consec_failures(jt) == 0
    rec.insert_record(jt, success=False, error_message="e3")
    assert emitted == []
    assert rec.consec_failures(jt) == 1
