"""paper_twin_star_job + twin_star_intraday_job run() paths (H1).

Thin cron wrappers — every branch is a record/no-record decision. All DB
access is stubbed; no network, no Postgres.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.scheduler import paper_twin_star_job as ptj
from data_sync_service.scheduler import twin_star_intraday_job as tsj

CN = ZoneInfo("Asia/Shanghai")


def _records(monkeypatch: pytest.MonkeyPatch, module_path: str):
    rows: list[tuple] = []
    monkeypatch.setattr(
        module_path,
        lambda jt, success, last_ts_code=None, error_message=None: rows.append(
            (jt, success, error_message)
        ),
    )
    return rows


# -- paper_twin_star_job -------------------------------------------------------


def _patch_ptj(monkeypatch: pytest.MonkeyPatch, *, intake=None, update=None):
    monkeypatch.setattr(ptj, "shanghai_today_iso", lambda: "2026-09-06")
    monkeypatch.setattr(
        ptj,
        "run_intake_twin_star",
        lambda trade_date: intake if intake is not None else {"inserted": 1, "skipped": 0},
    )
    monkeypatch.setattr(
        ptj,
        "run_update_twin_star",
        lambda today_iso_s: update if update is not None else {"closed": 2},
    )
    return _records(monkeypatch, "data_sync_service.scheduler.paper_twin_star_job.insert_record")


def test_ptj_ok_records_success(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_ptj(monkeypatch)
    ptj.run()
    assert rows == [("paper_twin_star", True, None)]


def test_ptj_intake_error_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_ptj(monkeypatch, intake={"error": "boom"})
    ptj.run()
    assert rows == [("paper_twin_star", False, "boom")]


def test_ptj_update_error_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_ptj(monkeypatch, update={"error": "stale"})
    ptj.run()
    assert rows == [("paper_twin_star", False, "stale")]


def test_ptj_exception_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ptj, "shanghai_today_iso", lambda: "2026-09-06")

    def _boom(**kw):
        raise RuntimeError("intake down")

    monkeypatch.setattr(ptj, "run_intake_twin_star", _boom)
    rows = _records(monkeypatch, "data_sync_service.scheduler.paper_twin_star_job.insert_record")
    ptj.run()
    assert rows == [("paper_twin_star", False, "intake down")]


def test_ptj_trigger_shape() -> None:
    trig = ptj.build_trigger()
    assert ptj.JOB_ID == "paper_twin_star" and ptj.TIMEZONE == "Asia/Shanghai"
    assert trig is not None


# -- twin_star_intraday_job ----------------------------------------------------


def _noon(status="ok"):
    # SNAPSHOT_EXPECT_MIN = 12:30 — use 12:31 so the post-expect path fires.
    return datetime(2026, 9, 6, 12, 31, tzinfo=CN)


def _patch_tsj(
    monkeypatch: pytest.MonkeyPatch, *, now=None, in_window=True, sat="sat", status=None, exc=None
):
    monkeypatch.setattr(tsj, "now_cn", lambda: now or _noon())
    monkeypatch.setattr(tsj, "in_live_tape_window", lambda n: in_window)
    if exc is not None:

        def _raise(*, now):
            raise exc

        monkeypatch.setattr(tsj, "maybe_refresh_intraday_sat", _raise)
    else:
        monkeypatch.setattr(tsj, "maybe_refresh_intraday_sat", lambda *, now: sat)
    monkeypatch.setattr(
        tsj,
        "intraday_snapshot_status",
        lambda *, now: status if status is not None else {"ok": True},
    )
    return _records(monkeypatch, "data_sync_service.db.sync_job_record.insert_record")


def _prev(monkeypatch: pytest.MonkeyPatch, prev):
    monkeypatch.setattr("data_sync_service.db.sync_job_record.get_today_run", lambda jt: prev)


def test_tsj_outside_window_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch, in_window=False)
    _prev(monkeypatch, None)
    tsj.run()
    assert rows == []


def test_tsj_refresh_exc_before_expect_min_no_record(monkeypatch: pytest.MonkeyPatch) -> None:
    early = datetime(2026, 9, 6, 9, 31, tzinfo=CN)
    rows = _patch_tsj(monkeypatch, now=early, exc=RuntimeError("em down"))
    _prev(monkeypatch, None)
    tsj.run()
    assert rows == []


def test_tsj_refresh_exc_after_expect_min_records(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch, exc=RuntimeError("em down"))
    _prev(monkeypatch, None)
    tsj.run()
    assert len(rows) == 1 and rows[0][1] is False and "em down" in (rows[0][2] or "")


def test_tsj_required_snapshot_failed_records(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch, status={"required": True, "ok": False, "reason": "stale file"})
    _prev(monkeypatch, None)
    tsj.run()
    assert len(rows) == 1 and rows[0][1] is False and "stale file" in (rows[0][2] or "")


def test_tsj_required_snapshot_failed_no_reason_default(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch, status={"required": True, "ok": False})
    _prev(monkeypatch, None)
    tsj.run()
    assert len(rows) == 1 and rows[0][1] is False


def test_tsj_no_sat_no_record(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch, sat=None)
    _prev(monkeypatch, None)
    tsj.run()
    assert rows == []


def test_tsj_success_records_once(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(
        monkeypatch,
        sat={
            "gateOpen": True,
            "breadth": 1,
            "gapCount": 2,
            "frozen": False,
            "candidates": [{"ts": "x"}],
        },
    )
    _prev(monkeypatch, None)
    tsj.run()
    assert len(rows) == 1 and rows[0][1] is True


def test_tsj_record_once_dedupes(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _patch_tsj(monkeypatch)
    _prev(monkeypatch, {"success": True})
    tsj._record_once(success=True)
    _prev(monkeypatch, {"success": False})
    tsj._record_once(success=False, error="x")
    assert rows == []


def test_tsj_record_db_error_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(jt):
        raise RuntimeError("db down")

    monkeypatch.setattr("data_sync_service.db.sync_job_record.get_today_run", _boom)
    tsj._record_once(success=True)  # must not raise


def test_tsj_trigger_shape() -> None:
    assert tsj.build_trigger() is not None and tsj.JOB_ID == "twin_star_intraday"
