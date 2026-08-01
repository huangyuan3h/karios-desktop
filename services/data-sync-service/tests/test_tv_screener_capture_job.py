"""Tests for the tv_screener_capture scheduler job wiring (AM + PM)."""

from __future__ import annotations

import pytest

from data_sync_service.scheduler import tv_screener_capture_job


def _capture_logs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []

    def fake_info(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    def fake_warning(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    monkeypatch.setattr(tv_screener_capture_job.logger, "info", fake_info)
    monkeypatch.setattr(tv_screener_capture_job.logger, "warning", fake_warning)
    return messages


def test_constants() -> None:
    assert tv_screener_capture_job.JOB_ID_AM == "tv_screener_capture_am"
    assert tv_screener_capture_job.JOB_ID_PM == "tv_screener_capture_pm"
    assert tv_screener_capture_job.TIMEZONE == "Asia/Shanghai"


def test_am_trigger_is_cron() -> None:
    from apscheduler.triggers.cron import CronTrigger

    trigger = tv_screener_capture_job.build_am_trigger()
    assert isinstance(trigger, CronTrigger)


def test_pm_trigger_is_cron() -> None:
    from apscheduler.triggers.cron import CronTrigger

    trigger = tv_screener_capture_job.build_pm_trigger()
    assert isinstance(trigger, CronTrigger)


def test_run_records_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tv_screener_capture_job,
        "_sync_screeners_step",
        lambda *, screeners_enabled: {
            "enabled": 2,
            "skipped": False,
            "failed": 0,
            "missing": 0,
            "screenerResults": [],
        },
    )
    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        tv_screener_capture_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    tv_screener_capture_job.run()

    assert len(captured) == 1
    jt, success, msg = captured[0]
    assert jt == tv_screener_capture_job.JOB_ID_AM
    assert success is True
    assert msg is None


def test_run_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tv_screener_capture_job,
        "_sync_screeners_step",
        lambda *, screeners_enabled: {
            "enabled": 3,
            "skipped": False,
            "failed": 1,
            "missing": 1,
            "screenerResults": [],
        },
    )
    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        tv_screener_capture_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    msgs = _capture_logs(monkeypatch)
    tv_screener_capture_job.run()

    assert len(captured) == 1
    jt, success, msg = captured[0]
    assert success is False
    assert msg is not None and "failed=1" in msg and "missing=1" in msg
    assert any("tv_screener_capture failed" in m for m in msgs)


def test_run_records_skipped_when_no_screeners(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tv_screener_capture_job,
        "_sync_screeners_step",
        lambda *, screeners_enabled: {
            "enabled": 0,
            "skipped": True,
            "failed": 0,
            "missing": 0,
            "screenerResults": [],
        },
    )
    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        tv_screener_capture_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    msgs = _capture_logs(monkeypatch)
    tv_screener_capture_job.run()

    assert len(captured) == 1
    jt, success, msg = captured[0]
    assert jt == tv_screener_capture_job.JOB_ID_AM
    assert success is True
    assert msg is not None and "no enabled screeners" in msg
    assert any("tv_screener_capture skipped" in m for m in msgs)


def test_run_records_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(**kwargs) -> dict:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(tv_screener_capture_job, "_sync_screeners_step", boom)
    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        tv_screener_capture_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    msgs = _capture_logs(monkeypatch)
    tv_screener_capture_job.run()

    assert len(captured) == 1
    jt, success, msg = captured[0]
    assert success is False
    assert msg == "kaboom"
    assert any("tv_screener_capture crashed" in m for m in msgs)