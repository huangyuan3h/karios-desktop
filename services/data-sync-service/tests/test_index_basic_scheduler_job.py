"""Tests for the index_basic_sync scheduler job wiring."""

from __future__ import annotations

import pytest

from data_sync_service.scheduler import index_basic_job


def _capture_logs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []

    def fake_info(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    def fake_warning(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    monkeypatch.setattr(index_basic_job.logger, "info", fake_info)
    monkeypatch.setattr(index_basic_job.logger, "warning", fake_warning)
    return messages


def test_constants() -> None:
    assert index_basic_job.JOB_ID == "index_basic_sync"
    assert index_basic_job.CRON_EXPRESSION == "15 17 * * 1-5"
    assert index_basic_job.TIMEZONE == "Asia/Shanghai"


def test_build_trigger_is_cron() -> None:
    from apscheduler.triggers.cron import CronTrigger

    trigger = index_basic_job.build_trigger()
    assert isinstance(trigger, CronTrigger)


def test_run_logs_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        index_basic_job,
        "sync_index_basic_full",
        lambda: {"ok": True, "skipped": True, "message": "already synced today"},
    )
    msgs = _capture_logs(monkeypatch)
    index_basic_job.run()
    assert any("index_basic_sync skipped" in m for m in msgs)


def test_run_logs_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        index_basic_job,
        "sync_index_basic_full",
        lambda: {"ok": True, "updated": 42},
    )
    msgs = _capture_logs(monkeypatch)
    index_basic_job.run()
    assert any("index_basic_sync ok: updated=42" in m for m in msgs)


def test_run_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        index_basic_job,
        "sync_index_basic_full",
        lambda: {"ok": False, "error": "boom"},
    )
    msgs = _capture_logs(monkeypatch)
    index_basic_job.run()
    assert any("index_basic_sync failed: boom" in m for m in msgs)