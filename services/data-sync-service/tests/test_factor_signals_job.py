"""Tests for the factor_signals_sync scheduler job wiring."""

from __future__ import annotations

import pytest

from data_sync_service.scheduler import factor_signals_job


def test_constants() -> None:
    assert factor_signals_job.JOB_ID == "factor_signals_sync"
    # 18:30 — after close_sync 17:10 (needs today's daily bars).
    assert factor_signals_job.CRON_EXPRESSION == "30 18 * * 1-5"
    assert factor_signals_job.TIMEZONE == "Asia/Shanghai"


def test_build_trigger_is_cron() -> None:
    from apscheduler.triggers.cron import CronTrigger

    trigger = factor_signals_job.build_trigger()
    assert isinstance(trigger, CronTrigger)


def test_run_scans_latest_open_date(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, str] = {}

    def fake_scan(trade_date: str) -> int:
        seen["trade_date"] = trade_date
        return 12

    monkeypatch.setattr(factor_signals_job, "scan_strong_scoop_exhaustion", fake_scan)
    monkeypatch.setattr(
        "data_sync_service.scheduler.factor_signals_job._latest_open_date",
        lambda: "2026-09-03",
    )

    captured: list[tuple[str, bool, str | None]] = []

    def fake_insert(job_type, success, last_ts_code=None, error_message=None):
        captured.append((job_type, success, error_message))

    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record", fake_insert
    )
    msgs: list[str] = []
    monkeypatch.setattr(
        factor_signals_job.logger, "info", lambda m, *a, **k: msgs.append(str(m) % a if a else str(m))
    )

    factor_signals_job.run()

    assert seen == {"trade_date": "2026-09-03"}
    assert captured == [(factor_signals_job.JOB_ID, True, None)]
    assert any("factor_signals_sync ok" in m for m in msgs)


def test_run_records_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_scan(trade_date: str) -> int:
        raise RuntimeError("db down")

    monkeypatch.setattr(factor_signals_job, "scan_strong_scoop_exhaustion", fake_scan)
    monkeypatch.setattr(
        "data_sync_service.scheduler.factor_signals_job._latest_open_date",
        lambda: "2026-09-03",
    )

    captured: list[tuple[str, bool, str | None]] = []

    def fake_insert(job_type, success, last_ts_code=None, error_message=None):
        captured.append((job_type, success, error_message))

    monkeypatch.setattr(
        "data_sync_service.scheduler._job_guard.insert_record", fake_insert
    )

    factor_signals_job.run()

    assert captured[0][0] == factor_signals_job.JOB_ID
    assert captured[0][1] is False
    assert "db down" in str(captured[0][2])


def test_latest_open_date_clamps_weekend(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import date as date_cls

    import data_sync_service.scheduler.factor_signals_job as job

    class FakeDate(date_cls):
        @classmethod
        def today(cls):  # type: ignore[override]
            return date_cls(2026, 9, 6)  # a Sunday

    monkeypatch.setattr(job, "date", FakeDate)
    # get_open_dates is imported inside the function; patch at source module.
    import data_sync_service.db.trade_calendar as tc

    monkeypatch.setattr(
        tc, "get_open_dates", lambda *a, **k: [date_cls(2026, 9, 4)]
    )
    assert job._latest_open_date() == "2026-09-04"

    monkeypatch.setattr(tc, "get_open_dates", lambda *a, **k: [])
    assert job._latest_open_date() == "2026-09-06"
