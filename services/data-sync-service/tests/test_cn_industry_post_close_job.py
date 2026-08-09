"""Tests for the cn_industry_post_close_sync scheduler job wiring."""

from __future__ import annotations

import pytest

from data_sync_service.scheduler import cn_industry_post_close_job


def _capture_logs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []

    def fake_info(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    def fake_warning(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    monkeypatch.setattr(cn_industry_post_close_job.logger, "info", fake_info)
    monkeypatch.setattr(cn_industry_post_close_job.logger, "warning", fake_warning)
    return messages


def test_constants() -> None:
    assert cn_industry_post_close_job.JOB_ID == "cn_industry_post_close_sync"
    assert cn_industry_post_close_job.CRON_EXPRESSION == "35 17 * * 1-5"
    assert cn_industry_post_close_job.TIMEZONE == "Asia/Shanghai"


def test_build_trigger_is_cron() -> None:
    from apscheduler.triggers.cron import CronTrigger

    trigger = cn_industry_post_close_job.build_trigger()
    assert isinstance(trigger, CronTrigger)


def test_run_calls_all_three(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, int] = {"industry": 0, "mainline": 0, "sentiment": 0}

    # Real service contract: success dicts carry NO "ok" key (regression 2026-08-06
    # — job judged success by "ok", which is always absent → permanent failure).
    def fake_industry(*, days: int = 10, top_n: int = 10) -> dict:
        called["industry"] += 1
        return {"asOfDate": "2026-07-04", "rows": 5, "histFailures": 0}

    def fake_mainline(*, force: bool = False) -> dict:
        called["mainline"] += 1
        return {"ok": True, "asOfDate": "2026-07-04"}

    def fake_sentiment(*, date_str: str, force: bool) -> dict:
        called["sentiment"] += 1
        return {"asOfDate": date_str, "days": 1, "items": []}

    monkeypatch.setattr(cn_industry_post_close_job, "sync_cn_industry_fund_flow", fake_industry)
    monkeypatch.setattr(cn_industry_post_close_job, "sync_cn_industry_mainline", fake_mainline)
    monkeypatch.setattr(cn_industry_post_close_job, "sync_cn_sentiment", fake_sentiment)

    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    cn_industry_post_close_job.run()

    assert called == {"industry": 1, "mainline": 1, "sentiment": 1}
    assert captured == [(cn_industry_post_close_job.JOB_ID, True, None)]


def test_run_skips_on_non_trading_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_industry_fund_flow",
        lambda *, days=10, top_n=10: {
            "ok": True,
            "skipped": True,
            "reason": "not_trading_day",
            "asOfDate": "2026-07-05",
        },
    )
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_industry_mainline",
        lambda *, force=False: {"ok": True, "asOfDate": "2026-07-03"},
    )
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_sentiment",
        lambda *, date_str, force: {"ok": True, "asOfDate": date_str},
    )
    captured: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success)),
    )

    msgs = _capture_logs(monkeypatch)
    cn_industry_post_close_job.run()

    assert captured == [(cn_industry_post_close_job.JOB_ID, True)]
    assert any("cn_industry_post_close_sync skipped" in m for m in msgs)


def test_run_logs_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_industry_fund_flow",
        lambda *, days=10, top_n=10: {"ok": False, "error": "upstream down"},
    )
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_industry_mainline",
        lambda *, force=False: {"ok": True},
    )
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "sync_cn_sentiment",
        lambda *, date_str, force: {"ok": True},
    )
    captured: list[tuple[str, bool, str | None]] = []
    monkeypatch.setattr(
        cn_industry_post_close_job,
        "insert_record",
        lambda jt, success, last_ts_code=None, error_message=None: captured.append((jt, success, error_message)),
    )

    msgs = _capture_logs(monkeypatch)
    cn_industry_post_close_job.run()

    assert captured == [(cn_industry_post_close_job.JOB_ID, False, "industry: upstream down")]
    assert any("cn_industry_post_close_sync failed" in m for m in msgs)