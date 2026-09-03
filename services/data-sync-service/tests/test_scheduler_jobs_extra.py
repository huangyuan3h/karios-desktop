"""Scheduler job wiring coverage: create_scheduler + all cron/interval jobs."""

from __future__ import annotations

import logging
from importlib import import_module

import pytest
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from data_sync_service import scheduler as scheduler_pkg

MODULES = {
    "adj_factor_job": ("adj_factor_full_sync", "sync_adj_factor_full", "30 18 * * 5"),
    "eastmoney_industry_job": ("eastmoney_industry_sync", "sync_eastmoney_industry_incremental", "0 18 * * 1-5"),
    "etf_daily_job": ("etf_daily_full_sync", "sync_etf_daily_full", "0 19 1 * *"),
    "fund_basic_job": ("etf_fund_basic_sync", "sync_etf_fund_basic", "0 4 1 * *"),
    "hk_basic_job": ("hk_basic_sync", "sync_hk_basic", "30 3 1 * *"),
    "hk_daily_job": ("hk_daily_full_sync", "sync_hk_daily_full", "30 17 * * *"),
    "hk_industry_job": ("hk_industry_sync", "sync_hk_industry", "0 2 * * *"),
    "index_daily_job": ("index_daily_full_sync", "sync_index_daily_full", "30 16 * * 1-5"),
    "macro_daily_job": ("macro_daily_full_sync", "sync_macro_daily_full", "0 7 * * 2-6"),
}


@pytest.mark.parametrize("mod_name", sorted(MODULES))
def test_cron_job_constants_and_trigger(mod_name: str) -> None:
    job = import_module(f"data_sync_service.scheduler.{mod_name}")
    expected_id, _, expected_cron = MODULES[mod_name]
    assert job.JOB_ID == expected_id
    assert job.CRON_EXPRESSION == expected_cron
    assert job.TIMEZONE == "Asia/Shanghai"
    assert isinstance(job.build_trigger(), CronTrigger)


@pytest.mark.parametrize("mod_name", sorted(MODULES))
def test_cron_job_run_logs_ok(monkeypatch: pytest.MonkeyPatch, mod_name: str) -> None:
    job = import_module(f"data_sync_service.scheduler.{mod_name}")
    _, svc, _ = MODULES[mod_name]
    if mod_name == "hk_industry_job":
        monkeypatch.setattr(job, "get_hk_industry_status", lambda: {"missingHk": 3, "totalHk": 100})
    monkeypatch.setattr(job, svc, lambda **kw: {"ok": True, "updated": 5})
    msgs = _capture_logs(monkeypatch, job)
    job.run()
    assert any("ok" in m for m in msgs)


@pytest.mark.parametrize("mod_name", sorted(MODULES))
def test_cron_job_run_logs_skipped(monkeypatch: pytest.MonkeyPatch, mod_name: str) -> None:
    job = import_module(f"data_sync_service.scheduler.{mod_name}")
    _, svc, _ = MODULES[mod_name]
    if mod_name == "hk_industry_job":
        monkeypatch.setattr(job, "get_hk_industry_status", lambda: {"missingHk": 0, "totalHk": 0})
    monkeypatch.setattr(job, svc, lambda **kw: {"ok": True, "skipped": True, "message": "noop", "coveragePct": 100})
    msgs = _capture_logs(monkeypatch, job)
    job.run()
    assert any("skipped" in m for m in msgs)


@pytest.mark.parametrize("mod_name", sorted(MODULES))
def test_cron_job_run_logs_failure(monkeypatch: pytest.MonkeyPatch, mod_name: str) -> None:
    job = import_module(f"data_sync_service.scheduler.{mod_name}")
    _, svc, _ = MODULES[mod_name]
    if mod_name == "hk_industry_job":
        monkeypatch.setattr(job, "get_hk_industry_status", lambda: {"missingHk": 5, "totalHk": 100})
    monkeypatch.setattr(job, svc, lambda **kw: {"ok": False, "error": "upstream down"})
    msgs = _capture_logs(monkeypatch, job)
    job.run()
    assert any("failed" in m and "upstream down" in m for m in msgs)


def _capture_logs(monkeypatch: pytest.MonkeyPatch, job) -> list[str]:  # noqa: ANN001
    messages: list[str] = []

    def fake_info(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    def fake_warning(msg: str, *args, **kwargs) -> None:
        messages.append(str(msg) % args if args else str(msg))

    monkeypatch.setattr(job.logger, "info", fake_info)
    monkeypatch.setattr(job.logger, "warning", fake_warning)
    return messages


class TestCreateScheduler:
    def test_registers_all_jobs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeScheduler:
            def __init__(self, **kwargs) -> None:  # noqa: ANN003
                self.jobs: list[tuple] = []

            def add_job(self, func, trigger, id=None, replace_existing=False) -> None:  # noqa: A002, ANN001
                self.jobs.append((func, trigger, id, replace_existing))

        monkeypatch.setattr(scheduler_pkg, "BackgroundScheduler", FakeScheduler)
        sched = scheduler_pkg.create_scheduler()
        ids = {job[2] for job in sched.jobs}
        expected = {
            "stock_basic_sync", "hk_basic_sync", "hk_daily_full_sync", "etf_fund_basic_sync",
            "etf_daily_full_sync", "daily_full_sync", "adj_factor_full_sync", "close_sync",
            "close_sync_catchup", "news_fetch_job", "alpha_radar_ingest_job",
            "alpha_radar_process_job", "alpha_radar_pipeline_job", "index_daily_full_sync",
            "macro_daily_full_sync", "watchlist_automation", "intraday_score",
            "eastmoney_industry_sync",
            "hk_industry_sync", "index_basic_sync", "cn_industry_post_close_sync",
            "paper_trading_intake", "paper_trading_update", "paper_s3_intake", "paper_twin_star",
            "allocation_decide", "backtest_paper_recon",
            "twin_star_reminder", "twin_star_intraday", "timeline_warmup",
            "stock_daily_basic_sync", "sleeve_etf_daily_sync",
            "news_enrich_job", "research_report_sync",
            "decision_snapshot", "decision_outcome", "decision_action_tracking",
            "morning_brief_am", "morning_brief_pm", "eod_chain_startup_catchup",
            "rolling_oos",
            "trading_brief_open", "trading_brief_midday", "trading_brief_action",
            "paper_chain_watchdog",
            "paper_backtest_mirror",
            "weekly_review",
            "intraday_alarm",
            "candidate_diff",
            "webhook_delivery",
            "behavior_audit",
            "minute_capture",
            "bar_5min_close",
            "sleeve_paper_auto",
        }
        assert ids == expected

    def test_triggers_are_cron_or_interval(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class FakeScheduler:
            def __init__(self, **kwargs) -> None:  # noqa: ANN003
                self.jobs: list[tuple] = []

            def add_job(self, func, trigger, id=None, replace_existing=False) -> None:  # noqa: A002, ANN001
                self.jobs.append((func, trigger, id, replace_existing))

        monkeypatch.setattr(scheduler_pkg, "BackgroundScheduler", FakeScheduler)
        sched = scheduler_pkg.create_scheduler()
        for _func, trigger, jid, _replace in sched.jobs:
            if jid == "eod_chain_startup_catchup":
                # One-shot restart catch-up (DateTrigger), not a cron/interval.
                from apscheduler.triggers.date import DateTrigger

                assert isinstance(trigger, DateTrigger)
                continue
            assert isinstance(trigger, (CronTrigger, IntervalTrigger))


class TestStockBasicJob:
    def test_constants_and_trigger(self) -> None:
        from data_sync_service.scheduler import stock_basic_job

        assert stock_basic_job.JOB_ID == "stock_basic_sync"
        assert stock_basic_job.CRON_EXPRESSION == "0 18 * * 5"
        assert isinstance(stock_basic_job.build_trigger(), CronTrigger)

    def test_run_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import stock_basic_job

        monkeypatch.setattr(stock_basic_job, "sync_stock_basic", lambda: {"ok": True, "updated": 5})
        msgs = _capture_logs(monkeypatch, stock_basic_job)
        stock_basic_job.run()
        assert any("stock_basic_sync ok" in m for m in msgs)

    def test_run_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import stock_basic_job

        monkeypatch.setattr(stock_basic_job, "sync_stock_basic", lambda: {"ok": False, "error": "boom"})
        msgs = _capture_logs(monkeypatch, stock_basic_job)
        stock_basic_job.run()
        assert any("stock_basic_sync failed" in m and "boom" in m for m in msgs)


class TestCloseSyncJob:
    def test_run_ok_runs_post(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_sync_job

        monkeypatch.setattr(close_sync_job, "sync_close", lambda exchange, force: {"ok": True, "updated_daily_rows": 100, "updated_adj_factor_rows": 5, "trade_dates": ["2026-08-07"]})
        monkeypatch.setattr(close_sync_job, "run_post_close_sync", lambda: {"indexDaily": {"ok": True}, "macroDaily": {"ok": True}})
        msgs = _capture_logs(monkeypatch, close_sync_job)
        close_sync_job.run()
        assert any("close_sync ok" in m for m in msgs)
        assert any("post_close_sync" in m for m in msgs)

    def test_run_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_sync_job

        monkeypatch.setattr(close_sync_job, "sync_close", lambda exchange, force: {"ok": True, "skipped": True, "message": "not trading day"})
        monkeypatch.setattr(close_sync_job, "run_post_close_sync", lambda: {})
        msgs = _capture_logs(monkeypatch, close_sync_job)
        close_sync_job.run()
        assert any("skipped" in m for m in msgs)

    def test_run_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_sync_job

        monkeypatch.setattr(close_sync_job, "sync_close", lambda exchange, force: {"ok": False, "error": "boom"})
        msgs = _capture_logs(monkeypatch, close_sync_job)
        close_sync_job.run()
        assert any("close_sync failed" in m and "boom" in m for m in msgs)


class TestDailySyncJob:
    def test_run_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import daily_sync_job

        monkeypatch.setattr(daily_sync_job, "sync_close", lambda exchange, force: {"ok": True, "updated_daily_rows": 10})
        msgs = _capture_logs(monkeypatch, daily_sync_job)
        daily_sync_job.run()
        assert any("ok" in m for m in msgs)

    def test_run_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import daily_sync_job

        monkeypatch.setattr(daily_sync_job, "sync_close", lambda exchange, force: {"ok": True, "skipped": True})
        msgs = _capture_logs(monkeypatch, daily_sync_job)
        daily_sync_job.run()
        assert any("skipped" in m for m in msgs)

    def test_run_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import daily_sync_job

        monkeypatch.setattr(daily_sync_job, "sync_close", lambda exchange, force: {"ok": False, "error": "boom"})
        msgs = _capture_logs(monkeypatch, daily_sync_job)
        daily_sync_job.run()
        assert any("failed" in m for m in msgs)

    def test_run_non_dict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import daily_sync_job

        monkeypatch.setattr(daily_sync_job, "sync_close", lambda exchange, force: None)
        msgs = _capture_logs(monkeypatch, daily_sync_job)
        daily_sync_job.run()
        assert any("completed" in m for m in msgs)


class TestCloseCatchupJob:
    def test_skips_when_close_done(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_catchup_job

        monkeypatch.setattr(close_catchup_job, "get_today_run", lambda job_type: {"success": True})
        monkeypatch.setattr(close_catchup_job, "sync_close", lambda exchange, force: (_ for _ in ()).throw(AssertionError("should not run")))
        close_catchup_job.run()

    def test_ok_records(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_catchup_job

        monkeypatch.setattr(close_catchup_job, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(close_catchup_job, "sync_close", lambda exchange, force: {"ok": True, "updated_daily_rows": 1, "updated_adj_factor_rows": 1, "trade_dates": ["2026-08-07"]})
        monkeypatch.setattr(close_catchup_job, "run_post_close_sync", lambda: {"ok": True})
        records = []
        monkeypatch.setattr(close_catchup_job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        close_catchup_job.run()
        assert records == [("close_sync_catchup", True, "2026-08-07", None)]

    def test_skipped_records(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_catchup_job

        monkeypatch.setattr(close_catchup_job, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(close_catchup_job, "sync_close", lambda exchange, force: {"ok": True, "skipped": True, "message": "not trading day"})
        monkeypatch.setattr(close_catchup_job, "run_post_close_sync", lambda: {})
        records = []
        monkeypatch.setattr(close_catchup_job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        close_catchup_job.run()
        assert records == [("close_sync_catchup", True, "not trading day", "skipped")]

    def test_failed_records(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_catchup_job

        monkeypatch.setattr(close_catchup_job, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(close_catchup_job, "sync_close", lambda exchange, force: {"ok": False, "error": "boom"})
        records = []
        monkeypatch.setattr(close_catchup_job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        close_catchup_job.run()
        assert records == [("close_sync_catchup", False, None, "boom")]

    def test_crash_records(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import close_catchup_job

        monkeypatch.setattr(close_catchup_job, "get_today_run", lambda job_type: None)
        monkeypatch.setattr(close_catchup_job, "sync_close", lambda exchange, force: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(close_catchup_job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        close_catchup_job.run()
        assert records == [("close_sync_catchup", False, None, "crash")]


class TestWatchlistAutomationJob:
    def test_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import watchlist_automation_job as job

        monkeypatch.setattr(job, "run_watchlist_automation", lambda trigger, force: {"ok": True, "remove": [], "alphaAdd": [], "runId": "r1"})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("watchlist_automation", True, "r1", None)]

    def test_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import watchlist_automation_job as job

        monkeypatch.setattr(job, "run_watchlist_automation", lambda trigger, force: {"ok": True, "skipped": True, "skipReason": "cold start", "runId": None})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        msgs = _capture_logs(monkeypatch, job)
        job.run()
        assert records == [("watchlist_automation", False, None, "cold start")]
        assert any("skipped" in m for m in msgs)

    def test_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import watchlist_automation_job as job

        monkeypatch.setattr(job, "run_watchlist_automation", lambda trigger, force: {"ok": False, "error": "boom"})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("watchlist_automation", False, None, "boom")]

    def test_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import watchlist_automation_job as job

        monkeypatch.setattr(job, "run_watchlist_automation", lambda trigger, force: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("watchlist_automation", False, None, "crash")]


class TestDecisionJobs:
    @pytest.mark.parametrize(
        ("mod_name", "svc", "record_id", "log_word"),
        [
            ("decision_action_job", "extract_pending_actions", "decision_action_tracking", "extracted"),
            ("decision_outcome_job", "apply_daily_outcomes", "decision_outcome", "updated"),
            ("decision_snapshot_job", "build_daily_snapshot", "decision_snapshot", None),
        ],
    )
    def test_ok(self, monkeypatch: pytest.MonkeyPatch, mod_name: str, svc: str, record_id: str, log_word: str | None) -> None:
        job = import_module(f"data_sync_service.scheduler.{mod_name}")
        if log_word == "extracted":
            monkeypatch.setattr(job, "extract_pending_actions", lambda: {"extracted": 2})
            monkeypatch.setattr(job, "match_executions", lambda: {"matched": 1})
            monkeypatch.setattr(job, "track_action_outcomes", lambda: {"tracked": 1})
        elif log_word == "updated":
            monkeypatch.setattr(job, "apply_daily_outcomes", lambda days: {"updated": 3})
        else:
            monkeypatch.setattr(job, "build_daily_snapshot", lambda: {"snapshot": "s"})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [(record_id, True, None)]

    @pytest.mark.parametrize(
        "mod_name",
        ["decision_action_job", "decision_outcome_job", "decision_snapshot_job"],
    )
    def test_exception(self, monkeypatch: pytest.MonkeyPatch, mod_name: str) -> None:
        job = import_module(f"data_sync_service.scheduler.{mod_name}")
        for name in dir(job):
            if name.startswith(("extract_pending_actions", "match_executions", "track_action_outcomes", "apply_daily_outcomes", "build_daily_snapshot")):
                monkeypatch.setattr(job, name, lambda **kw: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records[0][1] is False and records[0][2] == "crash"


class TestPaperTradingJobs:
    @pytest.mark.parametrize(
        ("mod_name", "svc", "record_id"),
        [
            ("paper_trading_intake_job", "run_intake", "paper_trading_intake"),
            ("paper_trading_update_job", "run_update", "paper_trading_update"),
        ],
    )
    def test_ok(self, monkeypatch: pytest.MonkeyPatch, mod_name: str, svc: str, record_id: str) -> None:
        job = import_module(f"data_sync_service.scheduler.{mod_name}")
        monkeypatch.setattr(job, svc, lambda **kw: {"inserted": 3, "skipped": 1, "skippedReasons": {}})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [(record_id, True, None)]

    @pytest.mark.parametrize(
        ("mod_name", "svc", "record_id"),
        [
            ("paper_trading_intake_job", "run_intake", "paper_trading_intake"),
            ("paper_trading_update_job", "run_update", "paper_trading_update"),
        ],
    )
    def test_partial_error(self, monkeypatch: pytest.MonkeyPatch, mod_name: str, svc: str, record_id: str) -> None:
        job = import_module(f"data_sync_service.scheduler.{mod_name}")
        monkeypatch.setattr(job, svc, lambda **kw: {"error": "partial"})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [(record_id, False, "partial")]

    @pytest.mark.parametrize(
        ("mod_name", "svc", "record_id"),
        [
            ("paper_trading_intake_job", "run_intake", "paper_trading_intake"),
            ("paper_trading_update_job", "run_update", "paper_trading_update"),
        ],
    )
    def test_exception(self, monkeypatch: pytest.MonkeyPatch, mod_name: str, svc: str, record_id: str) -> None:
        job = import_module(f"data_sync_service.scheduler.{mod_name}")
        monkeypatch.setattr(job, svc, lambda **kw: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [(record_id, False, "crash")]


class TestMorningBriefJob:
    def test_triggers(self) -> None:
        from data_sync_service.scheduler import morning_brief_job

        assert isinstance(morning_brief_job.build_am_trigger(), CronTrigger)
        assert isinstance(morning_brief_job.build_pm_trigger(), CronTrigger)
        assert morning_brief_job.JOB_ID_AM == "morning_brief_am"
        assert morning_brief_job.JOB_ID_PM == "morning_brief_pm"

    @pytest.mark.parametrize(
        ("brief_type", "job_id"),
        [("morning", "morning_brief_am"), ("midday", "morning_brief_pm")],
    )
    def test_ok(self, monkeypatch: pytest.MonkeyPatch, brief_type: str, job_id: str) -> None:
        from data_sync_service.scheduler import morning_brief_job as job

        monkeypatch.setattr("data_sync_service.service.morning_brief.generate_brief", lambda brief_type: {"items": [{"id": "i1"}]})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run(brief_type=brief_type)
        assert records == [(job_id, True, "1", None)]

    @pytest.mark.parametrize(
        ("brief_type", "job_id"),
        [("morning", "morning_brief_am"), ("midday", "morning_brief_pm")],
    )
    def test_empty_items_records_failure(self, monkeypatch: pytest.MonkeyPatch, brief_type: str, job_id: str) -> None:
        from data_sync_service.scheduler import morning_brief_job as job

        monkeypatch.setattr("data_sync_service.service.morning_brief.generate_brief", lambda brief_type: {"items": []})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run(brief_type=brief_type)
        assert records == [(job_id, False, "0", "No enriched items available for brief")]

    def test_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import morning_brief_job as job

        monkeypatch.setattr("data_sync_service.service.morning_brief.generate_brief", lambda brief_type: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run(brief_type="morning")
        assert records == [("morning_brief_am", False, "crash")]


class TestNewsJobs:
    def test_fetch_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_fetch_job as job

        monkeypatch.setattr(job, "fetch_all_sources", lambda: {"source_a": 5, "source_b": 3})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("news_fetch_job", True, "8", None)]

    def test_fetch_partial_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_fetch_job as job

        monkeypatch.setattr(job, "fetch_all_sources", lambda: {"a": 5, "b": -1, "c": "n/a"})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("news_fetch_job", False, "5", "1 source(s) failed; fetched=5")]

    def test_fetch_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_fetch_job as job

        monkeypatch.setattr(job, "fetch_all_sources", lambda: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [("news_fetch_job", False, "crash")]

    def test_enrich_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_enrich_job as job

        monkeypatch.setattr("data_sync_service.service.news_enrich.run_enrichment_cycle", lambda max_batches: {"totalFailed": 0, "totalEnriched": 10, "batchesProcessed": 2})
        monkeypatch.setattr("data_sync_service.db.news.count_by_enrichment_status", lambda: {"pending": 0})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("news_enrich_job", True, "10", None)]

    def test_enrich_partial_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_enrich_job as job

        monkeypatch.setattr("data_sync_service.service.news_enrich.run_enrichment_cycle", lambda max_batches: {"totalFailed": 2, "totalEnriched": 3, "batchesProcessed": 1})
        monkeypatch.setattr("data_sync_service.db.news.count_by_enrichment_status", lambda: {})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("news_enrich_job", False, "3", "failed=2; enriched=3")]

    def test_enrich_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import news_enrich_job as job

        monkeypatch.setattr("data_sync_service.service.news_enrich.run_enrichment_cycle", lambda max_batches: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [("news_enrich_job", False, "crash")]

    def test_enrich_trigger(self) -> None:
        from apscheduler.triggers.cron import CronTrigger

        from data_sync_service.scheduler import news_enrich_job

        # OPT-108: LLM off-peak — nightly cron (20:00/23:00/05:00).
        trig = news_enrich_job.build_trigger()
        assert isinstance(trig, CronTrigger)
        hours = sorted({e.first for e in trig.fields[5].expressions})  # hour field
        assert hours == [5, 20, 23]


class TestResearchJob:
    def test_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import research_report_job as job

        monkeypatch.setattr("data_sync_service.service.research.sync_research_reports", lambda days, max_pages: {"ok": True, "fetched": 4, "inserted": 3})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("research_report_sync", True, "3", None)]

    def test_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import research_report_job as job

        monkeypatch.setattr("data_sync_service.service.research.sync_research_reports", lambda days, max_pages: {"ok": False, "error": "upstream down", "fetched": 0, "inserted": 0})
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, last_ts_code, error_message)))
        job.run()
        assert records == [("research_report_sync", False, "0", "upstream down")]

    def test_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import research_report_job as job

        monkeypatch.setattr("data_sync_service.service.research.sync_research_reports", lambda days, max_pages: (_ for _ in ()).throw(RuntimeError("crash")))
        records = []
        monkeypatch.setattr(job, "insert_record", lambda jt, success, last_ts_code=None, error_message=None: records.append((jt, success, error_message)))
        job.run()
        assert records == [("research_report_sync", False, "crash")]

    def test_trigger(self) -> None:
        from data_sync_service.scheduler import research_report_job

        assert isinstance(research_report_job.build_trigger(), IntervalTrigger)


class TestAlphaRadarJobs:
    def test_fetch_skipped(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_fetch_job as job

        monkeypatch.setattr(job, "run_alpha_radar_pipeline", lambda force, trigger: {"skipped": True, "lastRunAt": "2026-08-08T00:00:00Z"})
        job.run()
        assert "skipped" in caplog.text

    def test_fetch_ok(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_fetch_job as job

        monkeypatch.setattr(job, "run_alpha_radar_pipeline", lambda force, trigger: {"ok": True, "ingestStats": {"stored": 3}, "trendCount": 2})
        job.run()
        assert "complete" in caplog.text

    def test_fetch_failed_result(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_fetch_job as job

        monkeypatch.setattr(job, "run_alpha_radar_pipeline", lambda force, trigger: {"ok": False, "errors": ["boom"]})
        job.run()
        assert "failed" in caplog.text

    def test_fetch_exception(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_fetch_job as job

        monkeypatch.setattr(job, "run_alpha_radar_pipeline", lambda force, trigger: (_ for _ in ()).throw(RuntimeError("crash")))
        job.run()
        assert "failed" in caplog.text

    def test_ingest_ok(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_ingest_job as job

        monkeypatch.setattr(job, "run_alpha_radar_ingest", lambda trigger: {"ingestStats": {"stored": 1, "new": 1, "requeued": 0, "unchanged": 0}, "rawBacklogCount": 2})
        job.run()
        assert "complete" in caplog.text

    def test_ingest_exception(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_ingest_job as job

        monkeypatch.setattr(job, "run_alpha_radar_ingest", lambda trigger: (_ for _ in ()).throw(RuntimeError("crash")))
        job.run()
        assert "failed" in caplog.text

    def test_ingest_interval_hours(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import alpha_radar_ingest_job as job

        monkeypatch.setenv("ALPHA_RADAR_INGEST_INTERVAL_HOURS", "6")
        assert job.ingest_interval_hours() == 6
        monkeypatch.setenv("ALPHA_RADAR_INGEST_INTERVAL_HOURS", "abc")
        assert job.ingest_interval_hours() == job.DEFAULT_INTERVAL_HOURS
        monkeypatch.setenv("ALPHA_RADAR_INGEST_INTERVAL_HOURS", "0")
        assert job.ingest_interval_hours() == 1
        monkeypatch.delenv("ALPHA_RADAR_INGEST_INTERVAL_HOURS")
        assert job.ingest_interval_hours() == job.DEFAULT_INTERVAL_HOURS
        assert isinstance(job.build_trigger(), IntervalTrigger)

    def test_process_ok(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_process_job as job

        monkeypatch.setattr(job, "run_alpha_radar_process", lambda trigger: {"processedHeadlines": 5, "trendsProduced": 2, "processRounds": 1, "rawBacklogCount": 0})
        job.run()
        assert "complete" in caplog.text

    def test_process_exception(self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.INFO)
        from data_sync_service.scheduler import alpha_radar_process_job as job

        monkeypatch.setattr(job, "run_alpha_radar_process", lambda trigger: (_ for _ in ()).throw(RuntimeError("crash")))
        job.run()
        assert "failed" in caplog.text

    def test_process_interval_hours(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from data_sync_service.scheduler import alpha_radar_process_job as job

        monkeypatch.setenv("ALPHA_RADAR_PROCESS_INTERVAL_HOURS", "3")
        assert job.process_interval_hours() == 3
        monkeypatch.setenv("ALPHA_RADAR_PROCESS_INTERVAL_HOURS", "xyz")
        assert job.process_interval_hours() == job.DEFAULT_INTERVAL_HOURS
        monkeypatch.delenv("ALPHA_RADAR_PROCESS_INTERVAL_HOURS")
        assert job.process_interval_hours() == job.DEFAULT_INTERVAL_HOURS
        from apscheduler.triggers.cron import CronTrigger

        assert isinstance(job.build_trigger(), CronTrigger)  # OPT-108 nightly

    def test_fetch_trigger(self) -> None:
        from apscheduler.triggers.cron import CronTrigger

        from data_sync_service.scheduler import alpha_radar_fetch_job

        assert isinstance(alpha_radar_fetch_job.build_trigger(), CronTrigger)  # OPT-108 nightly


# ---------------------------------------------------------------------------
# EOD-chain restart catch-up (2026-08-11: in-memory job store silently drops
# cron fires missed during a restart — 2026-08-10 17:30/18:15 both skipped).
# ---------------------------------------------------------------------------


def _monkey_cst(monkeypatch, h: int, m: int) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    monkeypatch.setattr(
        scheduler_pkg,
        "_cst_now",
        lambda: datetime(2026, 8, 10, h, m, tzinfo=ZoneInfo("Asia/Shanghai")),
    )


def _patch_today_runs(monkeypatch, *, close_ok=True, ran: set[str] | None = None) -> None:
    from data_sync_service.db import sync_job_record as sjr

    ran = ran or set()

    def fake_today_run(job_type: str) -> dict | None:
        if job_type == "stock_close_sync":
            return {"success": close_ok} if close_ok else None
        return {"success": True} if job_type in ran else None

    monkeypatch.setattr(sjr, "get_today_run", fake_today_run)


def test_catchup_reruns_missing_eod_chain(monkeypatch) -> None:
    """17:40 evening restart with close_sync done but no EOD steps recorded →
    watchlist_automation re-runs; paper_s3_intake waits for its slot guard
    (17:45) and the fresh watchlist record."""
    _monkey_cst(monkeypatch, 17, 40)
    _patch_today_runs(monkeypatch)
    calls: dict[str, int] = {"wa": 0, "s3": 0, "cn": 0}
    monkeypatch.setattr(scheduler_pkg.watchlist_automation_job, "run", lambda: calls.__setitem__("wa", calls["wa"] + 1))
    monkeypatch.setattr(scheduler_pkg.paper_s3_intake_job, "run", lambda: calls.__setitem__("s3", calls["s3"] + 1))
    monkeypatch.setattr(scheduler_pkg.cn_industry_post_close_job, "run", lambda: calls.__setitem__("cn", calls["cn"] + 1))
    scheduler_pkg.catchup_missed_eod_chain()
    assert calls == {"wa": 1, "s3": 0, "cn": 0}  # s3 guard (17:45) + cn guard (18:25) not reached


def test_catchup_respects_already_run_and_slots(monkeypatch) -> None:
    """Steps with a today-record are not re-run; s3 runs once past 17:45 with
    a watchlist record; the 18:15 step runs once past its slot; before the
    17:30 slot nothing runs."""
    _monkey_cst(monkeypatch, 18, 30)
    _patch_today_runs(monkeypatch, ran={"watchlist_automation"})
    calls: dict[str, int] = {"wa": 0, "s3": 0, "cn": 0}
    monkeypatch.setattr(scheduler_pkg.watchlist_automation_job, "run", lambda: calls.__setitem__("wa", calls["wa"] + 1))
    monkeypatch.setattr(scheduler_pkg.paper_s3_intake_job, "run", lambda: calls.__setitem__("s3", calls["s3"] + 1))
    monkeypatch.setattr(scheduler_pkg.cn_industry_post_close_job, "run", lambda: calls.__setitem__("cn", calls["cn"] + 1))
    scheduler_pkg.catchup_missed_eod_chain()
    assert calls == {"wa": 0, "s3": 1, "cn": 1}

    _monkey_cst(monkeypatch, 17, 20)
    calls2: dict[str, int] = {"wa": 0, "s3": 0, "cn": 0}
    monkeypatch.setattr(scheduler_pkg.watchlist_automation_job, "run", lambda: calls2.__setitem__("wa", calls2["wa"] + 1))
    monkeypatch.setattr(scheduler_pkg.paper_s3_intake_job, "run", lambda: calls2.__setitem__("s3", calls2["s3"] + 1))
    monkeypatch.setattr(scheduler_pkg.cn_industry_post_close_job, "run", lambda: calls2.__setitem__("cn", calls2["cn"] + 1))
    scheduler_pkg.catchup_missed_eod_chain()
    assert calls2 == {"wa": 0, "s3": 0, "cn": 0}


def test_catchup_skips_without_close_sync_or_on_weekend(monkeypatch) -> None:
    _monkey_cst(monkeypatch, 18, 30)
    _patch_today_runs(monkeypatch, close_ok=False)
    ran: list[str] = []
    monkeypatch.setattr(scheduler_pkg.watchlist_automation_job, "run", lambda: ran.append("wa"))
    monkeypatch.setattr(scheduler_pkg.paper_s3_intake_job, "run", lambda: ran.append("s3"))
    monkeypatch.setattr(scheduler_pkg.cn_industry_post_close_job, "run", lambda: ran.append("cn"))
    scheduler_pkg.catchup_missed_eod_chain()
    assert ran == []
