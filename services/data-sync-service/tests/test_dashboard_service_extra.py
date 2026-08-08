"""service/dashboard.py coverage: sync steps, screeners, sync flows."""

from __future__ import annotations

import queue
from datetime import date

from fastapi import HTTPException

from data_sync_service.service import dashboard as dash  # noqa: F401  (alias used below)


class TestNews:
    def test_news_items(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "ensure_news_tables", lambda: None)
        items = [
            {"id": "1", "sourceId": "s", "title": "t1", "link": "l", "publishedAt": "p", "importance": 2, "relevanceScore": 30, "actionability": "a", "tickers": ["X"], "aiSummary": "sum"},
            {"id": "2", "sourceId": "s", "title": "t2", "link": "l", "publishedAt": "p", "importance": None, "relevanceScore": None, "actionability": None, "tickers": None, "aiSummary": None},
            {"id": "3", "sourceId": "s", "title": "t3", "link": "l", "publishedAt": "p", "importance": 0, "relevanceScore": 0, "actionability": None, "tickers": [], "aiSummary": None},
            {"id": "4", "sourceId": "s", "title": "t4", "link": "l", "publishedAt": "p", "importance": 1, "relevanceScore": 10, "actionability": None, "tickers": [], "aiSummary": None},
        ]
        monkeypatch.setattr(dash, "fetch_items", lambda limit, hours: (4, items))
        out = dash._news_items(hours=24, limit=50)
        assert out["total"] == 4
        ids = [i["id"] for i in out["items"]]
        assert ids == ["1", "4", "2"]
        assert out["items"][0]["tickers"] == ["X"]
        assert out["items"][2]["tickers"] == []


class TestScreenersStatus:
    def test_status(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "list_screeners", lambda: {"items": [
            {"id": "s1", "name": "One", "enabled": True, "updatedAt": "u"},
            {"id": "s2", "name": "Two", "enabled": False, "updatedAt": "u"},
            {"id": "", "name": "no-id", "enabled": True},
            "not-a-dict",
        ]})
        monkeypatch.setattr(dash, "list_latest_snapshots_for_screeners", lambda sids: {
            "s1": {"capturedAt": "c", "rowCount": 5, "filters": [1, 2]},
        })
        rows = dash._screeners_status(limit=50)
        assert len(rows) == 1
        assert rows[0]["id"] == "s1"
        assert rows[0]["rowCount"] == 5 and rows[0]["filtersCount"] == 2

    def test_status_non_dict(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "list_screeners", lambda: "not-a-dict")
        monkeypatch.setattr(dash, "list_latest_snapshots_for_screeners", lambda sids: {})
        assert dash._screeners_status() == []


class TestIsoHelpers:
    def test_iso(self) -> None:
        assert dash._now_iso().endswith("+00:00")
        assert dash._today_iso_date().count("-") == 2
        assert dash._shanghai_today_iso().count("-") == 2

        dash._index_signal_items(as_of_date="d") if False else None

    def test_index_signal_items(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "get_index_signals", lambda **kw: [{"k": "v"}])
        assert dash._index_signal_items(as_of_date="2026-08-07") == [{"k": "v"}]


class TestBundles:
    def test_build_industry_bundle(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "ensure_industry", lambda: None)
        monkeypatch.setattr(dash, "trade_dates_upto", lambda *a, **k: ["d1", "d2"])
        monkeypatch.setattr(dash, "get_rows_for_dates", lambda dates: [1, 2])
        monkeypatch.setattr(dash, "build_dashboard_industry_bundle", lambda **kw: {"ok": True})
        assert dash._build_industry_bundle(as_of_date="2026-08-07") == {"ok": True}

    def test_industry_top_by_date(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "ensure_industry", lambda: None)
        monkeypatch.setattr(dash, "trade_dates_upto", lambda *a, **k: ["d1"])
        monkeypatch.setattr(dash, "get_rows_for_dates", lambda dates: [])
        monkeypatch.setattr(dash, "top_by_date_from_rows", lambda rows, dates, top_k: [{"x": 1}])
        assert dash._industry_top_by_date(as_of_date="d", days=5) == [{"x": 1}]

    def test_sentiment_bundle(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "trade_dates_upto", lambda *a, **k: ["d1", "d2"])
        monkeypatch.setattr(dash, "list_sentiment_days_for_dates", lambda dates: [{"downCount": 3, "upCount": 10, "riskMode": "risk-on"}])
        monkeypatch.setattr(dash, "apply_breadth_panic_sentiment_items", lambda items, dc: items)
        monkeypatch.setattr(dash, "apply_breadth_panic_index_signals", lambda sig, dc: sig)
        monkeypatch.setattr(dash, "get_index_signals", lambda **kw: [{"k": "v"}])
        monkeypatch.setattr(dash, "build_etf_fund_flow_bundle", lambda **kw: {"items": []})
        monkeypatch.setattr(dash, "build_etf_flow_signal", lambda **kw: {"verdict": "neutral"})
        monkeypatch.setattr(dash, "compute_srv_index", lambda **kw: 1.5)
        monkeypatch.setattr(dash, "_industry_top_by_date", lambda **kw: [])
        monkeypatch.setattr(dash, "ensure_industry", lambda: None)
        monkeypatch.setattr(dash, "get_rows_for_dates", lambda dates: [])
        monkeypatch.setattr(dash, "max_net_inflow_for_date", lambda rows, d: (1e8, None))
        monkeypatch.setattr(dash, "compute_execution_gate", lambda **kw: {"ok": True})
        out = dash._build_market_sentiment_bundle(as_of_date="2026-08-07", use_realtime_index=False)
        assert out["srvIndex"] == 1.5
        assert out["executionGate"] == {"ok": True}
        assert out["indexSignals"] == [{"k": "v"}]

    def test_sentiment_bundle_realtime(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "trade_dates_upto", lambda *a, **k: ["d1"])
        monkeypatch.setattr(dash, "list_sentiment_days_for_dates", lambda dates: [])
        monkeypatch.setattr(dash, "apply_breadth_panic_sentiment_items", lambda items, dc: items)
        monkeypatch.setattr(dash, "apply_breadth_panic_index_signals", lambda sig, dc: sig)
        monkeypatch.setattr(dash, "build_etf_fund_flow_bundle", lambda **kw: {"items": []})
        monkeypatch.setattr(dash, "build_etf_flow_signal", lambda **kw: {"verdict": "neutral"})
        monkeypatch.setattr(dash, "compute_srv_index", lambda **kw: 0.0)
        monkeypatch.setattr(dash, "_industry_top_by_date", lambda **kw: [])
        monkeypatch.setattr(dash, "ensure_industry", lambda: None)
        monkeypatch.setattr(dash, "get_rows_for_dates", lambda dates: [])
        monkeypatch.setattr(dash, "max_net_inflow_for_date", lambda rows, d: (None, None))
        monkeypatch.setattr(dash, "compute_execution_gate", lambda **kw: {"ok": True})
        out = dash._build_market_sentiment_bundle(as_of_date="2026-08-07", use_realtime_index=True, index_signals=[])
        assert out["indexSignals"] == []


class TestRunStep:
    def test_ok_dict(self, monkeypatch) -> None:
        monkeypatch.setattr(dash.time, "perf_counter", lambda: 1.0)
        out = dash._run_step("x", lambda: {"a": 1})
        assert out["ok"] is True and out["meta"] == {"a": 1}

    def test_ok_non_dict(self) -> None:
        out = dash._run_step("x", lambda: 42)
        assert out["ok"] is True and out["meta"] == {}

    def test_exc(self) -> None:
        out = dash._run_step("x", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        assert out["ok"] is False and out["message"] == "boom"


class TestSyncSteps:
    def test_macro_step(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "sync_macro_daily_full", lambda: {"ok": True, "updated": 1})
        assert dash._sync_macro_step()["updated"] == 1
        monkeypatch.setattr(dash, "sync_macro_daily_full", lambda: None)
        assert dash._sync_macro_step()["ok"] is True

    def test_industry_step(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "sync_cn_industry_fund_flow", lambda **kw: {"ok": True})
        assert dash._sync_industry_step(force=True)["ok"] is True
        monkeypatch.setattr(dash, "sync_cn_industry_fund_flow", lambda **kw: None)
        assert dash._sync_industry_step()["ok"] is True

    def test_sentiment_step(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "shanghai_today_iso", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "sync_cn_sentiment", lambda **kw: {"items": [{"riskMode": "risk-off", "yesterdayLimitUpPremium": 0.5, "failedLimitUpRate": 10.0}], "asOfDate": "2026-08-07"})
        monkeypatch.setattr(dash, "sync_etf_fund_flow_watchlist", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "sync_top_inst_watchlist", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "sync_option_iv_daily", lambda **kw: {"ok": True})
        out = dash._sync_sentiment_step(force=True)
        assert out["riskMode"] == "risk-off"
        assert out["premium"] == 0.5
        monkeypatch.setattr(dash, "sync_cn_sentiment", lambda **kw: None)
        out = dash._sync_sentiment_step(force=True)
        assert out["asOfDate"] == "2026-08-07" and out["riskMode"] == ""

    def test_news_step(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "fetch_all_sources", lambda: {"a": 3, "b": 0, "c": -1})
        out = dash._sync_news_step()
        assert out == {"total": 3, "failed": 1, "sources": 3}


class TestScreenerHelpers:
    def test_skip_after_close(self, monkeypatch) -> None:
        assert dash._skip_screener_after_close_from_meta({"capturedAt": "2026-08-07 10:00", "rowCount": 5}, "2026-08-07") == (True, 5)
        assert dash._skip_screener_after_close_from_meta({"capturedAt": "2026-08-06", "rowCount": 5}, "2026-08-07") == (False, 5)
        assert dash._skip_screener_after_close_from_meta({}, "2026-08-07") == (False, 0)
        monkeypatch.setattr(dash, "list_latest_snapshots_for_screeners", lambda sids: {"s1": {"capturedAt": "2026-08-07", "rowCount": 2}})
        assert dash._should_skip_screener_after_close(sid="s1", today_sh="2026-08-07") == (True, 2)

    def test_job_to_result(self) -> None:
        ok = dash._job_to_screener_result({"screenerId": "s1", "status": "done", "rowCount": 3, "jobId": "j1"}, name="N", duration_ms=1)
        assert ok["status"] == "ok" and ok["ok"] is True
        missing = dash._job_to_screener_result({"screenerId": "s1", "status": "done", "rowCount": 0, "jobId": "j1"}, name="N", duration_ms=1)
        assert missing["status"] == "missing"
        failed = dash._job_to_screener_result({"screenerId": "s1", "status": "failed", "error": "e", "jobId": "j1"}, name="N", duration_ms=1)
        assert failed["status"] == "failed" and failed["error"] == "e"
        noerr = dash._job_to_screener_result({"screenerId": "s1", "status": "failed"}, name="N", duration_ms=1)
        assert noerr["error"] == "failed"

    def test_progress_from_job(self) -> None:
        assert dash._progress_from_job({"screenerId": "s1", "status": "done", "rowCount": 2, "jobId": "j"}, name="N")["status"] == "ok"
        assert dash._progress_from_job({"screenerId": "s1", "status": "done", "rowCount": 0, "jobId": "j"}, name="N")["status"] == "missing"
        assert dash._progress_from_job({"screenerId": "s1", "status": "failed", "jobId": "j"}, name="N")["status"] == "failed"
        assert dash._progress_from_job({"screenerId": "s1", "status": "running", "jobId": "j"}, name="N")["status"] == "running"
        assert dash._progress_from_job({"screenerId": "s1", "status": "queued", "jobId": "j"}, name="N")["status"] == "queued"


class TestSyncOneScreener:
    def test_missing_id(self) -> None:
        out = dash._sync_one_screener({}, skip_after_close=False, today_sh="d")
        assert out["status"] == "failed" and "missing screener id" in out["error"]

    def test_skip(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_should_skip_screener_after_close", lambda **kw: (True, 4))
        out = dash._sync_one_screener({"id": "s1", "name": "N"}, skip_after_close=True, today_sh="d")
        assert out["status"] == "skipped" and out["rowCount"] == 4

    def test_ok(self, monkeypatch) -> None:
        enqueued = {"jobId": "j1", "screenerId": "s1", "status": "queued"}
        monkeypatch.setattr(dash, "enqueue_screener_capture", lambda **kw: enqueued)
        def wait_fake(*a, **kw):
            kw.get("on_update")({"jobId": "j1", "screenerId": "s1", "status": "done", "rowCount": 7})
            return [{"jobId": "j1", "screenerId": "s1", "status": "done", "rowCount": 7}]

        monkeypatch.setattr(dash, "wait_for_capture_jobs", wait_fake)
        seen = []
        out = dash._sync_one_screener({"id": "s1", "name": "N"}, skip_after_close=False, today_sh="d", on_screener_progress=seen.append)
        assert out["status"] == "ok" and out["rowCount"] == 7
        assert len(seen) == 2

    def test_http_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "enqueue_screener_capture", lambda **kw: (_ for _ in ()).throw(HTTPException(status_code=409, detail="conflict")))
        out = dash._sync_one_screener({"id": "s1", "name": "N"}, skip_after_close=False, today_sh="d")
        assert out["status"] == "failed" and out["error"] == "conflict"

    def test_generic_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "enqueue_screener_capture", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        out = dash._sync_one_screener({"id": "s1", "name": "N"}, skip_after_close=False, today_sh="d")
        assert out["status"] == "failed" and out["error"] == "boom"


class TestSyncScreenersStep:
    def test_disabled(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "list_screeners", lambda: {"items": [{"id": "s1", "enabled": True}]})
        out = dash._sync_screeners_step(screeners_enabled=False)
        assert out == {"enabled": 1, "skipped": True, "failed": 0, "missing": 0}

    def test_full(self, monkeypatch) -> None:
        items = [
            {"id": "s1", "name": "A", "enabled": True},
            {"id": "s2", "name": "B", "enabled": True},
            {"id": "", "enabled": True},
            "not-a-dict",
            {"id": "s3", "name": "C", "enabled": False},
        ]
        monkeypatch.setattr(dash, "list_screeners", lambda: {"items": items})
        monkeypatch.setattr(dash, "_is_shanghai_sync_window", lambda: False)
        monkeypatch.setattr(dash, "_shanghai_today_iso", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "list_latest_snapshots_for_screeners", lambda sids: {"s1": {"capturedAt": "2026-08-07", "rowCount": 9}})
        results = []
        monkeypatch.setattr(dash, "_sync_one_screener", lambda sc, **kw: results.append(sc) or {"id": sc["id"], "status": "ok"})
        out = dash._sync_screeners_step(screeners_enabled=True)
        assert out["enabled"] == 3
        assert out["skippedIds"] == ["s1"]
        assert out["skipped"] is False
        assert [r["id"] for r in results] == ["s2"]

    def test_failed_and_missing(self, monkeypatch) -> None:
        items = [
            {"id": "s1", "name": "A", "enabled": True},
            {"id": "s2", "name": "B", "enabled": True},
        ]
        monkeypatch.setattr(dash, "list_screeners", lambda: {"items": items})
        monkeypatch.setattr(dash, "_is_shanghai_sync_window", lambda: True)
        monkeypatch.setattr(dash, "_shanghai_today_iso", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "list_latest_snapshots_for_screeners", lambda sids: {})
        monkeypatch.setattr(dash, "_sync_one_screener", lambda sc, **kw: {"id": sc["id"], "status": "failed"} if sc["id"] == "s1" else {"id": sc["id"], "status": "missing"})
        out = dash._sync_screeners_step(screeners_enabled=True)
        assert out["enabled"] == 2 and out["skipped"] is False
        assert out["failedIds"] == ["s1"] and out["missingIds"] == ["s2"]

    def test_progress_queue(self, monkeypatch) -> None:
        q = queue.Queue()
        q.put({"id": "s1"})
        events = list(dash._drain_screener_progress_events(q))
        assert len(events) == 1 and '"screener"' in events[0]
        assert list(dash._drain_screener_progress_events(queue.Queue())) == []

    def test_run_screeners_with_progress(self, monkeypatch) -> None:
        def step(**kw):
            kw["on_screener_progress"]({"id": "s1"})
            return {"ok": True, "failedIds": []}

        monkeypatch.setattr(dash, "_sync_screeners_step", step)
        q = queue.Queue()
        out = dash._run_screeners_step_with_progress(screeners_enabled=False, progress_queue=q)
        assert out["ok"] is True
        assert q.get_nowait() == {"id": "s1"}


class TestDashboardSummary:
    def _patch(self, monkeypatch, market_status=None, is_sync_window=False):
        monkeypatch.setattr(dash, "get_latest_sentiment_date", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "get_latest_industry_date", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "shanghai_today_iso", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "resolve_effective_as_of", lambda d: d)
        monkeypatch.setattr(dash, "compute_market_status", lambda: market_status or {"isPreMarket": False, "isMarketOpen": True})
        monkeypatch.setattr(dash, "_is_shanghai_sync_window", lambda: is_sync_window)
        monkeypatch.setattr(dash, "get_index_signals", lambda **kw: [{"k": "v"}])
        monkeypatch.setattr(dash, "_build_industry_bundle", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_build_market_sentiment_bundle", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_screeners_status", lambda *a, **k: [{"id": "s1"}])
        monkeypatch.setattr(dash, "_news_items", lambda *a, **k: {"total": 1, "items": []})
        monkeypatch.setattr(dash, "build_macro_snapshot", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "format_market_environment_zh", lambda s: "中文环境")
        monkeypatch.setattr(dash, "previous_open_date", lambda d: None)

    def test_summary(self, monkeypatch) -> None:
        self._patch(monkeypatch, is_sync_window=True)
        out = dash.dashboard_summary()
        assert out["asOfDate"] == "2026-08-07"
        assert out["meta"]["inSyncWindow"] is True
        assert out["meta"]["useRealtimeIndex"] is True
        assert out["marketEnvironmentZh"] == "中文环境"

    def test_summary_premarket_clamp(self, monkeypatch) -> None:
        self._patch(monkeypatch, market_status={"isPreMarket": True, "isMarketOpen": False})
        monkeypatch.setattr(dash, "previous_open_date", lambda d: date.fromisoformat("2026-08-06"))
        out = dash.dashboard_summary()
        assert out["asOfDate"] == "2026-08-06"

    def test_summary_no_blocks(self, monkeypatch) -> None:
        self._patch(monkeypatch, market_status={"isPreMarket": True, "isMarketOpen": False})
        monkeypatch.setattr(dash, "previous_open_date", lambda d: (_ for _ in ()).throw(ValueError("bad")))
        out = dash.dashboard_summary(
            include_macro=False,
            include_sentiment=False,
            include_news=False,
            include_industry=False,
            include_screeners=False,
        )
        assert out["marketEnvironmentZh"] == ""
        assert out["macroSnapshot"] is None
        assert out["news"] == {"hours": 24, "total": 0, "items": []}

    def test_summary_macro_exception(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(dash, "build_macro_snapshot", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        out = dash.dashboard_summary(include_sentiment=False, include_news=False, include_industry=False, include_screeners=False)
        assert out["marketEnvironmentZh"] == ""


class TestSyncFlows:
    def test_dashboard_sync(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True, "failedIds": ["s1"], "missingIds": ["s2"]})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync(force=True, screeners=True)
        assert out["ok"] is True
        assert out["screener"] == {"failed": ["s1"], "missing": ["s2"]}

    def test_dashboard_sync_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync(force=False, screeners=False)
        assert out["ok"] is False
        assert out["steps"][0]["ok"] is False

    def test_dashboard_sync_parallel(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True, "failedIds": ["s1"]})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync_parallel(force=True, screeners=True)
        assert out["ok"] is True
        names = [s["name"] for s in out["steps"]]
        assert names == ["industryFundFlow", "marketSentiment", "macroDaily", "screeners", "news"]

    def test_dashboard_sync_parallel_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync_parallel(force=False, screeners=False)
        assert out["ok"] is False
        assert out["steps"][0]["ok"] is False

    def test_dashboard_sync_stream(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_run_screeners_step_with_progress", lambda **kw: {"name": "screeners", "ok": True, "meta": {}})
        monkeypatch.setattr(dash, "dashboard_summary", lambda **kw: {"asOfDate": "d"})
        chunks = list(dash.dashboard_sync_stream(force=True, screeners=True))
        assert chunks[0].startswith('{"type": "start"')
        assert chunks[-1].startswith('{"type": "done"')
        assert '"asOfDate": "d"' in chunks[-1]

    def test_dashboard_sync_stream_no_screeners(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_screeners_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "dashboard_summary", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        chunks = list(dash.dashboard_sync_stream(force=True, screeners=False))
        assert chunks[-1].startswith('{"type": "done"')
