"""service/dashboard.py coverage: sync steps, screeners, sync flows."""

from __future__ import annotations

from datetime import date

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


class TestDashboardSummary:
    def _patch(self, monkeypatch, market_status=None, is_sync_window=False):
        _ = is_sync_window
        monkeypatch.setattr(dash, "get_latest_sentiment_date", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "get_latest_industry_date", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "shanghai_today_iso", lambda: "2026-08-07")
        monkeypatch.setattr(dash, "resolve_effective_as_of", lambda d: d)
        monkeypatch.setattr(dash, "compute_market_status", lambda: market_status or {"isPreMarket": False, "isMarketOpen": True})
        monkeypatch.setattr(dash, "get_index_signals", lambda **kw: [{"k": "v"}])
        monkeypatch.setattr(dash, "_build_industry_bundle", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_build_market_sentiment_bundle", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_news_items", lambda *a, **k: {"total": 1, "items": []})
        monkeypatch.setattr(dash, "build_macro_snapshot", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "format_market_environment_zh", lambda s: "中文环境")
        monkeypatch.setattr(dash, "previous_open_date", lambda d: None)

    def test_summary(self, monkeypatch) -> None:
        self._patch(monkeypatch, is_sync_window=True)
        out = dash.dashboard_summary()
        assert out["asOfDate"] == "2026-08-07"
        assert out["meta"]["inSyncWindow"] is False
        assert out["meta"]["useRealtimeIndex"] is False
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
        )
        assert out["marketEnvironmentZh"] == ""
        assert out["macroSnapshot"] is None
        assert out["news"] == {"hours": 24, "total": 0, "items": []}

    def test_summary_macro_exception(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(dash, "build_macro_snapshot", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        out = dash.dashboard_summary(include_sentiment=False, include_news=False, include_industry=False, )
        assert out["marketEnvironmentZh"] == ""


class TestSyncFlows:
    def test_dashboard_sync(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync(force=True)
        assert out["ok"] is True

    def test_dashboard_sync_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync(force=False)
        assert out["ok"] is False
        assert out["steps"][0]["ok"] is False

    def test_dashboard_sync_parallel(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync_parallel(force=True)
        assert out["ok"] is True
        names = [s["name"] for s in out["steps"]]
        assert names == ["industryFundFlow", "marketSentiment", "macroDaily", "news"]

    def test_dashboard_sync_parallel_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        out = dash.dashboard_sync_parallel(force=False)
        assert out["ok"] is False
        assert out["steps"][0]["ok"] is False

    def test_dashboard_sync_stream(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "dashboard_summary", lambda **kw: {"asOfDate": "d"})
        chunks = list(dash.dashboard_sync_stream(force=True))
        assert chunks[0].startswith('{"type": "start"')
        assert chunks[-1].startswith('{"type": "done"')
        assert '"asOfDate": "d"' in chunks[-1]

    def test_dashboard_sync_stream_no_screeners(self, monkeypatch) -> None:
        monkeypatch.setattr(dash, "_now_iso", lambda: "now")
        monkeypatch.setattr(dash, "_sync_industry_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda **kw: {"ok": True})
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "dashboard_summary", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        chunks = list(dash.dashboard_sync_stream(force=True))
        assert chunks[-1].startswith('{"type": "done"')
