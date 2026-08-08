"""service/etf_fund_flow.py coverage: helpers, sync, bundle build, signal."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import Mock

import pandas as pd
import pytest

from data_sync_service.db.etf_fund_flow import upsert_daily_rows
from data_sync_service.service import etf_fund_flow as eff
from data_sync_service.service.etf_fund_flow_em import EM_ETF_FLOW_SOURCE

CORE = eff.ETF_WATCHLIST


class TestPureHelpers:
    def test_infer_category(self) -> None:
        assert eff._infer_etf_category("510300") == "broad"
        assert eff._infer_etf_category("159819") == "sector"
        assert eff._infer_etf_category("") == "broad"
        assert eff._infer_etf_category(None) == "broad"

    def test_date_utils(self) -> None:
        assert eff._yyyymmdd_to_iso("20260807") == "2026-08-07"
        assert eff._yyyymmdd_to_iso("bad") == "bad"
        assert eff._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"
        assert eff._now_iso().endswith("+00:00")
        assert eff._today_yyyymmdd().isdigit()
        assert eff._shanghai_today_yyyymmdd().isdigit()

    def test_avg_price(self) -> None:
        assert eff.compute_avg_price(close=1.0, vol=100.0, amount=200.0) == pytest.approx(20.0)
        assert eff.compute_avg_price(close=1.0, vol=0.0, amount=0.0) == 1.0
        assert eff.compute_avg_price(close=None, vol=None, amount=None) is None
        assert eff.compute_avg_price(close="bad", vol=None, amount=None) is None
        assert eff.compute_avg_price(close=float("nan"), vol=0.0, amount=0.0) is None
        assert eff.compute_avg_price(close=1.0, vol="bad", amount="bad") == 1.0

    def test_net_inflow_1d(self) -> None:
        assert eff.compute_net_inflow_1d(fd_share_today=2.0, fd_share_prev=1.0, avg_price=3.0) == pytest.approx(30000.0)
        assert eff.compute_net_inflow_1d(fd_share_today=None, fd_share_prev=1.0, avg_price=3.0) is None
        assert eff.compute_net_inflow_1d(fd_share_today=2.0, fd_share_prev=None, avg_price=3.0) is None
        assert eff.compute_net_inflow_1d(fd_share_today=2.0, fd_share_prev=1.0, avg_price=None) is None
        assert eff.compute_net_inflow_1d(fd_share_today="bad", fd_share_prev=1.0, avg_price=3.0) is None
        assert eff.compute_net_inflow_1d(fd_share_today=2.0, fd_share_prev=1.0, avg_price=float("nan")) is None

    def test_classify_signal(self) -> None:
        assert eff.classify_signal(category="broad", net_flow_1d=1.0, net_flow_3d=1.0) == "National Team Buy"
        assert eff.classify_signal(category="broad", net_flow_1d=-1.0, net_flow_3d=-1.0) == "National Team Outflow"
        assert eff.classify_signal(category="broad", net_flow_1d=1.0, net_flow_3d=-1.0) == "Neutral"
        assert eff.classify_signal(category="sector", net_flow_1d=1.0, net_flow_3d=2e9) == "Sector Momentum"
        assert eff.classify_signal(category="sector", net_flow_1d=-1.0, net_flow_3d=-1.0) == "Inst Outflow"
        assert eff.classify_signal(category="sector", net_flow_1d=1.0, net_flow_3d=1.0) == "Neutral"
        assert eff.classify_signal(category="other", net_flow_1d=1.0, net_flow_3d=1.0) == "Neutral"
        assert eff.classify_signal(category="broad", net_flow_1d=None, net_flow_3d=1.0) == "Neutral"
        assert eff.classify_signal(category="broad", net_flow_1d=1.0, net_flow_3d=None) == "Neutral"

    def test_signal_display(self) -> None:
        assert eff.signal_display("National Team Buy").startswith("🛡️")
        assert eff.signal_display("Unknown") == "➖ Unknown"

    def test_with_retry(self, monkeypatch) -> None:
        calls = []
        flaky = Mock(side_effect=[ValueError("x"), 42])
        monkeypatch.setattr(eff.time, "sleep", lambda s: calls.append(s))
        assert eff._with_retry(flaky, tries=3) == 42
        assert len(calls) == 1
        always_fail = Mock(side_effect=ValueError("boom"))
        with pytest.raises(ValueError, match="boom"):
            eff._with_retry(always_fail, tries=2, base_sleep_s=0.01)
        with pytest.raises(ValueError, match="boom"):
            eff._with_retry(always_fail, tries=0)


class TestUniverse:
    def test_fetch_extended(self, monkeypatch) -> None:
        rows = [
            ("510300.SH", "510300", "沪深300ETF"),  # core → excluded
            ("159999.SZ", "159999", "其他ETF"),
            ("512999.SH", "512999", "宽基ETF"),  # 5xxxxx → broad
        ]
        seen = {}

        class Cur:
            def execute(self, sql, params):
                seen["sql"] = sql
                return self

            def fetchall(self):
                return rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def cursor(self):
                return Cur()

        monkeypatch.setattr("data_sync_service.db.get_connection", lambda: Conn())
        monkeypatch.setattr("data_sync_service.db.stock_basic.ensure_table", lambda: None)
        out = eff._fetch_extended_etf_universe()
        assert [x["symbol"] for x in out] == ["159999", "512999"]
        assert out[0]["category"] == "sector"
        assert out[1]["category"] == "broad"
        assert "LIMIT %s" in seen["sql"]

    def test_fetch_extended_no_conn(self, monkeypatch) -> None:
        monkeypatch.setattr("data_sync_service.db.stock_basic.ensure_table", lambda: None)
        monkeypatch.setattr("data_sync_service.db.get_connection", lambda: (_ for _ in ()).throw(RuntimeError("no db")))
        with pytest.raises(RuntimeError):
            eff._fetch_extended_etf_universe(max_size=0)

    def test_watchlist_extended(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "_fetch_extended_etf_universe", lambda **kw: [{"symbol": "9", "ts_code": "9", "name": "x", "category": "sector"}])
        out = eff.get_etf_watchlist_extended()
        assert out[0]["symbol"] == CORE[0]["symbol"]
        assert out[-1]["symbol"] == "9"
        assert eff.get_etf_watchlist_extended(include_core=False, max_size=1) == [{"symbol": "9", "ts_code": "9", "name": "x", "category": "sector"}]
        assert eff.get_etf_watchlist_extended(max_size=1) == [CORE[0]]


class TestMerge:
    def test_merge_frames(self) -> None:
        share = pd.DataFrame([{"trade_date": "20260807", "fd_share": "100"}, {"trade_date": "bad", "fd_share": "9"}])
        daily = pd.DataFrame([{"trade_date": "2026-08-07", "close": 1.0, "vol": 100.0, "amount": 200.0}])
        out = eff._merge_tushare_frames("510300.SH", share_df=share, daily_df=daily, updated_at="now")
        assert out[0]["trade_date"] == "2026-08-07"
        assert out[0]["fd_share"] == 100.0
        assert out[0]["avg_price"] == pytest.approx(20.0)

    def test_merge_frames_empty_and_bad(self) -> None:
        assert eff._merge_tushare_frames("510300.SH", share_df=None, daily_df=None, updated_at="x") == []
        share = pd.DataFrame([{"trade_date": "20260807", "fd_share": "bad"}])
        daily = pd.DataFrame([{"trade_date": "20260807", "close": float("nan"), "vol": float("nan"), "amount": float("nan")}])
        out = eff._merge_tushare_frames("510300.SH", share_df=share, daily_df=daily, updated_at="x")
        assert out[0]["fd_share"] is None
        assert out[0]["close"] is None
        assert out[0]["avg_price"] is None

    def test_merge_frames_bad_values(self) -> None:
        share = pd.DataFrame([{"trade_date": "", "fd_share": "1"}, {"trade_date": "20260807", "fd_share": "1"}])
        daily = pd.DataFrame([{"trade_date": "", "close": "x", "vol": "x", "amount": "x"}, {"trade_date": "20260807", "close": "x", "vol": "x", "amount": "x"}])
        out = eff._merge_tushare_frames("510300.SH", share_df=share, daily_df=daily, updated_at="x")
        assert len(out) == 1
        assert out[0]["close"] is None and out[0]["avg_price"] is None


class TestRecompute:
    def test_recompute(self, monkeypatch) -> None:
        rows = [
            {"trade_date": "2026-08-07", "ts_code": "510300.SH", "fd_share": 200.0, "close": 4.0, "avg_price": 4.0, "source": ""},
            {"trade_date": "2026-08-06", "ts_code": "510300.SH", "fd_share": 100.0, "close": 3.0, "avg_price": 3.0, "source": ""},
        ]
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: rows)
        monkeypatch.setattr(eff, "fetch_row", lambda code, td: {})
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        seen = {}
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda updates: (seen.update(updates=updates) or len(updates)))
        n = eff._recompute_net_inflows_for_code("510300.SH", updated_at="now")
        assert n == 2
        by_td = {u["trade_date"]: u for u in seen["updates"]}
        assert by_td["2026-08-07"]["net_inflow"] == pytest.approx(4_000_000.0)
        assert by_td["2026-08-06"]["net_inflow"] is None

    def test_recompute_em_source(self, monkeypatch) -> None:
        rows = [
            {"trade_date": "2026-08-07", "ts_code": "510300.SH", "fd_share": 1.0, "source": EM_ETF_FLOW_SOURCE, "main_net_inflow": 55.0},
        ]
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: rows)
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [date(2026, 8, 6), date(2026, 8, 7)])
        seen = {}
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda updates: (seen.update(updates=updates) or len(updates)))
        eff._recompute_net_inflows_for_code("510300.SH", updated_at="now")
        assert seen["updates"][0]["net_inflow"] == 55.0

    def test_recompute_empty_and_bad(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [])
        assert eff._recompute_net_inflows_for_code("x", updated_at="n") == 0
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [{"trade_date": "bad-date", "ts_code": "x"}])
        assert eff._recompute_net_inflows_for_code("x", updated_at="n") == 0

    def test_recompute_first_day(self, monkeypatch) -> None:
        rows = [{"trade_date": "2026-08-05", "ts_code": "510300.SH", "fd_share": 100.0, "avg_price": 4.0, "source": ""}]
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: rows)
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [date(2026, 8, 5)])
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda updates: len(updates))
        assert eff._recompute_net_inflows_for_code("510300.SH", updated_at="n") == 0


class TestSyncTushare:
    def test_no_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "get_settings", lambda: Mock(tu_share_api_key=""))
        assert eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="n") == 0

    def test_full(self, monkeypatch) -> None:
        pro = Mock()
        pro.fund_share.return_value = pd.DataFrame([{"trade_date": "20260807", "fd_share": "100"}])
        pro.fund_daily.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0, "vol": 10.0, "amount": 10.0}])
        monkeypatch.setattr(eff, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(eff.ts, "pro_api", lambda key: pro)
        monkeypatch.setattr(eff, "get_last_trade_date", lambda code: None)
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [])
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
        monkeypatch.setattr(eff, "_recompute_net_inflows_for_code", lambda code, updated_at: 0)
        n = eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="n")
        assert n == 1
        pro.fund_share.assert_called_with(ts_code="510300.SH", start_date="20230101", end_date="20260807")

    def test_up_to_date(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(eff, "get_last_trade_date", lambda code: date(2026, 8, 7))
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [{}] * 5)
        assert eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="n") == 0


class TestEmFlowHelpers:
    def test_em_flow_trade_date(self) -> None:
        assert eff._em_flow_trade_date({"dataDate": "2026-08-07 15:00:00"}, fallback_iso="2026-08-06") == "2026-08-07"
        assert eff._em_flow_trade_date({"dataDate": "garbage"}, fallback_iso="2026-08-06") == "2026-08-06"
        assert eff._em_flow_trade_date({}, fallback_iso="2026-08-06") == "2026-08-06"

    def test_is_current_realtime_trade_date(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "last_open_date_on_or_before", lambda d: date(2026, 8, 7))
        assert eff._is_current_realtime_trade_date("2026-08-07", fallback_iso="2026-08-07")
        assert not eff._is_current_realtime_trade_date("2026-08-05", fallback_iso="2026-08-07")
        assert eff._is_current_realtime_trade_date("bad", fallback_iso="bad")
        assert not eff._is_current_realtime_trade_date("a", fallback_iso="b")

    def test_em_flow_to_daily_row(self) -> None:
        row = eff._em_flow_to_daily_row(ts_code="510300.SH", trade_date_iso="2026-08-07", flow={"mainNetInflow": 1.5, "fdShareWan": 2.0, "latestPrice": 4.0, "tradeTime": "15:00", "superLargeNetInflow": 0.5}, updated_at="n")
        assert row["net_inflow"] == 1.5
        assert row["source"] == EM_ETF_FLOW_SOURCE
        assert row["trade_time"] == "15:00"
        assert eff._em_flow_to_daily_row(ts_code="x", trade_date_iso="d", flow={"mainNetInflow": None}, updated_at="n") is None


class TestSyncWatchlist:
    def test_skip_today(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda **kw: True)
        assert eff.sync_etf_fund_flow_watchlist() == {"ok": True, "skipped": True, "message": "already synced today"}

    def test_should_skip(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "get_today_run", lambda job: {"success": True})
        monkeypatch.setattr(eff, "_is_shanghai_sync_window", lambda: False)
        assert eff._should_skip_etf_sync_today(force=False)
        assert not eff._should_skip_etf_sync_today(force=True)
        monkeypatch.setattr(eff, "get_today_run", lambda job: None)
        assert not eff._should_skip_etf_sync_today(force=False)
        monkeypatch.setattr(eff, "get_today_run", lambda job: {"success": False})
        assert not eff._should_skip_etf_sync_today(force=False)

    def test_sync_success(self, monkeypatch) -> None:
        flow = {"mainNetInflow": 1.5, "fdShareWan": 2.0, "latestPrice": 4.0, "dataDate": "2026-08-07"}
        monkeypatch.setattr(eff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda **kw: False)
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda syms: {s: dict(flow) for s in syms})
        monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: None)
        monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 5)
        monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: True)
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
        seen = {}
        monkeypatch.setattr(eff, "insert_record", lambda **kw: seen.update(kw))
        out = eff.sync_etf_fund_flow_watchlist()
        assert out["ok"] is True and out["updated"] == len(CORE)
        assert seen["job_type"] == "etf_fund_flow_watchlist" and seen["success"] is True

    def test_sync_fail_and_stale(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda **kw: False)
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda syms: {})
        monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: "boom")
        monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 0)
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: 0)
        monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
        out = eff.sync_etf_fund_flow_watchlist()
        assert out["ok"] is False and out["error"] == "boom"
        assert out["missingSymbols"] == [w["symbol"] for w in CORE]

        monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda syms: {s: {"dataDate": "2026-08-01"} for s in syms})
        monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: False)
        out = eff.sync_etf_fund_flow_watchlist()
        assert out["staleSymbols"] == [w["symbol"] for w in CORE]

    def test_sync_flow_missing_main_net(self, monkeypatch) -> None:
        good = {"mainNetInflow": 1.5, "dataDate": "2026-08-07"}
        monkeypatch.setattr(eff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda **kw: False)
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        flows = {w["symbol"]: dict(good) for w in CORE}
        flows[CORE[0]["symbol"]] = {"dataDate": "2026-08-07"}
        monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda syms: flows)
        monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: None)
        monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 0)
        monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: True)
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
        monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
        out = eff.sync_etf_fund_flow_watchlist()
        assert out["ok"] is True and out["updated"] == len(CORE) - 1
        assert CORE[0]["symbol"] in out["missingSymbols"]

    def test_sync_history_error_captured(self, monkeypatch) -> None:
        flow = {"mainNetInflow": 1.5, "dataDate": "2026-08-07"}
        monkeypatch.setattr(eff, "shanghai_today", lambda: date(2026, 8, 7))
        monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda **kw: False)
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda syms: {s: dict(flow) for s in syms})
        monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: None)
        monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: (_ for _ in ()).throw(RuntimeError("hist")))
        monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: True)
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
        monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
        out = eff.sync_etf_fund_flow_watchlist()
        assert out["ok"] is True and out["historyError"] == "hist"


class TestEstimate:
    def test_sum_net_inflow(self) -> None:
        rows = {"a": {"net_inflow": 1.0}, "b": {"net_inflow": "2.0"}}
        assert eff._sum_net_inflow_for_dates(rows, ["a", "b"]) == 3.0
        assert eff._sum_net_inflow_for_dates(rows, ["a", "c"]) is None
        assert eff._sum_net_inflow_for_dates(rows, ["a", "b", "c"]) is None
        assert eff._sum_net_inflow_for_dates(rows, []) is None
        rows2 = {"a": {"net_inflow": "bad"}}
        assert eff._sum_net_inflow_for_dates(rows2, ["a"]) is None
        rows3 = {"a": {"net_inflow": None}}
        assert eff._sum_net_inflow_for_dates(rows3, ["a"]) is None

    def test_latest_net_inflow_row(self) -> None:
        rows = {
            "2026-08-06": {"net_inflow": 1.0},
            "2026-08-05": {"net_inflow": None},
            "2026-08-04": {"net_inflow": "bad"},
            "2026-08-07": {"net_inflow": None},
        }
        open_iso = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07"]
        assert eff._latest_net_inflow_row(rows, open_iso, up_to="2026-08-07") == ("2026-08-06", 1.0)
        assert eff._latest_net_inflow_row(rows, open_iso, up_to="2026-08-03") == (None, None)
        assert eff._latest_net_inflow_row(rows, open_iso, up_to="2026-08-04") == (None, None)
        assert eff._latest_net_inflow_row(rows, open_iso, up_to="2026-08-02") == (None, None)
        assert eff._prev_open_iso(open_iso, "2026-08-07") == "2026-08-06"
        assert eff._prev_open_iso(open_iso, "2026-08-04") == "2026-08-03"

    def test_estimate_net_1d_from_em(self) -> None:
        rows = {"2026-08-06": {"fd_share": 100.0, "avg_price": 2.0}}
        open_iso = ["2026-08-06", "2026-08-07"]
        em = {"symbol": {"dataDate": "2026-08-07", "mainNetInflow": "5.0", "fdShareWan": 101.0}}
        assert eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em) == 5.0
        assert eff._estimate_net_1d_from_em(symbol="other", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em) is None
        em_stale = {"symbol": {"dataDate": "2026-08-06"}}
        assert eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em_stale) is None
        em_no_main = {"symbol": {"dataDate": "2026-08-07", "fdShareWan": 102.0}}
        v = eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em_no_main)
        assert v == pytest.approx(40_000.0)
        em_bad = {"symbol": {"dataDate": "2026-08-07", "fdShareWan": None}}
        assert eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em_bad) is None
        em_main_bad = {"symbol": {"dataDate": "2026-08-07", "mainNetInflow": "bad"}}
        assert eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-07", rows_by_date=rows, open_iso=open_iso, em_spot=em_main_bad) is None
        em_first = {"symbol": {"dataDate": "2026-08-06", "fdShareWan": 100.0}}
        assert eff._estimate_net_1d_from_em(symbol="symbol", as_of="2026-08-06", rows_by_date=rows, open_iso=["2026-08-06"], em_spot=em_first) is None

    def test_apply_em_spot_fallback(self, monkeypatch) -> None:
        rows = {}
        monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", lambda syms: {})
        assert not eff._apply_em_spot_fallback(ts_code="c", symbol="s", trade_date_iso="d", rows_by_date=rows, updated_at="n")
        monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", lambda syms: {"s": {"fdShareWan": None, "mainNetInflow": None}})
        assert not eff._apply_em_spot_fallback(ts_code="c", symbol="s", trade_date_iso="d", rows_by_date=rows, updated_at="n")
        monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", lambda syms: {"s": {"fdShareWan": 9.0, "mainNetInflow": 3.0}})
        seen = {}
        monkeypatch.setattr(eff, "upsert_daily_rows", lambda r: (seen.update(r=r) or 1))
        assert eff._apply_em_spot_fallback(ts_code="c", symbol="s", trade_date_iso="d", rows_by_date=rows, updated_at="n")
        assert seen["r"][0]["fd_share"] == 9.0 and seen["r"][0]["net_inflow"] == 3.0


class TestBundle:
    OPEN = [date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)]

    def _patch(self, monkeypatch, rows=None, open_dates=None, status=None, spot=None):
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "get_latest_date", lambda: "2026-08-07")
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: open_dates if open_dates is not None else self.OPEN)
        monkeypatch.setattr(eff, "compute_market_status", lambda: status or {"isMarketOpen": True})
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes, end_date=None: rows or [])
        monkeypatch.setattr(eff, "_is_shanghai_sync_window", lambda: True)
        monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", lambda syms: spot or {})

    def test_empty_as_of(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(eff, "get_latest_date", lambda: "")
        assert eff.build_etf_fund_flow_bundle(as_of_date="") == {"asOfDate": "", "shareLag": False, "intradaySafe": True, "items": []}

    def test_bad_as_of(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        out = eff.build_etf_fund_flow_bundle(as_of_date="not-a-date")
        assert out["asOfDate"] == "not-a-date" and out["items"] == []

    def test_normal_tushare_rows(self, monkeypatch) -> None:
        rows = []
        for i, spec in enumerate(CORE):
            rows.append({
                "ts_code": spec["ts_code"], "trade_date": "2026-08-07",
                "fd_share": 100.0 + i, "close": 4.0, "avg_price": 4.0,
                "net_inflow": 5.0 + i, "source": "", "trade_time": None,
                "main_net_inflow": None, "super_large_net_inflow": None,
                "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
            })
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert len(out["items"]) == len(CORE)
        assert out["shareLag"] is False and out["intradaySafe"] is True
        it = out["items"][0]
        assert it["netFlow1d"] == 5.0 and it["source"] == "tushare"
        assert it["flowStatus"] == "Historical"

    def test_em_realtime_rows(self, monkeypatch) -> None:
        rows = []
        for spec in CORE:
            rows.append({
                "ts_code": spec["ts_code"], "trade_date": "2026-08-07",
                "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
                "net_inflow": 5.0, "source": EM_ETF_FLOW_SOURCE, "trade_time": "14:00",
                "main_net_inflow": 5.0, "super_large_net_inflow": 1.0,
                "large_net_inflow": 2.0, "medium_net_inflow": 1.0, "small_net_inflow": 1.0,
            })
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        it = out["items"][0]
        assert it["live"] is True and it["flowStatus"] == "Live"
        assert it["flowProvider"] == "eastmoney"
        assert it["tradeTime"] == "14:00"

    def test_lag_and_spot(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-06",
            "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
            "net_inflow": 5.0, "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        it = out["items"][0]
        assert it["netFlow1dLagged"] == 5.0 and it["flowAsOfDate"] == "2026-08-06"

    def test_missing_market_closed(self, monkeypatch) -> None:
        self._patch(monkeypatch, status={"isMarketOpen": False})
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert all(it["flowStatus"] == "Missing" for it in out["items"])
        assert out["intradaySafe"] is False

    def test_szse_fallback_open_dates(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "get_latest_date", lambda: "2026-08-07")
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [] if exch == "SSE" else [date(2026, 8, 6)])
        monkeypatch.setattr(eff, "compute_market_status", lambda: {"isMarketOpen": True})
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes, end_date=None: [])
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert len(out["items"]) == len(CORE)

    def test_net3d_append_path(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-04",
            "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
            "net_inflow": 1.0, "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        monkeypatch.setattr(eff, "ensure_table", lambda: None)
        monkeypatch.setattr(eff, "get_latest_date", lambda: "2026-08-07")
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        monkeypatch.setattr(eff, "compute_market_status", lambda: {"isMarketOpen": True})
        monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes, end_date=None: rows)
        monkeypatch.setattr(eff, "_is_shanghai_sync_window", lambda: False)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert out["items"][0]["netFlow3d"] == pytest.approx(3.0)

    def test_eastmoney_source_and_mixed(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-07",
            "fd_share": None, "close": 4.0, "avg_price": 4.0,
            "net_inflow": 5.0, "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        it = out["items"][0]
        assert it["source"] == "eastmoney" and it["flowStatus"] == "Live"

    def test_bad_net_flow_value(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-07",
            "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
            "net_inflow": "bad", "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert out["items"][0]["flowStatus"] == "Missing"

    def test_market_closed_lagged(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-06",
            "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
            "net_inflow": 5.0, "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        self._patch(monkeypatch, rows=rows, status={"isMarketOpen": False})
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert out["items"][0]["flowStatus"] == "MarketClosed"

    def test_net3d_fallback_break(self, monkeypatch) -> None:
        rows = [{
            "ts_code": "510300.SH", "trade_date": "2026-08-04",
            "fd_share": 100.0, "close": 4.0, "avg_price": 4.0,
            "net_inflow": None, "source": "", "trade_time": None,
            "main_net_inflow": None, "super_large_net_inflow": None,
            "large_net_inflow": None, "medium_net_inflow": None, "small_net_inflow": None,
        }]
        self._patch(monkeypatch, rows=rows)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        assert out["items"][0]["netFlow3d"] is None

    def test_stale_em_fallback_spot(self, monkeypatch) -> None:
        spot = {spec["symbol"]: {"dataDate": "2026-08-07", "mainNetInflow": 9.0, "fdShareWan": 110.0} for spec in CORE}
        self._patch(monkeypatch, spot=spot)
        out = eff.build_etf_fund_flow_bundle(as_of_date="2026-08-07")
        it = out["items"][0]
        assert it["netFlow1d"] == 9.0
        assert it["source"] == EM_ETF_FLOW_SOURCE
        assert it["mainNetInflow"] == 9.0


class TestSignal:
    def test_aggregate_confirm(self) -> None:
        bundle = {
            "asOfDate": "2026-08-07", "intradaySafe": True, "shareLag": False,
            "items": [
                {"category": "broad", "signal": "National Team Buy"},
                {"category": "sector", "signal": "Sector Momentum"},
            ],
        }
        out = eff.aggregate_etf_flow_signal(bundle)
        assert out["verdict"] == "confirm" and out["confirmCount"] == 2
        assert out["incomplete"] is False

    def test_aggregate_contradict(self) -> None:
        bundle = {
            "asOfDate": "2026-08-07", "intradaySafe": True, "shareLag": True,
            "items": [
                {"category": "broad", "signal": "National Team Outflow"},
                {"category": "sector", "signal": "Inst Outflow"},
            ],
        }
        out = eff.aggregate_etf_flow_signal(bundle)
        assert out["verdict"] == "contradict" and out["incomplete"] is True

    def test_aggregate_mixed_and_neutral(self) -> None:
        bundle = {
            "asOfDate": "2026-08-07", "intradaySafe": False, "shareLag": False,
            "items": [
                {"category": "broad", "signal": "National Team Buy"},
                {"category": "broad", "signal": "National Team Outflow"},
                {"category": "sector", "signal": "Data Lag"},
            ],
        }
        out = eff.aggregate_etf_flow_signal(bundle)
        assert out["broadDirection"] == "mixed" and out["sectorDirection"] == "neutral"
        assert out["verdict"] == "neutral"
        assert eff.aggregate_etf_flow_signal({"items": [], "intradaySafe": True})["verdict"] == "neutral"
        assert eff.aggregate_etf_flow_signal({})["verdict"] == "neutral"

    def test_build_etf_flow_signal(self, monkeypatch) -> None:
        bundle = {"asOfDate": "2026-08-07", "intradaySafe": True, "shareLag": False, "items": []}
        monkeypatch.setattr(eff, "build_etf_fund_flow_bundle", lambda **kw: bundle)
        out = eff.build_etf_flow_signal(as_of_date="2026-08-07")
        assert out["asOfDate"] == "2026-08-07"


class TestPrevOpenDate:
    def test_prev_open_date(self, monkeypatch) -> None:
        monkeypatch.setattr(eff, "get_open_dates", lambda exch, s, d: [date(2026, 8, 4), date(2026, 8, 5), date(2026, 8, 6), date(2026, 8, 7)])
        assert eff._prev_open_date("SSE", date(2026, 8, 7)) == date(2026, 8, 6)
        assert eff._prev_open_date("SSE", date(2026, 8, 4)) is None

    def test_exchange_for_ts_code(self) -> None:
        assert eff._exchange_for_ts_code("510300.SH") == "SSE"
        assert eff._exchange_for_ts_code("159819.SZ") == "SZSE"
