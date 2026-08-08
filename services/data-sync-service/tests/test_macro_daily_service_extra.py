"""service/macro_daily.py coverage: helpers, resolvers, full sync."""

from __future__ import annotations

import sys
from datetime import date
from unittest.mock import Mock

import pandas as pd
import pytest

from data_sync_service.service import macro_daily as md


class TestHelpers:
    def test_dates(self) -> None:
        assert md._today_yyyymmdd().isdigit()
        assert md._date_to_yyyymmdd(date(2026, 8, 7)) == "20260807"

    def test_tushare_pro_raises(self, monkeypatch) -> None:
        monkeypatch.setattr(md, "get_settings", lambda: Mock(tu_share_api_key=""))
        with pytest.raises(RuntimeError, match="TU_SHARE_API_KEY"):
            md._tushare_pro()
        assert md.try_tushare_pro() is None

    def test_tushare_pro_ok(self, monkeypatch) -> None:
        pro = Mock()
        seen = {}
        monkeypatch.setattr(md, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(md.ts, "set_token", lambda k: seen.update(k=k))
        monkeypatch.setattr(md.ts, "pro_api", lambda k: pro)
        assert md._tushare_pro() is pro
        assert md.try_tushare_pro() is pro
        assert seen == {"k": "k"}

    def test_try_tushare_pro_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(md, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(md.ts, "set_token", lambda k: None)
        monkeypatch.setattr(md.ts, "pro_api", lambda k: (_ for _ in ()).throw(RuntimeError("x")))
        assert md.try_tushare_pro() is None

    def test_normalize_us(self) -> None:
        assert md._normalize_us_daily_df(None) is None
        assert md._normalize_us_daily_df(pd.DataFrame()) is not None
        df = pd.DataFrame([{"pct_change": 1.5}])
        out = md._normalize_us_daily_df(df)
        assert out["pct_chg"].iloc[0] == 1.5
        df2 = pd.DataFrame([{"pct_chg": 2.0}])
        assert md._normalize_us_daily_df(df2)["pct_chg"].iloc[0] == 2.0

    def test_normalize_fx(self) -> None:
        assert md._normalize_fx_daily_df(None) is None
        df = pd.DataFrame([{"bid_close": 1.0, "bid_open": 2.0, "bid_high": 3.0, "bid_low": 0.5}])
        out = md._normalize_fx_daily_df(df)
        assert out["close"].iloc[0] == 1.0
        assert out["open"].iloc[0] == 2.0
        assert out["high"].iloc[0] == 3.0
        assert out["low"].iloc[0] == 0.5


class TestPaged:
    def test_paged_index_global(self) -> None:
        pro = Mock()
        pro.index_global.side_effect = [
            pd.DataFrame([{"trade_date": "2023-05-01", "close": 1.0}]),
            pd.DataFrame([{"trade_date": "2024-01-02", "close": 2.0}]),
        ]
        out = md._paged_index_global(pro, "IXIC", "20230101", "20240201")
        assert out is not None and len(out) == 2
        assert pro.index_global.call_count == 2

    def test_paged_index_global_errors(self) -> None:
        pro = Mock()
        pro.index_global.side_effect = [RuntimeError("x"), pd.DataFrame([{"trade_date": "2023-01-02", "close": 1.0}])]
        out = md._paged_index_global(pro, "IXIC", "20230101", "20240201")
        assert out is not None and len(out) == 1
        pro.index_global.side_effect = [RuntimeError("x"), RuntimeError("y")]
        assert md._paged_index_global(pro, "IXIC", "20230101", "20230103") is None

    def test_paged_index_global_dedup(self) -> None:
        pro = Mock()
        pro.index_global.side_effect = [
            pd.DataFrame([{"trade_date": "2023-05-01", "close": 1.0}]),
            pd.DataFrame([{"trade_date": "2023-05-01", "close": 2.0}]),
        ]
        out = md._paged_index_global(pro, "IXIC", "20230101", "20240201")
        assert len(out) == 1 and out.iloc[0]["close"] == 2.0

    def test_paged_fut_daily(self) -> None:
        pro = Mock()
        pro.fut_daily.side_effect = [pd.DataFrame([{"trade_date": "2023-05-01", "close": 1.0}])]
        out = md._paged_fut_daily(pro, "AU9999", "20230101", "20230102")
        assert out is not None and len(out) == 1
        pro.fut_daily.side_effect = [RuntimeError("x"), RuntimeError("y")]
        assert md._paged_fut_daily(pro, "AU9999", "20230101", "20230103") is None


class FakeAk:
    @staticmethod
    def stock_hk_index_daily_sina(symbol="HSTECH"):
        return pd.DataFrame([
            {"date": "2026-08-07", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100.0, "amount": 200.0},
            {"date": "2020-01-01", "open": 1.0, "high": 2.0, "low": 0.5, "close": None, "volume": 100.0, "amount": 200.0},
        ])


class FakeTicker:
    @staticmethod
    def history(start=None, end=None):
        return pd.DataFrame([
            {"Date": pd.Timestamp("2026-08-07"), "Open": 1.0, "High": 2.0, "Low": 0.5, "Close": 1.5, "Volume": 100},
            {"Date": pd.Timestamp("2020-01-01"), "Open": 1.0, "High": 2.0, "Low": 0.5, "Close": None, "Volume": 100},
        ])


class FakeYf:
    @staticmethod
    def Ticker(symbol):
        return FakeTicker()


class TestHstech:
    def test_ak_darwin(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "darwin")
        assert md._fetch_hstech_bars_via_ak("20230101", "20260808") is None

    def test_ak_import_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "linux")
        monkeypatch.setitem(sys.modules, "akshare", None)
        assert md._fetch_hstech_bars_via_ak("20230101", "20260808") is None
        monkeypatch.delitem(sys.modules, "akshare", raising=False)

    def test_ak_raise(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "linux")
        ak = Mock()
        ak.stock_hk_index_daily_sina.side_effect = RuntimeError("x")
        monkeypatch.setitem(sys.modules, "akshare", ak)
        assert md._fetch_hstech_bars_via_ak("20230101", "20260808") is None

    def test_ak_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "linux")
        monkeypatch.setitem(sys.modules, "akshare", FakeAk)
        out = md._fetch_hstech_bars_via_ak("20230101", "20260808")
        assert out is not None
        assert out["trade_date"].iloc[0] == "2026-08-07"
        assert out["close"].iloc[0] == 1.5
        assert len(out) == 1

    def test_ak_empty_df(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "linux")
        ak = Mock()
        ak.stock_hk_index_daily_sina.return_value = pd.DataFrame()
        monkeypatch.setitem(sys.modules, "akshare", ak)
        assert md._fetch_hstech_bars_via_ak("20230101", "20260808") is None
        ak.stock_hk_index_daily_sina.return_value = pd.DataFrame([{"date": "2026-08-07", "close": None}])
        assert md._fetch_hstech_bars_via_ak("20230101", "20260808") is None

    def test_yf_empty_hist(self, monkeypatch) -> None:
        yf = Mock()
        yf.Ticker.return_value.history.return_value = pd.DataFrame()
        monkeypatch.setitem(sys.modules, "yfinance", yf)
        assert md._fetch_hstech_bars_via_yf("20230101", "20260808") is None
        yf.Ticker.return_value.history.return_value = pd.DataFrame([{"Date": pd.Timestamp("2026-08-07"), "Close": None}])
        assert md._fetch_hstech_bars_via_yf("20230101", "20260808") is None

    def test_resolver_sort_failure(self, monkeypatch) -> None:
        df = pd.DataFrame([{"ts_code": "FTXA50", "name": "FTSE A50", "list_date": "2020-01-01"}])
        pro = Mock()
        pro.fut_basic.return_value = df
        orig = pd.DataFrame.sort_values
        monkeypatch.setattr(pd.DataFrame, "sort_values", lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("no sort")))
        assert md.resolve_sgx_a50_main(pro) == "FTXA50"
        assert md.resolve_main_fut_by_prefix(pro, "SGX", "FT") == "FTXA50"
        monkeypatch.setattr(pd.DataFrame, "sort_values", orig)
        pro2 = Mock()
        pro2.fut_basic.return_value = pd.DataFrame([{"ts_code": "SC2601.INE", "name": "x", "list_date": "2025-01-01"}])
        monkeypatch.setattr(pd.DataFrame, "sort_values", lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("no sort")))
        assert md.resolve_ine_sc_main(pro2) == "SC2601.INE"

    def test_ak_out_of_window(self, monkeypatch) -> None:
        monkeypatch.setattr(md.sys, "platform", "linux")
        monkeypatch.setitem(sys.modules, "akshare", FakeAk)
        assert md._fetch_hstech_bars_via_ak("20230101", "20230102") is None

    def test_yf_import_fail(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "yfinance", None)
        assert md._fetch_hstech_bars_via_yf("20230101", "20260808") is None

    def test_yf_raise_and_empty(self, monkeypatch) -> None:
        yf = Mock()
        yf.Ticker.side_effect = RuntimeError("x")
        monkeypatch.setitem(sys.modules, "yfinance", yf)
        assert md._fetch_hstech_bars_via_yf("20230101", "20260808") is None
        yf.Ticker.return_value = Mock(history=Mock(side_effect=RuntimeError("y")))
        assert md._fetch_hstech_bars_via_yf("20230101", "20260808") is None

    def test_yf_ok(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "yfinance", FakeYf)
        out = md._fetch_hstech_bars_via_yf("20230101", "20260808")
        assert out is not None
        assert out["trade_date"].iloc[0] == "2026-08-07"
        assert out["close"].iloc[0] == 1.5


class TestResolvers:
    def test_resolve_sgx_a50_main(self) -> None:
        pro = Mock()
        pro.fut_basic.side_effect = RuntimeError("x")
        assert md.resolve_sgx_a50_main(pro) is None
        pro.fut_basic.side_effect = None
        pro.fut_basic.return_value = pd.DataFrame()
        assert md.resolve_sgx_a50_main(pro) is None
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": "X"}])
        assert md.resolve_sgx_a50_main(pro) is None
        df = pd.DataFrame([
            {"ts_code": "FTXA50", "name": "FTSE China A50", "list_date": "2020-01-01"},
            {"ts_code": "FTXCN2", "name": "CN futures", "list_date": "2021-01-01"},
            {"ts_code": "OTHR", "name": "Other", "list_date": "2022-01-01"},
        ])
        pro.fut_basic.return_value = df
        assert md.resolve_sgx_a50_main(pro) == "FTXCN2"
        df_no_match = pd.DataFrame([{"ts_code": "OTHR", "name": "Other", "list_date": "2022-01-01"}])
        pro.fut_basic.return_value = df_no_match
        assert md.resolve_sgx_a50_main(pro) == "OTHR"
        df_nan = pd.DataFrame([{"ts_code": None, "name": "x"}])
        pro.fut_basic.return_value = df_nan
        assert md.resolve_sgx_a50_main(pro) is None
        df_nan2 = pd.DataFrame([{"ts_code": float("nan"), "name": "x"}])
        pro.fut_basic.return_value = df_nan2
        assert md.resolve_sgx_a50_main(pro) is None

    def test_resolve_main_fut_by_prefix(self) -> None:
        pro = Mock()
        pro.fut_basic.side_effect = RuntimeError("x")
        assert md.resolve_main_fut_by_prefix(pro, "SHFE", "AU") is None
        pro.fut_basic.side_effect = None
        pro.fut_basic.return_value = pd.DataFrame()
        assert md.resolve_main_fut_by_prefix(pro, "SHFE", "AU") is None
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": "X"}])
        assert md.resolve_main_fut_by_prefix(pro, "SHFE", "AU") is None
        df = pd.DataFrame([
            {"ts_code": "AU2512.SHF", "name": "au2512", "list_date": "2024-01-01"},
            {"ts_code": "CU2601.SHF", "name": "cu2601", "list_date": "2025-01-01"},
        ])
        pro.fut_basic.return_value = df
        assert md.resolve_main_fut_by_prefix(pro, "SHFE", "au") == "AU2512.SHF"
        df_none = pd.DataFrame([{"ts_code": None}])
        pro.fut_basic.return_value = df_none
        assert md.resolve_main_fut_by_prefix(pro, "SHFE", "AU") is None

    def test_resolve_ine_sc_main(self, monkeypatch) -> None:
        pro = Mock()
        pro.fut_basic.side_effect = RuntimeError("x")
        assert md.resolve_ine_sc_main(pro) is None
        pro.fut_basic.side_effect = None
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": "SC2601.INE", "name": "原油", "list_date": "2025-01-01"}])
        assert md.resolve_ine_sc_main(pro) == "SC2601.INE"
        pro.fut_basic.side_effect = [pd.DataFrame([{"ts_code": "OTHER.INE", "name": "x"}]), RuntimeError("x")]
        assert md.resolve_ine_sc_main(pro) is None
        pro.fut_basic.side_effect = [pd.DataFrame([{"ts_code": "OTHER.INE", "name": "x"}]), pd.DataFrame([{"ts_code": "SC2601.INE", "name": "y", "list_date": "2025-01-01"}])]
        orig_sort = pd.DataFrame.sort_values
        monkeypatch.setattr(pd.DataFrame, "sort_values", lambda self, *a, **k: (_ for _ in ()).throw(RuntimeError("no sort")))
        assert md.resolve_ine_sc_main(pro) == "SC2601.INE"
        monkeypatch.setattr(pd.DataFrame, "sort_values", orig_sort)
        pro.fut_basic.side_effect = None
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": "X", "name": "y"}])
        assert md.resolve_ine_sc_main(pro) is None
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": "OTHER.INE", "name": "原油"}])
        assert md.resolve_ine_sc_main(pro) == "OTHER.INE"
        pro.fut_basic.return_value = pd.DataFrame([{"ts_code": None}])
        assert md.resolve_ine_sc_main(pro) is None


def _ok_df():
    return pd.DataFrame([{"trade_date": "2026-08-07", "close": 1.0}])


class TestSyncFull:
    def test_already_synced(self, monkeypatch) -> None:
        monkeypatch.setattr(md, "get_today_run", lambda job: {"success": True})
        assert md.sync_macro_daily_full() == {"ok": True, "skipped": True, "message": "already synced today"}

    def test_no_api_key(self, monkeypatch) -> None:
        monkeypatch.setattr(md, "get_today_run", lambda job: None)
        monkeypatch.setattr(md, "get_settings", lambda: Mock(tu_share_api_key=""))
        assert md.sync_macro_daily_full() == {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    def _happy(self, monkeypatch, *, fail=None, last_ts=None):
        monkeypatch.setattr(md, "get_today_run", lambda job: {"success": False, "last_ts_code": last_ts})
        monkeypatch.setattr(md, "get_settings", lambda: Mock(tu_share_api_key="k"))
        monkeypatch.setattr(md, "get_last_trade_date", lambda sid: None)
        monkeypatch.setattr(md, "_paged_index_global", lambda pro, code, s, e: _ok_df())
        monkeypatch.setattr(md, "_paged_fut_daily", lambda pro, code, s, e: _ok_df())
        monkeypatch.setattr(md, "resolve_sgx_a50_main", lambda pro: "FTXA50")
        monkeypatch.setattr(md, "resolve_ine_sc_main", lambda pro: "SC2601.INE")
        monkeypatch.setattr(md, "resolve_main_fut_by_prefix", lambda pro, exch, pre: "AU2512.SHF")
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_ak", lambda s, e: None)
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_yf", lambda s, e: _ok_df())
        upsert = Mock(side_effect=lambda df, series_id=None, source=None, underlying_ts_code=None: len(df))
        monkeypatch.setattr(md, "upsert_from_dataframe", upsert)
        pro = Mock()
        pro.fx_daily.return_value = pd.DataFrame([{"trade_date": "2026-08-07", "bid_close": 7.0}])
        monkeypatch.setattr(md, "_tushare_pro", lambda: pro)
        seen = {}
        monkeypatch.setattr(md, "insert_record", lambda **kw: seen.update(kw))
        return pro, upsert, seen

    def test_full_success(self, monkeypatch) -> None:
        pro, upsert, seen = self._happy(monkeypatch)
        out = md.sync_macro_daily_full()
        assert out["ok"] is True and out["updated"] == 10
        assert seen == {"job_type": md.JOB_TYPE, "success": True, "last_ts_code": None, "error_message": None}
        assert pro.fx_daily.call_count == 1

    def test_resume_from_last(self, monkeypatch) -> None:
        _, upsert, _ = self._happy(monkeypatch, last_ts="SPX")
        md.sync_macro_daily_full()
        assert upsert.call_count == 7

    def test_failure_records(self, monkeypatch) -> None:
        _, upsert, seen = self._happy(monkeypatch)
        upsert.side_effect = RuntimeError("boom")
        out = md.sync_macro_daily_full()
        assert out["ok"] is False and out["error"] == "boom"
        assert seen["success"] is False and seen["error_message"].startswith("IXIC:")

    def test_up_to_date(self, monkeypatch) -> None:
        _, upsert, seen = self._happy(monkeypatch)
        monkeypatch.setattr(md, "get_last_trade_date", lambda sid: date.today())
        out = md.sync_macro_daily_full()
        assert out["ok"] is True and out["updated"] == 0
        assert seen["success"] is True

    def test_all_empty(self, monkeypatch) -> None:
        pro, upsert, seen = self._happy(monkeypatch)
        monkeypatch.setattr(md, "_paged_index_global", lambda pro, code, s, e: None)
        monkeypatch.setattr(md, "_paged_fut_daily", lambda pro, code, s, e: None)
        monkeypatch.setattr(md, "resolve_sgx_a50_main", lambda pro: None)
        monkeypatch.setattr(md, "resolve_main_fut_by_prefix", lambda pro, exch, pre: None)
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_yf", lambda s, e: None)
        pro.fx_daily.return_value = pd.DataFrame()
        out = md.sync_macro_daily_full()
        assert out["ok"] is True and out["updated"] == 0

    def test_fx_raise_and_a50_xin9(self, monkeypatch) -> None:
        pro, upsert, seen = self._happy(monkeypatch)
        pro.fx_daily.side_effect = RuntimeError("fx down")
        monkeypatch.setattr(md, "_paged_fut_daily", lambda pro2, code, s, e: None if code == "FTXA50" else _ok_df())
        monkeypatch.setattr(md, "_paged_index_global", lambda pro2, code, s, e: _ok_df() if code == "XIN9" else None)
        out = md.sync_macro_daily_full()
        assert out["ok"] is True
        xin9 = [c.kwargs for c in upsert.call_args_list if c.kwargs.get("underlying_ts_code") == "XIN9"]
        assert len(xin9) == 1

    def test_hstech_ak_fallback(self, monkeypatch) -> None:
        _, upsert, _ = self._happy(monkeypatch)
        monkeypatch.setattr(md, "get_today_run", lambda job: None)
        monkeypatch.setattr(md, "_paged_index_global", lambda pro2, code, s, e: None if code == "HSTECH" else _ok_df())
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_ak", lambda s, e: _ok_df())
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_yf", lambda s, e: _ok_df())
        md.sync_macro_daily_full()
        ak = [c.kwargs for c in upsert.call_args_list if c.kwargs.get("source") == "akshare"]
        assert len(ak) == 1

    def test_hstech_yf_fallback(self, monkeypatch) -> None:
        _, upsert, _ = self._happy(monkeypatch)
        monkeypatch.setattr(md, "get_today_run", lambda job: None)
        monkeypatch.setattr(md, "_paged_index_global", lambda pro2, code, s, e: None if code == "HSTECH" else _ok_df())
        monkeypatch.setattr(md.sys, "platform", "linux")
        monkeypatch.setitem(sys.modules, "akshare", Mock())
        monkeypatch.setattr(md, "_fetch_hstech_bars_via_yf", lambda s, e: _ok_df())
        md.sync_macro_daily_full()
        yf = [c.kwargs for c in upsert.call_args_list if c.kwargs.get("source") == "yfinance"]
        assert len(yf) == 1

    def test_resume_unknown_series(self, monkeypatch) -> None:
        _, upsert, _ = self._happy(monkeypatch, last_ts="NOT_A_SERIES")
        md.sync_macro_daily_full()
        assert upsert.call_count == 10

    def test_comm_no_underlying(self, monkeypatch) -> None:
        _, upsert, _ = self._happy(monkeypatch)
        monkeypatch.setattr(md, "resolve_ine_sc_main", lambda pro2: None)
        monkeypatch.setattr(md, "resolve_main_fut_by_prefix", lambda pro2, exch, pre: None)
        monkeypatch.setattr(md, "_paged_fut_daily", lambda pro2, code, s, e: _ok_df())
        monkeypatch.setattr(md, "_paged_index_global", lambda pro2, code, s, e: _ok_df())
        out = md.sync_macro_daily_full()
        assert out["ok"] is True and out["updated"] == 7
