"""service/macro_snapshot_on_demand.py coverage."""

from __future__ import annotations

import sys
from datetime import date, timedelta
from unittest.mock import Mock

import pandas as pd
import pytest

from data_sync_service.service import macro_snapshot_on_demand as mod
from data_sync_service.service.macro_daily import (
    SID_A50,
    SID_COMM_COPPER,
    SID_COMM_ENERGY,
    SID_COMM_GOLD,
    SID_DJI,
    SID_HSI,
    SID_HSTECH,
    SID_IXIC,
    SID_SPX,
    SID_USDCNH,
)


def _hist_df(n=25):
    idx = pd.bdate_range(end=pd.Timestamp(date.today()), periods=n)
    return pd.DataFrame({"Close": [100.0 + i for i in range(len(idx))]}, index=idx)


class TestLookback:
    def test_range(self) -> None:
        s, e = mod._lookback_range(120)
        assert len(s) == 8 and len(e) == 8
        s2, e2 = mod._lookback_range()
        assert s2 <= e2

    def test_stale(self, monkeypatch) -> None:
        assert mod._is_data_stale(None)
        assert mod._is_data_stale("bad-date")
        today = date.today().isoformat()
        assert not mod._is_data_stale(today)
        old = (date.today() - timedelta(days=5)).isoformat()
        assert mod._is_data_stale(old)


class TestYfinance:
    def test_import_fail(self, monkeypatch) -> None:
        monkeypatch.setitem(sys.modules, "yfinance", None)
        assert mod._fetch_yfinance_index("^IXIC") is None

    def test_raise(self, monkeypatch) -> None:
        yf = Mock()
        yf.Ticker.side_effect = RuntimeError("x")
        monkeypatch.setitem(sys.modules, "yfinance", yf)
        assert mod._fetch_yfinance_index("^IXIC") is None

    def test_empty_and_short(self, monkeypatch) -> None:
        class Yf:
            @staticmethod
            def Ticker(t):
                return Mock(history=Mock(return_value=pd.DataFrame()))

        monkeypatch.setitem(sys.modules, "yfinance", Yf)
        assert mod._fetch_yfinance_index("^IXIC") is None

        class Yf2:
            @staticmethod
            def Ticker(t):
                return Mock(history=Mock(return_value=_hist_df(3)))

        monkeypatch.setitem(sys.modules, "yfinance", Yf2)
        assert mod._fetch_yfinance_index("^IXIC") is None

    def test_ok(self, monkeypatch) -> None:
        hist = _hist_df(25)
        class Yf:
            @staticmethod
            def Ticker(t):
                return Mock(history=Mock(return_value=hist))

        monkeypatch.setitem(sys.modules, "yfinance", Yf)
        out = mod._fetch_yfinance_index("^IXIC")
        n = len(hist)
        assert out["close"] == pytest.approx(100.0 + n - 1)
        assert out["pctChg"] == pytest.approx(100.0 / (100.0 + n - 2))
        assert out["ma5"] is not None and out["ma20"] is not None
        assert out["asOfDate"] == hist.index[-1].strftime("%Y-%m-%d")

    def test_bad_closes(self, monkeypatch) -> None:
        idx = pd.bdate_range(end=pd.Timestamp(date.today()), periods=25)
        df = pd.DataFrame({"Close": ["x"] * len(idx)}, index=idx)
        class Yf:
            @staticmethod
            def Ticker(t):
                return Mock(history=Mock(return_value=df))

        monkeypatch.setitem(sys.modules, "yfinance", Yf)
        assert mod._fetch_yfinance_index("^IXIC") is None


class TestSina:
    def test_darwin(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "darwin")
        assert mod._fetch_hstech_via_sina() is None

    def test_import_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "linux")
        monkeypatch.setitem(sys.modules, "akshare", None)
        assert mod._fetch_hstech_via_sina() is None

    def test_raise_and_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "linux")
        ak = Mock()
        ak.stock_hk_index_daily_sina.side_effect = RuntimeError("x")
        monkeypatch.setitem(sys.modules, "akshare", ak)
        assert mod._fetch_hstech_via_sina() is None
        ak.stock_hk_index_daily_sina.side_effect = None
        ak.stock_hk_index_daily_sina.return_value = pd.DataFrame()
        assert mod._fetch_hstech_via_sina() is None

    def test_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "linux")
        df = pd.DataFrame({
            "date": ["2026-08-07", "2026-08-06", "2026-08-05"],
            "close": [1.0, 2.0, 3.0],
        })
        class Ak:
            @staticmethod
            def stock_hk_index_daily_sina(symbol="HSTECH"):
                return df

        monkeypatch.setitem(sys.modules, "akshare", Ak)
        out = mod._fetch_hstech_via_sina()
        assert out["close"] == pytest.approx(3.0)
        assert out["asOfDate"] == "2026-08-05"

    def test_no_closes(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "linux")
        class Ak:
            @staticmethod
            def stock_hk_index_daily_sina(symbol="HSTECH"):
                return pd.DataFrame({"date": ["2026-08-07"], "close": ["bad"]})

        monkeypatch.setitem(sys.modules, "akshare", Ak)
        assert mod._fetch_hstech_via_sina() is None

    def test_bad_asof(self, monkeypatch) -> None:
        monkeypatch.setattr(mod.sys, "platform", "linux")
        class Ak:
            @staticmethod
            def stock_hk_index_daily_sina(symbol="HSTECH"):
                return pd.DataFrame({"date": ["garbage"], "close": [1.0]})

        monkeypatch.setitem(sys.modules, "akshare", Ak)
        assert mod._fetch_hstech_via_sina() is None


class TestDfToMetrics:
    def test_empty(self) -> None:
        assert mod._df_to_metrics(None) == {}
        assert mod._df_to_metrics(pd.DataFrame()) == {}
        assert mod._df_to_metrics(pd.DataFrame([{"close": 1.0}])) == {}
        assert mod._df_to_metrics(pd.DataFrame([{"trade_date": "20260807"}])) == {}

    def test_settle_fillna(self) -> None:
        df = pd.DataFrame([
            {"trade_date": "20260806", "close": None, "settle": 2.0},
            {"trade_date": "20260807", "close": 1.0, "settle": 3.0},
        ])
        out = mod._df_to_metrics(df)
        assert out["close"] == pytest.approx(1.0)
        assert out["asOfDate"] == "2026-08-07"

    def test_settle_only(self) -> None:
        df = pd.DataFrame([{"trade_date": "20260807", "settle": 5.0}])
        out = mod._df_to_metrics(df)
        assert out["close"] == pytest.approx(5.0)

    def test_bad_dates_fallback(self) -> None:
        df = pd.DataFrame([{"trade_date": "2026-08-07", "close": 1.0}])
        out = mod._df_to_metrics(df)
        assert out["close"] == pytest.approx(1.0)

    def test_pct_from_cols(self) -> None:
        df = pd.DataFrame([{"trade_date": "20260807", "close": 1.0, "pct_chg": 2.5}])
        out = mod._df_to_metrics(df)
        assert out["pctChg"] == pytest.approx(2.5)
        df2 = pd.DataFrame([{"trade_date": "20260807", "close": 1.0, "pct_change": 3.5}])
        assert mod._df_to_metrics(df2)["pctChg"] == pytest.approx(3.5)
        df3 = pd.DataFrame([{"trade_date": "20260807", "close": 1.0, "pct_chg": "bad"}])
        assert mod._df_to_metrics(df3)["pctChg"] is None
        df4 = pd.DataFrame([{"trade_date": "20260807", "close": 1.0, "pct_chg": None}])
        assert mod._df_to_metrics(df4)["pctChg"] is None

    def test_pct_computed(self) -> None:
        df = pd.DataFrame([
            {"trade_date": "20260806", "close": 100.0},
            {"trade_date": "20260807", "close": 110.0},
        ])
        out = mod._df_to_metrics(df)
        assert out["pctChg"] == pytest.approx(10.0)

    def test_no_closes(self) -> None:
        df = pd.DataFrame([{"trade_date": "20260807", "close": "x"}])
        assert mod._df_to_metrics(df) == {}

    def test_all_dates_dropped(self) -> None:
        df = pd.DataFrame([{"trade_date": "not-a-date", "close": 1.0}])
        assert mod._df_to_metrics(df) == {}


class TestOnDemandSeries:
    def test_ixic_yf(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_ixic_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_IXIC)
        assert metrics == {} and src is None and und is None

        monkeypatch.setattr(mod, "_fetch_ixic_via_yfinance", lambda: {"close": 1.0})
        metrics, src, und = mod._fetch_on_demand_series(None, SID_IXIC)
        assert src == "yfinance.on_demand" and und == "IXIC"

    def test_ixic_tushare(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_ixic_via_yfinance", lambda: None)
        pro = Mock()
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_IXIC)
        assert src == "tushare.index_global.on_demand" and metrics["close"] == 1.0
        pro.index_global.return_value = pd.DataFrame()
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_IXIC)
        assert src is None and metrics == {}

    def test_dji_tushare_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_dji_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_DJI)
        assert metrics == {} and src is None
        pro = Mock()
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_DJI)
        assert und == "DJI" and src == "tushare.index_global.on_demand"

    def test_spx_tushare_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_spx_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_SPX)
        assert metrics == {} and src is None
        pro = Mock()
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_SPX)
        assert und == "SPX" and src == "tushare.index_global.on_demand"

    def test_hsi_tushare_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_hsi_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_HSI)
        assert metrics == {} and src is None
        pro = Mock()
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_HSI)
        assert und == "HSI" and src == "tushare.index_global.on_demand"

    def test_hstech_tushare_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_hstech_via_sina", lambda: None)
        monkeypatch.setattr(mod, "_fetch_hstech_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_HSTECH)
        assert metrics == {} and src is None
        pro = Mock()
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_HSTECH)
        assert und == "HSTECH" and src == "tushare.index_global.on_demand"

    def test_copper_no_underlying(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "resolve_main_fut_by_prefix", lambda pro2, e, p: None)
        metrics, src, und = mod._fetch_on_demand_series(Mock(), SID_COMM_COPPER)
        assert metrics == {} and src is None

    def test_wrapper_functions(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_yfinance_index", lambda t: {"close": 1.0, "ticker": t})
        assert mod._fetch_ixic_via_yfinance()["ticker"] == "^IXIC"
        assert mod._fetch_dji_via_yfinance()["ticker"] == "^DJI"
        assert mod._fetch_spx_via_yfinance()["ticker"] == "^GSPC"
        assert mod._fetch_hsi_via_yfinance()["ticker"] == "^HSI"
        assert mod._fetch_hstech_via_yfinance()["ticker"] == "^HSTECH"

    def test_dji_spx_hsi_yf(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_dji_via_yfinance", lambda: {"close": 1.0})
        metrics, src, und = mod._fetch_on_demand_series(None, SID_DJI)
        assert und == "DJI"
        monkeypatch.setattr(mod, "_fetch_spx_via_yfinance", lambda: {"close": 1.0})
        _, _, und = mod._fetch_on_demand_series(None, SID_SPX)
        assert und == "SPX"
        monkeypatch.setattr(mod, "_fetch_hsi_via_yfinance", lambda: {"close": 1.0})
        _, _, und = mod._fetch_on_demand_series(None, SID_HSI)
        assert und == "HSI"

    def test_hstech_sina_then_yf(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "_fetch_hstech_via_sina", lambda: {"close": 1.0})
        _, src, und = mod._fetch_on_demand_series(None, SID_HSTECH)
        assert src == "akshare.on_demand" and und == "HSTECH"
        monkeypatch.setattr(mod, "_fetch_hstech_via_sina", lambda: None)
        monkeypatch.setattr(mod, "_fetch_hstech_via_yfinance", lambda: {"close": 1.0})
        _, src, und = mod._fetch_on_demand_series(None, SID_HSTECH)
        assert src == "yfinance.on_demand"
        monkeypatch.setattr(mod, "_fetch_hstech_via_yfinance", lambda: None)
        metrics, src, und = mod._fetch_on_demand_series(None, SID_HSTECH)
        assert metrics == {} and src is None

    def test_usdcnh(self, monkeypatch) -> None:
        pro = Mock()
        pro.fx_daily.return_value = pd.DataFrame([{"trade_date": "20260807", "bid_close": 7.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_USDCNH)
        assert und == "USDCNH.FXCM" and metrics["close"] == 7.0
        pro.fx_daily.return_value = pd.DataFrame()
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_USDCNH)
        assert metrics == {} and src is None

    def test_a50_fut_and_xin9(self, monkeypatch) -> None:
        pro = Mock()
        monkeypatch.setattr(mod, "resolve_sgx_a50_main", lambda pro2: "FTXA50")
        pro.fut_daily.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_A50)
        assert und == "FTXA50" and src == "tushare.fut_daily.on_demand"
        monkeypatch.setattr(mod, "resolve_sgx_a50_main", lambda pro2: None)
        pro.index_global.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 2.0}])
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_A50)
        assert und == "XIN9"

    def test_comm_series(self, monkeypatch) -> None:
        pro = Mock()
        pro.fut_daily.return_value = pd.DataFrame([{"trade_date": "20260807", "close": 1.0}])
        monkeypatch.setattr(mod, "resolve_ine_sc_main", lambda pro2: "SC2601.INE")
        _, src, und = mod._fetch_on_demand_series(pro, SID_COMM_ENERGY)
        assert und == "SC2601.INE"
        monkeypatch.setattr(mod, "resolve_ine_sc_main", lambda pro2: None)
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_COMM_ENERGY)
        assert metrics == {} and src is None
        monkeypatch.setattr(mod, "resolve_main_fut_by_prefix", lambda pro2, e, p: "AU2512.SHF")
        _, _, und = mod._fetch_on_demand_series(pro, SID_COMM_GOLD)
        assert und == "AU2512.SHF"
        monkeypatch.setattr(mod, "resolve_main_fut_by_prefix", lambda pro2, e, p: None)
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_COMM_GOLD)
        assert metrics == {}
        monkeypatch.setattr(mod, "resolve_main_fut_by_prefix", lambda pro2, e, p: "CU2601.SHF")
        _, _, und = mod._fetch_on_demand_series(pro, SID_COMM_COPPER)
        assert und == "CU2601.SHF"

    def test_pro_none_defaults(self, monkeypatch) -> None:
        metrics, src, und = mod._fetch_on_demand_series(None, "UNKNOWN")
        assert metrics == {} and src is None and und is None
        metrics, src, und = mod._fetch_on_demand_series(Mock(), "UNKNOWN")
        assert metrics == {} and src is None and und is None

    def test_exception_caught(self, monkeypatch) -> None:
        pro = Mock()
        monkeypatch.setattr(mod, "resolve_sgx_a50_main", lambda pro2: "FTXA50")
        pro.fut_daily.side_effect = RuntimeError("boom")
        metrics, src, und = mod._fetch_on_demand_series(pro, SID_A50)
        assert metrics == {} and src is None


class TestEnrich:
    def test_no_fetch(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "try_tushare_pro", lambda: None)
        items = [{"seriesId": "X", "close": 1.0, "realtime": True, "asOfDate": date.today().isoformat()}]
        out = mod.enrich_macro_items_on_demand(items)
        assert out == items

    def test_fetch_all_paths(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "try_tushare_pro", lambda: None)
        seen = {}
        monkeypatch.setattr(mod, "_fetch_on_demand_series", lambda pro, sid: seen.update(sid=sid) or ({"close": 9.0, "pctChg": 1.0, "asOfDate": "2026-08-07", "ma5": 1.0, "ma20": 2.0}, "src", "und"))
        items = [
            {"seriesId": "X", "close": None},
            {"seriesId": SID_IXIC, "close": 1.0},
            {"seriesId": "Y", "close": 1.0, "asOfDate": "2020-01-01"},
        ]
        out = mod.enrich_macro_items_on_demand(items)
        assert out[0]["close"] == 9.0
        assert out[0]["source"] == "src"
        assert out[0]["underlyingTsCode"] == "und"
        assert out[0]["dataSource"] == "on_demand"

    def test_empty_metrics_skipped(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "try_tushare_pro", lambda: None)
        monkeypatch.setattr(mod, "_fetch_on_demand_series", lambda pro, sid: ({}, None, None))
        items = [{"seriesId": "X", "close": None}]
        out = mod.enrich_macro_items_on_demand(items)
        assert out[0].get("close") is None


class TestPublic:
    def test_fetch_hk_index(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "try_tushare_pro", lambda: None)
        monkeypatch.setattr(mod, "_fetch_on_demand_series", lambda pro, sid: ({"close": 1.0}, "yf", "HSTECH"))
        metrics, src = mod.fetch_hk_index_on_demand(SID_HSTECH)
        assert metrics == {"close": 1.0} and src == "yf"

    def test_warning(self, monkeypatch) -> None:
        monkeypatch.setattr(mod, "get_settings", lambda: Mock(tu_share_api_key=""))
        assert "TU_SHARE_API_KEY" in mod.macro_snapshot_warning()
        monkeypatch.setattr(mod, "get_settings", lambda: Mock(tu_share_api_key="k"))
        assert mod.macro_snapshot_warning() is None
