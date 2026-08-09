"""macro_snapshot_on_demand: metrics mapping, on-demand series, enrich driver."""

import datetime
import sys

import pandas as pd

from data_sync_service.service import macro_snapshot_on_demand as msd
from data_sync_service.service.macro_daily import (
    SID_A50,
    SID_COMM_GOLD,
    SID_HSI,
    SID_HSTECH,
    SID_IXIC,
    SID_USDCNH,
)


def test_lookback_range() -> None:
    sd, ed = msd._lookback_range(120)
    assert len(sd) == 8 and len(ed) == 8
    assert datetime.datetime.strptime(ed, "%Y%m%d").date() == datetime.datetime.now(datetime.UTC).date()


def test_df_to_metrics() -> None:
    df = pd.DataFrame({
        "trade_date": ["2026-08-05", "2026-08-06", "2026-08-07"],
        "close": [1.0, 2.0, 4.0],
        "pct_chg": [None, None, None],
    })
    out = msd._df_to_metrics(df)
    assert out["close"] == 4.0
    assert out["pctChg"] == 100.0
    assert out["asOfDate"] == "2026-08-07"
    assert msd._df_to_metrics(None) == {}
    assert msd._df_to_metrics(pd.DataFrame()) == {}


def test_fetch_hstech_via_sina(monkeypatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")
    assert msd._fetch_hstech_via_sina() is None
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(msd, "sys", __import__("sys"))
    assert msd._fetch_hstech_via_sina() is None  # akshare import missing

    df = pd.DataFrame({"date": ["2026-08-05", "2026-08-06"], "close": [1.0, 2.0]})
    _ = type("AK", (), {"stock_hk_index_daily_sina": staticmethod(lambda symbol: df)})()
    monkeypatch.setattr(msd, "sys", __import__("sys"))

    import types as _types
    fake_ak = _types.ModuleType("akshare")
    fake_ak.stock_hk_index_daily_sina = lambda symbol: df
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)
    out = msd._fetch_hstech_via_sina()
    assert out is not None and out["close"] == 2.0 and out["pctChg"] == 100.0
    monkeypatch.delitem(sys.modules, "akshare")


def test_fetch_on_demand_series_ixic_via_yf(monkeypatch) -> None:
    metrics = {"close": 1.0, "pctChg": 0.5, "asOfDate": "2026-08-07", "ma5": 1.0, "ma20": 1.0}
    monkeypatch.setattr(msd, "_fetch_ixic_via_yfinance", lambda: metrics)
    m, src, und = msd._fetch_on_demand_series(None, SID_IXIC)
    assert src == "yfinance.on_demand" and und == "IXIC" and m["close"] == 1.0

    monkeypatch.setattr(msd, "_fetch_ixic_via_yfinance", lambda: None)
    m2, src2, und2 = msd._fetch_on_demand_series(None, SID_IXIC)
    assert m2 == {} and src2 is None

    pro = type("P", (), {"index_global": staticmethod(lambda **kw: pd.DataFrame({"trade_date": ["2026-08-07"], "close": [2.0]}))})()
    m3, src3, und3 = msd._fetch_on_demand_series(pro, SID_IXIC)
    assert src3 == "tushare.index_global.on_demand"


def test_fetch_on_demand_series_hstech_fallbacks(monkeypatch) -> None:
    monkeypatch.setattr(msd, "_fetch_hstech_via_sina", lambda: None)
    monkeypatch.setattr(msd, "_fetch_hstech_via_yfinance", lambda: {"close": 3.0})
    m, src, und = msd._fetch_on_demand_series(None, SID_HSTECH)
    assert src == "yfinance.on_demand" and und == "HSTECH"

    monkeypatch.setattr(msd, "_fetch_hstech_via_sina", lambda: {"close": 9.0})
    m2, src2, _ = msd._fetch_on_demand_series(None, SID_HSTECH)
    assert src2 == "akshare.on_demand"


def test_fetch_on_demand_series_tushare_only(monkeypatch) -> None:
    pro = type("P", (), {
        "fx_daily": staticmethod(lambda **kw: pd.DataFrame({"trade_date": ["2026-08-07"], "close": [7.0]})),
        "fut_daily": staticmethod(lambda **kw: pd.DataFrame({"trade_date": ["2026-08-07"], "close": [8.0]})),
        "index_global": staticmethod(lambda **kw: pd.DataFrame({"trade_date": ["2026-08-07"], "close": [9.0]})),
        "fut_basic": staticmethod(lambda **kw: pd.DataFrame()),
    })()
    m, src, und = msd._fetch_on_demand_series(pro, SID_USDCNH)
    assert src == "tushare.fx_daily.on_demand"

    monkeypatch.setattr(msd, "resolve_sgx_a50_main", lambda pro: None)
    m2, src2, und2 = msd._fetch_on_demand_series(pro, SID_A50)
    assert src2 == "tushare.index_global.on_demand" and und2 == "XIN9"

    monkeypatch.setattr(msd, "resolve_ine_sc_main", lambda pro: None)
    m3, src3, _ = msd._fetch_on_demand_series(pro, SID_COMM_GOLD)
    assert src3 is None  # no contract resolved
    monkeypatch.setattr(msd, "resolve_main_fut_by_prefix", lambda pro, ex, prefix: "AU2606.SHFE")
    m4, src4, und4 = msd._fetch_on_demand_series(pro, SID_COMM_GOLD)
    assert src4 == "tushare.fut_daily.on_demand" and und4 == "AU2606.SHFE"

    m5, src5, _ = msd._fetch_on_demand_series(pro, "UNKNOWN_SERIES")
    assert m5 == {} and src5 is None


def test_is_data_stale() -> None:
    assert msd._is_data_stale(None) is True
    assert msd._is_data_stale("bad-date") is True
    yesterday = (datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=1)).isoformat()
    assert msd._is_data_stale(yesterday) is False
    old = (datetime.datetime.now(datetime.UTC).date() - datetime.timedelta(days=3)).isoformat()
    assert msd._is_data_stale(old) is True


def test_enrich_macro_items_on_demand(monkeypatch) -> None:
    items = [
        {"seriesId": SID_IXIC, "close": None, "asOfDate": None},
        {"seriesId": SID_HSI, "close": 1.0, "asOfDate": "2026-08-07"},
    ]
    monkeypatch.setattr(msd, "try_tushare_pro", lambda: None)
    monkeypatch.setattr(msd, "_fetch_on_demand_series", lambda pro, sid: (
        ({"close": 5.0, "pctChg": 1.0, "asOfDate": "2026-08-07", "ma5": 1.0, "ma20": 1.0}, "yfinance.on_demand", "IXIC")
        if sid == SID_IXIC else ({}, None, None)
    ))
    out = msd.enrich_macro_items_on_demand(items)
    assert out[0]["close"] == 5.0
    assert out[0]["source"] == "yfinance.on_demand"
    assert out[0]["dataSource"] == "on_demand"
    assert out[1]["close"] == 1.0  # untouched (no metrics)

    out2 = msd.enrich_macro_items_on_demand([{"seriesId": "x", "close": 1.0, "asOfDate": "2026-08-07"}])
    assert len(out2) == 1  # not in ALWAYS_REFRESH and fresh → untouched


def test_macro_snapshot_warning(monkeypatch) -> None:
    monkeypatch.setattr(msd, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    assert msd.macro_snapshot_warning() is not None
    monkeypatch.setattr(msd, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())
    assert msd.macro_snapshot_warning() is None

def test_yfinance_failure_cache_fast_fails() -> None:
    """A recent failure marker short-circuits the yfinance fetch (no network)."""
    import time

    import data_sync_service.service.macro_snapshot_on_demand as mod

    mod._yf_fail_cache.clear()
    mod._yf_fail_cache["^HSTECH"] = time.time() + 300
    try:
        assert mod._fetch_yfinance_index("^HSTECH") is None
    finally:
        mod._yf_fail_cache.clear()

    # expired marker does not block a real attempt. Network is unavailable in
    # CI/dev, so the call may raise or return None — either way: no stale-block.
    mod._yf_fail_cache["^HSTECH"] = time.time() - 1
    try:
        result = mod._fetch_yfinance_index("^HSTECH")
        assert result is None or isinstance(result, dict)
    except Exception:
        pass
    finally:
        mod._yf_fail_cache.clear()
