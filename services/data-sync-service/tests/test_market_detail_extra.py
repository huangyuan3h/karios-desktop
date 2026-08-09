"""market_detail service coverage (chips / fund flow detail endpoints)."""

from __future__ import annotations

import builtins
import sys

import pytest
from fastapi import HTTPException

from data_sync_service.service import market_detail as md


def _patch_parse(monkeypatch, parsed=("CN", "600000", "600000.SH")):
    monkeypatch.setattr(md, "_parse_symbol", lambda s: parsed)
    monkeypatch.setattr(md, "_lookup_name", lambda code: "浦发")
    monkeypatch.setattr(md, "_today_cn_date_str", lambda: "2026-08-08")


def test_now_iso() -> None:
    out = md._now_iso()
    assert out.endswith("+00:00") and "T" in out


def test_today_cn_date_str(monkeypatch) -> None:
    monkeypatch.setattr(md, "ZoneInfo", lambda zone: (_ for _ in ()).throw(Exception("no tz")))
    out = md._today_cn_date_str()
    assert len(out) == 10


def test_parse_symbol_cn(monkeypatch) -> None:
    assert md._parse_symbol("CN:000001") == ("CN", "000001", "000001.SZ")
    assert md._parse_symbol("CN:600000") == ("CN", "600000", "600000.SH")
    assert md._parse_symbol("CN:12345") is None
    assert md._parse_symbol("CN:abcdef") is None


def test_parse_symbol_hk(monkeypatch) -> None:
    assert md._parse_symbol("HK:700") == ("HK", "00700", "00700.HK")
    assert md._parse_symbol("HK:12345") == ("HK", "12345", "12345.HK")
    assert md._parse_symbol("HK:") is None
    assert md._parse_symbol("HK:abc") is None
    assert md._parse_symbol("HK:123456") is None


def test_parse_symbol_etf(monkeypatch) -> None:
    assert md._parse_symbol("ETF:510300") == ("ETF", "510300", "510300.SH")
    assert md._parse_symbol("ETF:159915") == ("ETF", "159915", "159915.SZ")
    assert md._parse_symbol("ETF:600000") == ("ETF", "600000", "600000.SH")
    assert md._parse_symbol("ETF:12345") is None


def test_parse_symbol_ts_code_direct(monkeypatch) -> None:
    assert md._parse_symbol("000001.sz") == ("CN", "000001", "000001.SZ")
    assert md._parse_symbol("00700.HK") == ("HK", "00700", "00700.HK")
    assert md._parse_symbol("") is None
    assert md._parse_symbol(None) is None
    assert md._parse_symbol("garbage") is None


def test_parse_symbol_cn_only_alias() -> None:
    assert md._parse_symbol_cn_only is md._parse_symbol


def test_lookup_name_found(monkeypatch) -> None:
    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def execute(self, sql, params=None):
            return self

        def fetchone(self):
            return ("浦发",)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return Cur()

    from data_sync_service import db as dblib

    monkeypatch.setattr(md, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(dblib, "get_connection", lambda: Conn())
    assert md._lookup_name("600000.SH") == "浦发"


def test_lookup_name_missing(monkeypatch) -> None:
    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def execute(self, sql, params=None):
            return self

        def fetchone(self):
            return (None,)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return Cur()

    from data_sync_service import db as dblib

    monkeypatch.setattr(md, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(dblib, "get_connection", lambda: Conn())
    assert md._lookup_name("600000.SH") is None


def test_lookup_name_error(monkeypatch) -> None:
    monkeypatch.setattr(md, "ensure_stock_basic", lambda: (_ for _ in ()).throw(Exception("db down")))
    assert md._lookup_name("600000.SH") is None


def test_fetch_cn_a_chip_summary_darwin() -> None:
    if sys.platform == "darwin":
        with pytest.raises(RuntimeError, match="akshare_disabled_on_darwin"):
            md.fetch_cn_a_chip_summary("600000")


def test_fetch_cn_a_chip_summary_linux(monkeypatch) -> None:
    monkeypatch.setattr(md.sys, "platform", "linux")
    df = _Df([
        {"日期": "2026-08-01", "获利比例": "0.8", "平均成本": "10.1",
         "90成本-低": "9", "90成本-高": "11", "90集中度": "0.1",
         "70成本-低": "9.5", "70成本-高": "10.5", "70集中度": "0.05"},
        {"日期": "", "获利比例": "0.7"},  # empty date skipped
    ])
    out = _chip_with_df(monkeypatch, df)
    assert out[0]["date"] == "2026-08-01" and out[0]["profitRatio"] == "0.8"
    assert len(out) == 1


def _chip_with_df(monkeypatch, df):
    calls = {"n": 0}

    def stock_cyq_em(symbol=None, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TypeError("old signature")
        return df

    _fake_akshare(monkeypatch, {"stock_cyq_em": stock_cyq_em})
    return md.fetch_cn_a_chip_summary("600000", days=60)


def test_fetch_cn_a_chip_summary_akshare_missing(monkeypatch) -> None:
    monkeypatch.setattr(md.sys, "platform", "linux")
    real = builtins.__import__

    def imp(name, *a, **k):
        if name == "akshare":
            raise ImportError("no akshare")
        return real(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", imp)
    with pytest.raises(RuntimeError, match="AkShare is required"):
        md.fetch_cn_a_chip_summary("600000")


def test_fetch_cn_a_chip_summary_no_func(monkeypatch) -> None:
    monkeypatch.setattr(md.sys, "platform", "linux")
    real = builtins.__import__

    def imp(name, *a, **k):
        if name == "akshare":
            return type("ak", (), {})()
        return real(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", imp)
    with pytest.raises(RuntimeError, match="missing stock_cyq_em"):
        md.fetch_cn_a_chip_summary("600000")


def test_fetch_cn_a_fund_flow_linux(monkeypatch) -> None:
    monkeypatch.setattr(md.sys, "platform", "linux")
    df = _Df([
        {"日期": "2026-08-01", "收盘价": "10.0", "涨跌幅": "1.0",
         "主力净流入-净额": "100", "主力净流入-净占比": "0.1",
         "超大单净流入-净额": "50", "超大单净流入-净占比": "0.05"},
        {"日期": "", "收盘价": "9.0"},
    ])
    calls = {"n": 0}

    def fund_flow(stock=None, market=None, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TypeError("old")
        return df

    _fake_akshare(monkeypatch, {"stock_individual_fund_flow": fund_flow})
    out = md.fetch_cn_a_fund_flow("000001", days=30)
    assert out[0]["close"] == "10.0" and out[0]["mainNetAmount"] == "100"
    assert len(out) == 1


def test_fetch_cn_a_fund_flow_darwin() -> None:
    if sys.platform == "darwin":
        with pytest.raises(RuntimeError, match="akshare_disabled_on_darwin"):
            md.fetch_cn_a_fund_flow("600000")


def test_fetch_cn_a_fund_flow_akshare_missing(monkeypatch) -> None:
    monkeypatch.setattr(md.sys, "platform", "linux")
    real = builtins.__import__

    def imp(name, *a, **k):
        if name == "akshare":
            raise ImportError("no akshare")
        return real(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", imp)
    with pytest.raises(RuntimeError, match="AkShare is required"):
        md.fetch_cn_a_fund_flow("600000")


class _Df:
    def __init__(self, records) -> None:
        self._records = records

    def to_dict(self, orient):
        return self._records


def _fake_akshare(monkeypatch, funcs):
    import types as _types

    mod = _types.ModuleType("akshare")
    for name, fn in funcs.items():
        setattr(mod, name, fn)
    monkeypatch.setitem(md.sys.modules, "akshare", mod)
    return mod


def test_get_market_chips_cached(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    cached = [("2026-08-07", {"date": "2026-08-07", "avgCost": "10"})]
    monkeypatch.setattr(md, "list_chips_cached", lambda sym, limit: cached)
    monkeypatch.setattr(md, "upsert_chips", lambda *a, **k: None)
    out = md.get_market_chips(symbol="CN:600000", days=60)
    assert out["name"] == "浦发" and out["currency"] == "CNY"
    assert out["items"] == [{"date": "2026-08-07", "avgCost": "10"}]


def test_get_market_chips_fetches_and_upserts(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    monkeypatch.setattr(md, "list_chips_cached", lambda sym, limit: [])
    upserted = {}

    def fake_fetch(ticker, days):
        assert ticker == "600000"
        return [{"date": "2026-08-08", "avgCost": "10.5"}]

    monkeypatch.setattr(md, "fetch_cn_a_chip_summary", fake_fetch)
    monkeypatch.setattr(md, "upsert_chips", lambda sym, items, updated_at=None: upserted.update(sym=sym, items=items, ts=updated_at))
    out = md.get_market_chips(symbol="CN:600000", days=60, force=True)
    assert out["items"][0]["avgCost"] == "10.5"
    assert upserted["sym"] == "CN:600000"


def test_get_market_chips_fetch_fail_uses_cache(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    cached = [("2026-08-01", {"date": "2026-08-01"})]
    monkeypatch.setattr(md, "list_chips_cached", lambda sym, limit: cached)

    def boom(ticker, days):
        raise RuntimeError("akshare disabled")

    monkeypatch.setattr(md, "fetch_cn_a_chip_summary", boom)
    out = md.get_market_chips(symbol="CN:600000", days=60, force=True)
    assert out["items"] == [{"date": "2026-08-01"}]


def test_get_market_chips_fetch_fail_empty(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    monkeypatch.setattr(md, "list_chips_cached", lambda sym, limit: [])
    monkeypatch.setattr(md, "fetch_cn_a_chip_summary", lambda t, days: (_ for _ in ()).throw(RuntimeError("x")))
    out = md.get_market_chips(symbol="CN:600000", days=60, force=True)
    assert out["items"] == []


def test_get_market_chips_bad_symbol(monkeypatch) -> None:
    monkeypatch.setattr(md, "_parse_symbol", lambda s: None)
    with pytest.raises(HTTPException) as e:
        md.get_market_chips(symbol="junk", days=60)
    assert e.value.status_code == 400


def test_get_market_chips_non_cn(monkeypatch) -> None:
    _patch_parse(monkeypatch, parsed=("HK", "00700", "00700.HK"))
    with pytest.raises(HTTPException) as e:
        md.get_market_chips(symbol="HK:00700", days=60)
    assert "CN A-shares" in e.value.detail


def test_get_market_fund_flow_cached(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    cached = [("2026-08-08", {"date": "2026-08-08", "close": "10"})]
    monkeypatch.setattr(md, "list_fund_flow_cached", lambda sym, limit: cached)
    out = md.get_market_fund_flow(symbol="CN:600000", days=60)
    assert out["items"][0]["close"] == "10"


def test_get_market_fund_flow_fetches(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    monkeypatch.setattr(md, "list_fund_flow_cached", lambda sym, limit: [])
    upserted = {}
    monkeypatch.setattr(md, "fetch_cn_a_fund_flow", lambda t, days: [{"date": "2026-08-08", "close": "10"}])
    monkeypatch.setattr(md, "upsert_fund_flow", lambda sym, items, updated_at=None: upserted.update(sym=sym))
    out = md.get_market_fund_flow(symbol="CN:600000", days=60, force=True)
    assert out["items"][0]["close"] == "10" and upserted["sym"] == "CN:600000"


def test_get_market_fund_flow_fail_uses_cache_with_warning(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    cached = [("2026-08-01", {"date": "2026-08-01"})]
    monkeypatch.setattr(md, "list_fund_flow_cached", lambda sym, limit: cached)

    def boom(t, days):
        raise ValueError("bad")

    monkeypatch.setattr(md, "fetch_cn_a_fund_flow", boom)
    out = md.get_market_fund_flow(symbol="CN:600000", days=60, force=True)
    assert out["items"] == [{"date": "2026-08-01"}]
    assert "using cached fund flow" in out["warning"]


def test_get_market_fund_flow_fail_empty_with_warning(monkeypatch) -> None:
    _patch_parse(monkeypatch)
    monkeypatch.setattr(md, "list_fund_flow_cached", lambda sym, limit: [])
    monkeypatch.setattr(md, "fetch_cn_a_fund_flow", lambda t, days: (_ for _ in ()).throw(ValueError("bad")))
    out = md.get_market_fund_flow(symbol="CN:600000", days=60, force=True)
    assert out["items"] == [] and "fund flow unavailable" in out["warning"]


def test_get_market_fund_flow_bad_symbol(monkeypatch) -> None:
    monkeypatch.setattr(md, "_parse_symbol", lambda s: None)
    with pytest.raises(HTTPException):
        md.get_market_fund_flow(symbol="junk", days=60)
