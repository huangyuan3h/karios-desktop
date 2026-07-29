import pandas as pd

from data_sync_service.service.realtime_quote import (
    _as_str,
    _fetch_em_hk_quote,
    _get,
    _is_hk,
    _split_hk,
)


def test_as_str_none():
    assert _as_str(None) is None


def test_as_str_nan():
    assert _as_str(pd.NA) is None
    assert _as_str(float("nan")) is None


def test_as_str_valid():
    assert _as_str("hello") == "hello"
    assert _as_str(123) == "123"
    assert _as_str("  test  ") == "test"


def test_as_str_empty():
    assert _as_str("") is None
    assert _as_str("   ") is None


def test_get_first_key():
    obj = {"a": 1, "b": 2}
    assert _get(obj, "a", "b") == 1


def test_get_second_key():
    obj = {"b": 2}
    assert _get(obj, "a", "b") == 2


def test_get_missing():
    obj = {"c": 3}
    assert _get(obj, "a", "b") is None


def test_get_with_none_value():
    obj = {"a": None, "b": 2}
    assert _get(obj, "a", "b") == 2


def test_is_hk():
    assert _is_hk("00700.HK") is True
    assert _is_hk("00700.hk") is True
    assert _is_hk("510300.SH") is False
    assert _is_hk("") is False


def test_split_hk_basic():
    hk, other = _split_hk(["00700.HK", "510300.SH", "00001.SZ"])
    assert hk == ["00700.HK"]
    assert other == ["510300.SH", "00001.SZ"]


def test_split_hk_dedup_preserves_order():
    hk, other = _split_hk(["00700.HK", "510300.SH", "09988.HK", "510300.SH", "00700.HK"])
    assert hk == ["00700.HK", "09988.HK"]
    assert other == ["510300.SH"]


def test_split_hk_only_other():
    hk, other = _split_hk(["510300.SH", "00001.SZ"])
    assert hk == []
    assert other == ["510300.SH", "00001.SZ"]


def test_split_hk_empty():
    hk, other = _split_hk([])
    assert hk == []
    assert other == []


def test_fetch_em_hk_quote_returns_none_for_non_hk():
    assert _fetch_em_hk_quote("510300.SH") is None


def test_fetch_em_hk_quote_returns_none_when_network_fails(monkeypatch):
    """When EM push2 raises (offline / rate-limited), return None silently."""
    from data_sync_service.service import realtime_quote

    def _fake_em(url, *, params, referer, timeout=25.0):  # noqa: ARG001
        raise RuntimeError("offline")

    monkeypatch.setattr(realtime_quote, "em_get_json", _fake_em)
    assert _fetch_em_hk_quote("00700.HK") is None


def test_fetch_em_hk_quote_normalizes_fields(monkeypatch):
    from data_sync_service.service import realtime_quote

    def _fake_em(url, *, params, referer, timeout=25.0):  # noqa: ARG001
        assert params["secid"] == "116.00700"
        return {
            "data": {
                "f43": 380.5,
                "f44": 382.0,
                "f45": 378.0,
                "f46": 379.0,
                "f47": 12345678,
                "f48": 4.7e9,
                "f57": "00700",
                "f58": "腾讯控股",
                "f60": 379.0,
                "f169": 1.5,
                "f170": 0.4,
            }
        }

    monkeypatch.setattr(realtime_quote, "em_get_json", _fake_em)
    quote = _fetch_em_hk_quote("00700.HK")
    assert quote is not None
    assert quote["ts_code"] == "00700.HK"
    assert quote["price"] == "380.5"
    assert quote["high"] == "382.0"
    assert quote["low"] == "378.0"
    assert quote["pre_close"] == "379.0"
    assert quote["pct_chg"] == "0.4"


def test_fetch_em_hk_quote_returns_none_when_missing_price(monkeypatch):
    from data_sync_service.service import realtime_quote

    def _fake_em(url, *, params, referer, timeout=25.0):  # noqa: ARG001
        return {"data": {"f43": "-", "f44": 0}}

    monkeypatch.setattr(realtime_quote, "em_get_json", _fake_em)
    assert _fetch_em_hk_quote("00700.HK") is None


def test_fetch_em_hk_quote_returns_none_when_data_missing(monkeypatch):
    from data_sync_service.service import realtime_quote

    def _fake_em(url, *, params, referer, timeout=25.0):  # noqa: ARG001
        return {"data": None}

    monkeypatch.setattr(realtime_quote, "em_get_json", _fake_em)
    assert _fetch_em_hk_quote("00700.HK") is None