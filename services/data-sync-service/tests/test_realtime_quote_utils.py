import pandas as pd

from data_sync_service.service.realtime_quote import (
    _as_str,
    _fetch_em_hk_quote,
    _fetch_sina_hk_quote,
    _get,
    _is_hk,
    _parse_sina_hk_payload,
    _sina_hk_quotes_fresh,
    _split_hk,
    clear_sina_hk_quote_cache,
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


# ---------------------------------------------------------------------------
# Sina HK (primary source for HK realtime quotes)
# ---------------------------------------------------------------------------


def test_parse_sina_hk_payload_normalizes_fields():
    payload = (
        "TENCENT,腾讯控股,470.000,471.800,479.800,462.000,475.200,3.400,"
        "0.721,475.00000,475.20001,14692435323,31100240,0.000,0.000,"
        "675.134,411.000,2026/07/31,16:08"
    )
    quote = _parse_sina_hk_payload("00700", payload)
    assert quote is not None
    assert quote["ts_code"] == "00700.HK"
    assert quote["price"] == "475.2"
    assert quote["open"] == "470.0"
    assert quote["pre_close"] == "471.8"
    assert quote["high"] == "479.8"
    assert quote["low"] == "462.0"
    assert quote["change"] == "3.4"
    assert quote["pct_chg"] == "0.721"
    assert quote["volume"] == "31100240.0"
    assert quote["amount"] == "14692435323.0"
    assert quote["trade_time"] == "2026-07-31 16:08:00"


def test_parse_sina_hk_payload_preserves_negative_change():
    payload = (
        "PING AN,中国平安,58.450,58.700,58.700,57.800,58.650,-0.050,"
        "-0.085,58.60000,58.65000,1539095975,26319543,0.000,0.000,"
        "72.204,49.728,2026/07/31,16:08"
    )
    quote = _parse_sina_hk_payload("02318", payload)
    assert quote is not None
    assert quote["change"] == "-0.05"
    assert quote["pct_chg"] == "-0.085"


def test_parse_sina_hk_payload_returns_none_when_price_missing():
    payload = ",".join(["x"] * 18 + [""])
    assert _parse_sina_hk_payload("00700", payload) is None


def test_parse_sina_hk_payload_returns_none_when_too_short():
    assert _parse_sina_hk_payload("00700", "1,2,3") is None


def test_sina_hk_quotes_fresh_uses_http_and_caches(monkeypatch):
    clear_sina_hk_quote_cache()

    body = (
        "var hq_str_hk00700=\"TENCENT,腾讯控股,470.000,471.800,479.800,462.000,"
        "475.200,3.400,0.721,475.00000,475.20001,14692435323,31100240,0.000,"
        "0.000,675.134,411.000,2026/07/31,16:08\";\n"
        "var hq_str_hk02318=\"PING AN,中国平安,58.450,58.700,58.700,57.800,"
        "58.650,-0.050,-0.085,58.60000,58.65000,1539095975,26319543,0.000,"
        "0.000,72.204,49.728,2026/07/31,16:08\";\n"
    )
    calls: list[str] = []

    def _fake(url, *, timeout=10.0):  # noqa: ARG001
        calls.append(url)
        return body

    from data_sync_service.service import realtime_quote as rq

    monkeypatch.setattr(rq, "sina_get_text", _fake)

    quotes = _sina_hk_quotes_fresh(["00700", "02318"])
    assert len(calls) == 1
    assert "hk00700" in calls[0]
    assert "hk02318" in calls[0]
    assert sorted(quotes) == ["00700", "02318"]
    assert quotes["00700"]["price"] == "475.2"
    assert quotes["02318"]["change"] == "-0.05"

    # Second call within TTL must not re-hit Sina.
    again = _sina_hk_quotes_fresh(["00700"])
    assert len(calls) == 1
    assert again["00700"]["price"] == "475.2"


def test_sina_hk_quotes_fresh_returns_empty_when_network_fails(monkeypatch):
    clear_sina_hk_quote_cache()

    def _fake(url, *, timeout=10.0):  # noqa: ARG001
        raise RuntimeError("network down")

    from data_sync_service.service import realtime_quote as rq

    monkeypatch.setattr(rq, "sina_get_text", _fake)
    quotes = _sina_hk_quotes_fresh(["00700"])
    assert quotes == {}


def test_fetch_sina_hk_quote_returns_none_for_non_hk():
    clear_sina_hk_quote_cache()
    assert _fetch_sina_hk_quote("510300.SH") is None


def test_fetch_sina_hk_quote_returns_none_when_missing(monkeypatch):
    clear_sina_hk_quote_cache()

    def _fake(url, *, timeout=10.0):  # noqa: ARG001
        return 'var hq_str_hk00700=",,,,,,,,,,,,,,,,,,,2026/07/31,16:08";'

    from data_sync_service.service import realtime_quote as rq

    monkeypatch.setattr(rq, "sina_get_text", _fake)
    assert _fetch_sina_hk_quote("99999.HK") is None


def test_fetch_realtime_quotes_prefers_sina_for_hk(monkeypatch):
    """fetch_realtime_quotes must surface Sina quotes (with trade_time) for HK."""
    clear_sina_hk_quote_cache()

    body = (
        "var hq_str_hk00700=\"TENCENT,腾讯控股,470.000,471.800,479.800,462.000,"
        "475.200,3.400,0.721,475.00000,475.20001,14692435323,31100240,0.000,"
        "0.000,675.134,411.000,2026/07/31,16:08\";\n"
    )

    def _fake_sina(url, *, timeout=10.0):  # noqa: ARG001
        return body

    from data_sync_service.service import realtime_quote as rq

    monkeypatch.setattr(rq, "sina_get_text", _fake_sina)
    monkeypatch.setattr(
        rq,
        "_tushare_quotes",
        lambda codes, *, api_key: (_ for _ in ()).throw(
            AssertionError("tushare must not be called when Sina returns the HK quote")
        ),
    )
    monkeypatch.setattr(rq, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())

    resp = rq.fetch_realtime_quotes(["00700.HK"])
    assert resp["ok"] is True
    items = resp["items"]
    assert len(items) == 1
    assert items[0]["ts_code"] == "00700.HK"
    assert items[0]["price"] == "475.2"
    # Sina provides a timestamp; EM push2 does not. This is the contract change.
    assert items[0]["trade_time"] == "2026-07-31 16:08:00"


def test_fetch_realtime_quotes_falls_back_to_em_when_sina_misses(monkeypatch):
    """HK ticker absent from Sina response must fall back to EM push2."""
    clear_sina_hk_quote_cache()

    def _fake_sina(url, *, timeout=10.0):  # noqa: ARG001
        # Return an empty body (Sina dropped this ticker or it isn't listed).
        return ""

    def _fake_em(url, *, params, referer, timeout=25.0):  # noqa: ARG001
        assert params["secid"] == "116.09988"
        return {
            "data": {
                "f43": 117.0,
                "f44": 119.0,
                "f45": 113.0,
                "f46": 114.0,
                "f60": 112.0,
                "f169": 5.0,
                "f170": 4.46,
            }
        }

    from data_sync_service.service import realtime_quote as rq

    monkeypatch.setattr(rq, "sina_get_text", _fake_sina)
    monkeypatch.setattr(rq, "em_get_json", _fake_em)
    monkeypatch.setattr(rq, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())

    resp = rq.fetch_realtime_quotes(["09988.HK"])
    assert resp["ok"] is True
    items = resp["items"]
    assert len(items) == 1
    assert items[0]["ts_code"] == "09988.HK"
    # EM fallback keeps trade_time=None (Sina-style stamp would not be available).
    assert items[0]["trade_time"] is None