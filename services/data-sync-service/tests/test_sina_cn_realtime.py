"""Sina CN realtime fallback (2026-11-06): Tushare-down path must still quote."""

from __future__ import annotations

import pytest

from data_sync_service.service import realtime_quote as rq
from data_sync_service.service import sina_http


def test_build_cn_hq_url_skips_bad_codes() -> None:
    url = sina_http.build_cn_hq_url(["600000.SH", "000001.SZ", "510300.SH", "bad", "HSI"])
    assert "sh600000" in url and "sz000001" in url and "sh510300" in url
    assert "bad" not in url and "HSI" not in url


def test_parse_cn_hq_lines() -> None:
    text = (
        'var hq_str_sh600000="浦发银行,9.910,9.870,10.030,10.070,9.840,'
        "10.030,10.040,64270729,640470012.000,22000,10.030,75500,10.020,"
        "44300,10.010,98000,10.000,50100,9.990,57800,10.040,150900,10.050,"
        '252307,10.060,358272,10.070,547400,10.080,2026-11-06,15:34:59,00,x";\n'
        'var hq_str_hk00700="TENCENT,...";'
    )
    out = sina_http.parse_cn_hq_lines(text)
    assert out and out[0][0] == "sh600000"
    assert all(marker.startswith(("sh", "sz", "bj")) for marker, _ in out)


def test_parse_sina_cn_payload_matches_tushare_fields() -> None:
    payload = (
        "浦发银行,9.910,9.870,10.030,10.070,9.840,10.030,10.040,64270729,"
        "640470012.000,22000,10.030,75500,10.020,44300,10.010,98000,10.000,"
        "50100,9.990,57800,10.040,150900,10.050,252307,10.060,358272,10.070,"
        "547400,10.080,2026-11-06,15:34:59,00,x"
    )
    quote = rq._parse_sina_cn_payload("sh600000", payload)
    assert quote is not None
    assert quote["ts_code"] == "600000.SH"
    assert quote["price"] == "10.03"
    assert quote["pre_close"] == "9.87"
    assert quote["volume"] == "64270729.0"
    assert quote["amount"] == "640470012.0"
    assert quote["trade_time"] == "2026-11-06 15:34:59"
    assert quote["source"] == "sina.realtime_quote"


def test_fetch_realtime_falls_back_to_sina_cn_for_missing(monkeypatch) -> None:
    rq.clear_sina_cn_quote_cache()

    def fake_tushare(codes, *, api_key):
        return []  # simulate the token-file/quota failure

    def fake_sina(url, *, timeout=10.0):
        return (
            'var hq_str_sh600000="浦发银行,9.910,9.870,10.030,10.070,9.840,'
            "10.030,10.040,64270729,640470012.000,22000,10.030,75500,10.020,"
            "44300,10.010,98000,10.000,50100,9.990,57800,10.040,150900,10.050,"
            '252307,10.060,358272,10.070,547400,10.080,2026-11-06,15:34:59,00,x";\n'
        )

    monkeypatch.setattr(rq, "_tushare_quotes", fake_tushare)
    monkeypatch.setattr(rq, "sina_get_text", fake_sina)
    monkeypatch.setattr(rq, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())

    resp = rq.fetch_realtime_quotes(["600000.SH"])
    assert resp["ok"] is True
    assert resp["items"][0]["ts_code"] == "600000.SH"
    assert resp["items"][0]["source"] == "sina.realtime_quote"
    rq.clear_sina_cn_quote_cache()


def test_fetch_realtime_skips_sina_cn_for_indices(monkeypatch) -> None:
    calls: list[str] = []
    rq.clear_sina_cn_quote_cache()
    monkeypatch.setattr(rq, "_tushare_quotes", lambda codes, *, api_key: [])
    monkeypatch.setattr(rq, "sina_get_text", lambda url, *, timeout=10.0: calls.append(url) or "")
    monkeypatch.setattr(rq, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())
    out = rq.fetch_realtime_quotes(["000300.SH"])
    assert out["items"] == []
    # Index codes are excluded from the Sina fallback request entirely.
    assert calls == []


@pytest.mark.parametrize("code", ["000300.SH", "399001.SZ", "000001.SH"])
def test_is_sina_cn_index(code) -> None:
    assert rq._is_sina_cn_index(code) is True


@pytest.mark.parametrize("code", ["600000.SH", "000001.SZ", "510300.SH", "159819.SZ"])
def test_is_sina_cn_index_false(code) -> None:
    assert rq._is_sina_cn_index(code) is False
