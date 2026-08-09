"""alpha_radar_mapping service coverage."""

from __future__ import annotations

import json
import urllib.error
import urllib.request

from data_sync_service.service import alpha_radar_mapping as am


def _patch(monkeypatch, candidates=None, industry_rows=None):
    c = candidates if candidates is not None else []
    i = industry_rows if industry_rows is not None else []
    calls = {"market": 0, "industry": 0}

    def fake_fetch_market_stocks(**kw):
        calls["market"] += 1
        return 0, c

    monkeypatch.setattr(am, "fetch_market_stocks", fake_fetch_market_stocks)
    monkeypatch.setattr(am, "search_stocks_by_industry_keyword", lambda q, limit: calls.__setitem__("industry", calls["industry"] + 1) or i)
    monkeypatch.setattr(am, "get_settings", lambda: type("S", (), {"ai_service_base_url": ""})())
    return calls


def test_ai_service_base_url_env(monkeypatch) -> None:
    monkeypatch.setattr(am.os, "getenv", lambda k, d=None: "http://svc:9999/" if k == "AI_SERVICE_BASE_URL" else d)
    assert am._ai_service_base_url() == "http://svc:9999"
    monkeypatch.setattr(am.os, "getenv", lambda k, d=None: d)
    assert am._ai_service_base_url() == "http://127.0.0.1:4310"


def test_tavily_api_key(monkeypatch) -> None:
    monkeypatch.setattr(am.os, "getenv", lambda k, d=None: "  abc  " if k == "TAVILY_API_KEY" else d)
    assert am.tavily_api_key() == "abc"


def test_normalize_keyword() -> None:
    assert am._normalize_keyword(" 芯片 A股 ") == "芯片"
    assert am._normalize_keyword(None) == ""
    assert am._normalize_keyword("芯片A股") == "芯片"


def test_search_cn_candidates_basic(monkeypatch) -> None:
    _patch(monkeypatch, candidates=[
        {"symbol": "600000.SH", "ticker": "600000", "name": "浦发"},
        {"symbol": "600000.SH", "ticker": "600000", "name": "浦发"},  # dup → skipped
        {"symbol": "", "ticker": "x"},  # empty symbol → skipped
    ])
    out = am.search_cn_candidates(["   ", "a"])  # blank + short keywords skipped
    assert out == []


def test_search_cn_candidates_dedupe_and_industry(monkeypatch) -> None:
    calls = _patch(monkeypatch, candidates=[
        {"symbol": "600000.SH", "ticker": "600000", "name": "浦发"},
        {"symbol": "000001.SZ", "ticker": "1", "name": "平安"},
    ], industry_rows=[
        {"symbol": "000001.SZ", "source": "emIndustry"},  # dup
        {"symbol": "300001.SZ", "source": "emIndustry"},
    ])
    out = am.search_cn_candidates(["银行", "芯片"])
    assert calls["market"] == 2
    assert [c["symbol"] for c in out] == ["600000.SH", "000001.SZ", "300001.SZ"]
    assert out[0]["source"] == "nameSearch" and out[2]["source"] == "emIndustry"


def test_search_cn_candidates_ticker_search(monkeypatch) -> None:
    names = [{"symbol": "000001.SZ", "ticker": "000001", "name": "平安"}]
    tickers = [{"symbol": "600519.SH", "ticker": "600519", "name": "贵州茅台"}]
    calls = {"n": 0}

    def fake_fetch_market_stocks(**kw):
        calls["n"] += 1
        return 0, tickers if calls["n"] > 1 else names

    monkeypatch.setattr(am, "fetch_market_stocks", fake_fetch_market_stocks)
    monkeypatch.setattr(am, "search_stocks_by_industry_keyword", lambda q, limit: [])
    out = am.search_cn_candidates(["Moutai 白酒"], limit=12)
    assert calls["n"] == 2  # name search + ticker search
    assert [c["symbol"] for c in out] == ["000001.SZ", "600519.SH"]
    assert out[1]["source"] == "tickerSearch"


def test_search_cn_candidates_cap_24(monkeypatch) -> None:
    many = [{"symbol": f"600000.SH{i}", "ticker": str(i), "name": f"n{i}"} for i in range(30)]
    _patch(monkeypatch, candidates=many)
    assert len(am.search_cn_candidates(["kk"] * 5)) == 24


def test_tavily_search_no_key(monkeypatch) -> None:
    monkeypatch.setattr(am, "tavily_api_key", lambda: "")
    assert am.tavily_search_cn_context(["芯片"]) is None


def test_tavily_search_success(monkeypatch) -> None:
    monkeypatch.setattr(am, "tavily_api_key", lambda: "KEY")
    body = json.dumps({"results": [{"title": "T1", "content": "c" * 100}]}).encode()
    monkeypatch.setattr(am.urllib.request, "urlopen", lambda req, timeout=30: _Resp(body))
    out = am.tavily_search_cn_context(["芯片"])
    assert out == f"- T1: {'c' * 100}"
    assert out.count("c") == 100


def test_tavily_search_no_results(monkeypatch) -> None:
    monkeypatch.setattr(am, "tavily_api_key", lambda: "KEY")
    monkeypatch.setattr(am.urllib.request, "urlopen", lambda req, timeout=30: _Resp(b"{}"))
    assert am.tavily_search_cn_context(["芯片"]) is None
    monkeypatch.setattr(am.urllib.request, "urlopen", lambda req, timeout=30: _Resp(b'{"results":[{"title":"","content":""}]}'))
    assert am.tavily_search_cn_context(["芯片"]) is None


def test_tavily_search_error(monkeypatch, capsys) -> None:
    monkeypatch.setattr(am, "tavily_api_key", lambda: "KEY")

    def boom(req, timeout=30):
        raise OSError("net down")

    monkeypatch.setattr(am.urllib.request, "urlopen", boom)
    assert am.tavily_search_cn_context(["芯片"]) is None
    assert "Tavily search failed" in capsys.readouterr().out


class _Resp:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def read(self):
        return self._body


def test_ai_map_cn_symbols_success(monkeypatch) -> None:
    sent = {}

    def fake_urlopen(req, timeout=120):
        sent["url"] = req.full_url
        payload = json.loads(req.data.decode())
        sent["payload"] = payload
        return _Resp(json.dumps({"cnSymbols": ["600000.SH"]}).encode())

    monkeypatch.setattr(am.urllib.request, "urlopen", fake_urlopen)
    out = am._ai_map_cn_symbols(trend={"trend_name": "t"}, candidates=[{"symbol": "a"}], external_context="ctx")
    assert out == {"cnSymbols": ["600000.SH"]}
    assert sent["url"].endswith("/alpha-radar/map-cn")
    assert sent["payload"]["allowKnowledgeFallback"] is False
    assert sent["payload"]["seedSymbols"] == []


def test_ai_map_cn_symbols_fallback_flag(monkeypatch) -> None:
    monkeypatch.setattr(am.urllib.request, "urlopen", lambda req, timeout=120: _Resp(b"{}"))
    out = am._ai_map_cn_symbols(trend={}, candidates=[], external_context=None, seed_symbols=["s1"])
    assert out == {}


def test_ai_map_cn_symbols_http_error(monkeypatch) -> None:
    class Err(urllib.error.HTTPError):
        def __init__(self) -> None:
            super().__init__("http://x", 500, "err", {}, None)
            self._data = b"bad json"

        def read(self):
            return self._data

    monkeypatch.setattr(am.urllib.request, "urlopen", lambda req, timeout=120: (_ for _ in ()).throw(Err()))
    try:
        am._ai_map_cn_symbols(trend={}, candidates=[], external_context=None)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "map-cn error: bad json" in str(exc)


def test_map_trend_to_cn_from_row(monkeypatch) -> None:
    row = {
        "trendJson": {"keywordsForMapping": ["芯片"], "aShareMapping": ["600000.SH"]},
        "trendName": "芯片牛市",
        "macroTheme": "半导体",
        "catalystGrade": "A",
    }
    calls = {"mapping": None, "risk": None}
    monkeypatch.setattr(am, "fetch_trend_by_id", lambda tid: row)
    monkeypatch.setattr(am, "search_cn_candidates", lambda kws: [{"symbol": "600000.SH"}])
    monkeypatch.setattr(am, "tavily_search_cn_context", lambda kws: "ctx")
    monkeypatch.setattr(am, "_ai_map_cn_symbols",
                        lambda **kw: {"cnSymbols": ["600000.SH"], "mappingConfidence": 0.9})
    monkeypatch.setattr(am, "compute_risk_status", lambda **kw: {"level": "low"})
    monkeypatch.setattr(am, "update_trend_mapping", lambda **kw: calls.__setitem__("mapping", kw))

    out = am.map_trend_to_cn(trend_id="t1")
    assert out["cnSymbols"] == ["600000.SH"] and out["mappingConfidence"] == 0.9
    assert out["riskStatus"] == {"level": "low"}
    assert calls["mapping"]["trend_id"] == "t1"
    assert calls["mapping"]["mapping_confidence"] == 0.9
    assert calls["mapping"]["cn_symbols"] == ["600000.SH"]


def test_map_trend_to_cn_snake_case_payload(monkeypatch) -> None:
    monkeypatch.setattr(am, "fetch_trend_by_id", lambda tid: {"trendJson": {"keywords_for_mapping": ["k"]}})
    monkeypatch.setattr(am, "search_cn_candidates", lambda kws: [])
    monkeypatch.setattr(am, "tavily_search_cn_context", lambda kws: None)
    monkeypatch.setattr(am, "_ai_map_cn_symbols", lambda **kw: {"cn_symbols": ["x"], "mapping_confidence": 0.5})
    monkeypatch.setattr(am, "compute_risk_status", lambda **kw: {})
    monkeypatch.setattr(am, "update_trend_mapping", lambda **kw: None)
    out = am.map_trend_to_cn(trend_id="t1")
    assert out["cnSymbols"] == ["x"] and out["mappingConfidence"] == 0.5


def test_map_trend_to_cn_trend_not_found(monkeypatch) -> None:
    monkeypatch.setattr(am, "fetch_trend_by_id", lambda tid: None)
    try:
        am.map_trend_to_cn(trend_id="nope")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_map_trend_to_cn_uses_passed_trend(monkeypatch) -> None:
    seen = {}
    monkeypatch.setattr(am, "search_cn_candidates", lambda kws: seen.__setitem__("kws", kws) or [])
    monkeypatch.setattr(am, "tavily_search_cn_context", lambda kws: None)
    monkeypatch.setattr(am, "_ai_map_cn_symbols", lambda **kw: {})
    monkeypatch.setattr(am, "compute_risk_status", lambda **kw: {})
    monkeypatch.setattr(am, "update_trend_mapping", lambda **kw: None)
    am.map_trend_to_cn(trend_id="t1", trend={"keywords_for_mapping": ["k1", "k2"], "a_share_mapping": ["s1", "s2"]})
    assert "k1" in seen["kws"] and "s1" in seen["kws"]


def test_remap_trend_by_id(monkeypatch) -> None:
    from data_sync_service.service import alpha_radar_process as proc
    from data_sync_service.service import alpha_radar_symbol_resolve as sr

    row = {"trendJson": {"keywordsForMapping": ["芯片"]}, "macroTheme": "T", "catalystGrade": "A"}
    monkeypatch.setattr(proc, "_load_risk_context", lambda: (["芯片"], {"半导体": 90.0}))
    monkeypatch.setattr(am, "fetch_trend_by_id", lambda tid: row)
    calls = {}

    def fake_hybrid(**kw):
        calls.update(kw)
        return {"cnSymbols": ["600000.SH"]}

    monkeypatch.setattr(sr, "map_trend_hybrid", fake_hybrid)
    out = am.remap_trend_by_id("t1")
    assert out == {"cnSymbols": ["600000.SH"]}
    assert calls["trend_id"] == "t1"
    assert calls["hot_industry_names"] == ["芯片"]
    assert calls["mainline_by_industry"] == {"半导体": 90.0}


def test_remap_trend_by_id_not_found(monkeypatch) -> None:
    from data_sync_service.service import alpha_radar_process as proc

    monkeypatch.setattr(proc, "_load_risk_context", lambda: ([], {}))
    monkeypatch.setattr(am, "fetch_trend_by_id", lambda tid: None)
    try:
        am.remap_trend_by_id("nope")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
