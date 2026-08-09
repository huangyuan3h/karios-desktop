"""service/alpha_radar_symbol_resolve.py coverage."""

from __future__ import annotations

from data_sync_service.service import alpha_radar_symbol_resolve as sr


class TestNormalize:
    def test_normalize_ticker(self) -> None:
        assert sr._normalize_ticker("CN:600519") == "600519"
        assert sr._normalize_ticker("cn:600519") == "600519"
        assert sr._normalize_ticker("600519") == "600519"
        assert sr._normalize_ticker(" 600519. ") == "600519"
        assert sr._normalize_ticker("") is None
        assert sr._normalize_ticker(None) is None
        assert sr._normalize_ticker("abc") is None
        assert sr._normalize_ticker("12345") is None

    def test_normalize_hk_ticker(self) -> None:
        assert sr._normalize_hk_ticker("00700") == "00700"
        assert sr._normalize_hk_ticker("700") == "00700"
        assert sr._normalize_hk_ticker("HK:700") == "00700"
        assert sr._normalize_hk_ticker("HK00700") == "00700"
        assert sr._normalize_hk_ticker("hk:123456") is None
        assert sr._normalize_hk_ticker("") is None
        assert sr._normalize_hk_ticker(None) is None
        assert sr._normalize_hk_ticker("abc") is None
        assert sr._normalize_hk_ticker("1234567") is None


class TestLookupCn:
    def test_by_ticker(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "600519", "symbol": "600519", "name": "贵州茅台"}]))
        out = sr._lookup_by_ticker("600519")
        assert out["symbol"] == "CN:600519"
        assert out["confidence"] == 0.85
        assert out["rationale"] == "Ticker match"
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "600519", "symbol": "CN:600519", "name": "贵州茅台"}]))
        assert sr._lookup_by_ticker("600519")["symbol"] == "CN:600519"
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "000001", "symbol": "000001", "name": "平安银行"}]))
        assert sr._lookup_by_ticker("600519") is None
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "", "symbol": "", "name": ""}]))
        assert sr._lookup_by_ticker("600519") is None

    def test_by_name(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "search_cn_candidates", lambda names, limit: [])
        assert sr._lookup_by_name("贵州茅台") is None
        monkeypatch.setattr(sr, "search_cn_candidates", lambda names, limit: [{"symbol": "CN:600519", "name": "贵州茅台"}])
        out = sr._lookup_by_name("贵州茅台")
        assert out["confidence"] == 0.75 and out["ambiguous"] is False
        monkeypatch.setattr(sr, "search_cn_candidates", lambda names, limit: [{"symbol": "600519", "ticker": "600519", "name": "贵州茅台"}])
        out = sr._lookup_by_name("贵州茅台")
        assert out["symbol"] == "CN:600519"

    def test_by_name_ambiguous(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "search_cn_candidates", lambda names, limit: [{"symbol": "CN:600519", "name": "贵州茅台"}])
        monkeypatch.setattr("data_sync_service.service.alpha_radar_qa.name_search_is_ambiguous", lambda **kw: True)
        out = sr._lookup_by_name("贵州茅台")
        assert out["confidence"] == 0.55
        assert out["rationale"].endswith("(ambiguous)")
        monkeypatch.setattr("data_sync_service.service.alpha_radar_qa.name_search_is_ambiguous", lambda **kw: (_ for _ in ()).throw(RuntimeError("x")))
        out = sr._lookup_by_name("贵州茅台")
        assert out["confidence"] == 0.75


class TestLookupHk:
    def test_by_ticker(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "00700", "symbol": "00700", "name": "腾讯"}]))
        out = sr._lookup_hk_by_ticker("00700")
        assert out["symbol"] == "HK:00700"
        assert out["confidence"] == 0.85
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "00700", "symbol": "HK:00700", "name": "腾讯"}]))
        assert sr._lookup_hk_by_ticker("00700")["symbol"] == "HK:00700"
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "00941", "symbol": "00941", "name": "中移动"}]))
        assert sr._lookup_hk_by_ticker("00700") is None
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"ticker": "", "symbol": "", "name": ""}]))
        assert sr._lookup_hk_by_ticker("00700") is None

    def test_by_name(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, []))
        assert sr._lookup_hk_by_name("") is None
        assert sr._lookup_hk_by_name("腾讯") is None
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"symbol": "00700", "ticker": "00700", "name": "腾讯"}]))
        out = sr._lookup_hk_by_name("腾讯")
        assert out["symbol"] == "HK:00700"
        assert out["confidence"] == 0.7
        monkeypatch.setattr(sr, "fetch_market_stocks", lambda **kw: (1, [{"symbol": "HK:00700", "ticker": "00700", "name": "腾讯"}]))
        assert sr._lookup_hk_by_name("腾讯")["symbol"] == "HK:00700"


class TestResolve:
    def test_resolve_hk(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "_normalize_hk_ticker", lambda t: "00700" if t == "700" else None)
        monkeypatch.setattr(sr, "_lookup_hk_by_ticker", lambda t: {"symbol": "HK:00700", "name": "腾讯", "confidence": 0.85, "rationale": "HK ticker match"})
        monkeypatch.setattr(sr, "_lookup_hk_by_name", lambda n: None)
        resolved, unresolved = sr.resolve_hk_mapping(["700", "腾讯", " ", "00700", "a", "b", "c"])
        assert len(resolved) == 1 and resolved[0]["symbol"] == "HK:00700"
        assert unresolved == ["腾讯"]

    def test_resolve_hk_by_name(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "_normalize_hk_ticker", lambda t: None)
        monkeypatch.setattr(sr, "_lookup_hk_by_name", lambda n: {"symbol": "HK:00941", "name": "中移动", "confidence": 0.7, "rationale": "name"})
        resolved, unresolved = sr.resolve_hk_mapping(["中移动", "中移动"])
        assert len(resolved) == 1

    def test_map_trend_hk(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "resolve_hk_mapping", lambda raw, logic_summary: ([{"symbol": "HK:00700", "name": "腾讯", "confidence": 0.85, "rationale": "r"}] if raw else [], ["x"]))
        seen = {}
        monkeypatch.setattr(sr, "update_trend_hk_mapping", lambda trend_id, hk_symbols: seen.update(trend_id=trend_id, hk_symbols=hk_symbols))
        out = sr.map_trend_hk(trend_id="t1", trend={"hk_mapping": ["00700"], "logic_summary": "summary"})
        assert out["mappingMode"] == "local_resolve_hk"
        assert seen["trend_id"] == "t1"
        out2 = sr.map_trend_hk(trend_id="t2", trend={"hkMapping": []})
        assert out2["hkSymbols"] == []

    def test_resolve_a_share(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "_normalize_ticker", lambda t: "600519" if t == "600519" else None)
        monkeypatch.setattr(sr, "_lookup_by_ticker", lambda t: {"symbol": "CN:600519", "name": "贵州茅台", "confidence": 0.85, "rationale": "r", "ambiguous": False})
        monkeypatch.setattr(sr, "_lookup_by_name", lambda n: None)
        resolved, unresolved = sr.resolve_a_share_mapping(["600519", "600519", " ", "贵州茅台", "x"])
        assert len(resolved) == 1 and resolved[0]["symbol"] == "CN:600519"
        assert unresolved == []
        monkeypatch.setattr(sr, "_lookup_by_name", lambda n: {"symbol": "CN:600519" if n == "贵州茅台" else "CN:300751", "name": n, "confidence": 0.75, "rationale": "n", "ambiguous": False})
        resolved, unresolved = sr.resolve_a_share_mapping(["贵州茅台", "隆基"])
        assert len(resolved) == 2
        monkeypatch.setattr(sr, "_lookup_by_name", lambda n: None)
        resolved, unresolved = sr.resolve_a_share_mapping(["贵州茅台"])
        assert resolved == [] and unresolved == ["贵州茅台"]

    def test_map_trend_hybrid_local(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "resolve_a_share_mapping", lambda raw, logic_summary: ([{"symbol": "CN:600519", "name": "贵州茅台", "confidence": 0.9, "ambiguous": False}], []))
        monkeypatch.setattr("data_sync_service.db.alpha_radar.update_trend_mapping", lambda **kw: None)
        monkeypatch.setattr("data_sync_service.service.alpha_radar_risk.compute_risk_status", lambda **kw: "low")
        out = sr.map_trend_hybrid(trend_id="t1", trend={"a_share_mapping": ["600519"], "macro_theme": "白酒", "keywords_for_mapping": ["茅台"]}, hot_industry_names=["电子"], mainline_by_industry={})
        assert out["mappingMode"] == "local_resolve"
        assert out["riskStatus"] == "low"
        assert out["mappingConfidence"] == 0.9

    def test_map_trend_hybrid_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(sr, "resolve_a_share_mapping", lambda raw, logic_summary: ([], ["贵州茅台"]))
        seen = {}
        monkeypatch.setattr(sr, "map_trend_to_cn", lambda **kw: seen.update(kw) or {"cnSymbols": [], "mappingConfidence": 0.5, "riskStatus": "medium", "ok": True})
        out = sr.map_trend_hybrid(trend_id="t1", trend={"keywordsForMapping": ["贵州茅台"], "macroTheme": "白酒"})
        assert out["mappingMode"] == "map_cn_fallback"
        assert seen["seed_symbols"] == ["贵州茅台"]
        assert seen["trend"]["keywords_for_mapping"] == ["贵州茅台", "白酒"]
