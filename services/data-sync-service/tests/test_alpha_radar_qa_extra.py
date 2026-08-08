"""service/alpha_radar_qa.py coverage."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

from data_sync_service.service import alpha_radar_qa as qa


class TestHelpers:
    def test_to_ticker(self) -> None:
        assert qa._to_ticker("CN:600519") == "600519"
        assert qa._to_ticker("600519") == "600519"
        assert qa._to_ticker(" CN:600519 ") == "600519"
        assert qa._to_ticker("abc") is None
        assert qa._to_ticker("") is None
        assert qa._to_ticker(None) is None
        assert qa._ts_code_from_ticker("600519") == "600519.SH"
        assert qa._ts_code_from_ticker("000001") == "000001.SZ"

    def test_load_theme_map(self, tmp_path) -> None:
        assert qa._load_theme_industry_map(tmp_path / "nope.json") == {}
        bad = tmp_path / "bad.json"
        bad.write_text("{not json")
        assert qa._load_theme_industry_map(bad) == {}
        no_themes = tmp_path / "no.json"
        no_themes.write_text(json.dumps({"other": 1}))
        assert qa._load_theme_industry_map(no_themes) == {}
        not_dict = tmp_path / "nd.json"
        not_dict.write_text(json.dumps({"themes": [1, 2]}))
        assert qa._load_theme_industry_map(not_dict) == {}
        good = tmp_path / "good.json"
        good.write_text(json.dumps({"themes": {"T1": ["A", "B"], "T2": [], "T3": ["C", None]}}))
        out = qa._load_theme_industry_map(good)
        assert out == {"T1": ["A", "B"], "T3": ["C"]}

    def test_industry_match(self) -> None:
        assert qa._industry_match("半导体", ["半导体"]) is True
        assert qa._industry_match("半导体设备", ["半导体"]) is True
        assert qa._industry_match("半导体", ["设备"]) is False
        assert qa._industry_match(None, ["A"]) is False
        assert qa._industry_match("A", []) is False

    def test_bullish_keyword(self) -> None:
        assert qa._is_bullish_keyword("资金流入加速")
        assert qa._is_bullish_keyword("Inflow expected")
        assert not qa._is_bullish_keyword("")
        assert not qa._is_bullish_keyword("资金流出")

    def test_paper_trade_macro_theme(self) -> None:
        assert qa._paper_trade_macro_theme({}) is None
        assert qa._paper_trade_macro_theme({"whyAtEntry": ""}) is None
        why = '{"trendName": "t", "macroTheme": "HBM 涨价"}'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why}) == "HBM 涨价"
        why_bad = '{"macroTheme": "x"'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why_bad}) is None
        why_tn = '{"trendName": "半导体链"}'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why_tn}) == "半导体链"
        why_tn_bad = '{"trendName": "x"'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why_tn_bad}) is None
        why_empty = '{"macroTheme": ""}'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why_empty}) is None
        why_other = '{"other": 1}'
        assert qa._paper_trade_macro_theme({"whyAtEntry": why_other}) is None


class TestWinRates:
    def test_rates(self, monkeypatch) -> None:
        monkeypatch.setattr(qa, "ensure_paper_tables", lambda: None)
        trades = [
            {"whyAtEntry": '{"macroTheme": "T1"}', "pnlPct": 5.0},
            {"whyAtEntry": '{"macroTheme": "T1"}', "pnlPct": -2.0},
            {"whyAtEntry": '{"macroTheme": "T1"}', "pnlPct": "3.0"},
            {"whyAtEntry": '{"macroTheme": "T1"}', "pnlPct": None},
            {"whyAtEntry": '{"macroTheme": "T2"}', "pnlPct": 1.0},
            {"whyAtEntry": "no theme", "pnlPct": 5.0},
        ]
        seen = {}
        monkeypatch.setattr(qa, "list_paper_trades", lambda **kw: seen.update(kw) or trades)
        rates = qa.fetch_theme_win_rates(since_days=30, min_trades=2)
        assert rates["T1"]["wins"] == 1 and rates["T1"]["total"] == 4
        assert rates["T1"]["winRate"] == 0.25
        assert "T2" not in rates
        assert seen["status"] == "closed" and seen["limit"] == 500


class TestSectorFlow:
    def test_top_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(qa, "get_latest_industry_date", lambda: "")
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.resolve_effective_as_of", lambda d: None)
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.trade_dates_upto", lambda *a, **k: [])
        assert qa._recent_sector_flow_top() == {}
        assert qa._recent_sector_flow_out() == {}

    def test_top_dates_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(qa, "get_latest_industry_date", lambda: "2026-08-07")
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.resolve_effective_as_of", lambda d: None)
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.trade_dates_upto", lambda *a, **k: [])
        assert qa._recent_sector_flow_top() == {}
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.resolve_effective_as_of", lambda d: "2026-08-07")
        assert qa._recent_sector_flow_out() == {}
        assert qa._recent_sector_flow_top() == {}

    def test_top_and_out(self, monkeypatch) -> None:
        monkeypatch.setattr(qa, "get_latest_industry_date", lambda: "2026-08-07")
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.resolve_effective_as_of", lambda d: "2026-08-07")
        monkeypatch.setattr("data_sync_service.service.trade_calendar_utils.trade_dates_upto", lambda *a, **k: ["2026-08-07"])
        sums = [
            {"industry_name": "电子", "sum_inflow": 100.0},
            {"industry_name": "银行", "sum_inflow": -50.0},
            {"industry_name": "非SWL1", "sum_inflow": 999.0},
        ]
        monkeypatch.setattr(qa, "get_sum_by_industry_for_dates", lambda dates: sums)
        top = qa._recent_sector_flow_top()
        out = qa._recent_sector_flow_out()
        assert top["电子"] == 100.0 and "非SWL1" not in top
        assert "银行" in out


class TestPenalty:
    def test_no_ticker(self, monkeypatch) -> None:
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        r = qa.compute_auto_qa_penalty(symbol="", macro_theme="T", confidence=1.0)
        assert r["penalty"] == 0.0 and r["industry"] is None

    def test_mismatch(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {"T1": ["半导体"]}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        r = qa.compute_auto_qa_penalty(symbol="CN:600519", macro_theme="T1", confidence=0.9, config=cfg)
        assert r["penalty"] == 0.6
        assert "industry_mismatch" in r["signals"]
        assert r["signals"]["industry_mismatch"]["expected"] == ["半导体"]

    def test_match(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {"T1": ["白酒"]}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        r = qa.compute_auto_qa_penalty(symbol="CN:600519", macro_theme="T1", confidence=0.9, config=cfg)
        assert r["penalty"] == 0.0 and r["industry"] == "白酒"

    def test_ambiguous(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {})
        r = qa.compute_auto_qa_penalty(symbol="CN:600519", macro_theme=None, confidence=0.5, name_ambiguous=True, config=cfg)
        assert r["penalty"] == 0.4
        assert "name_ambiguous" in r["signals"]

    def test_unknown_theme(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒"})
        r = qa.compute_auto_qa_penalty(symbol="CN:600519", macro_theme="NOPE", confidence=0.5, config=cfg)
        assert r["penalty"] == 0.0


class TestCatalyst:
    def test_batch(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {"T1": ["半导体"]}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒", "300750.SZ": "半导体"})
        items = [
            {"symbol": "CN:600519", "macroTheme": "T1", "nameAmbiguous": True},
            {"symbol": "CN:300750", "macroTheme": "T1", "nameAmbiguous": False},
            {"symbol": "CN:300750", "macroTheme": None, "articles": [{"macroTheme": "T1"}], "nameAmbiguous": False},
            {"symbol": "bad", "macroTheme": "T1", "nameAmbiguous": False},
        ]
        out = qa.compute_auto_qa_penalty_for_catalyst(items, config=cfg)
        assert out["CN:600519"]["penalty"] == 0.6
        assert out["CN:300750"]["penalty"] == 0.0
        assert out["bad"]["industry"] is None
        assert len(out) == 3


class TestStats:
    def test_stats(self, monkeypatch, tmp_path) -> None:
        seed = tmp_path / "map.json"
        seed.write_text(json.dumps({"themes": {"T1": ["半导体"]}}))
        cfg = qa.AutoQaConfig(seed_path=str(seed), lookback_days=30, min_win_rate=0.3)
        monkeypatch.setattr(qa, "fetch_trends", lambda **kw: (
            [],
            [
                {
                    "id": "tr-1",
                    "trendName": "半导体链",
                    "macroTheme": "T1",
                    "cnSymbols": [
                        {"symbol": "CN:600519", "name": "贵州茅台"},
                        {"symbol": "CN:300750", "name": "宁德时代"},
                        "not-a-dict",
                    ],
                },
                {"id": "tr-2", "trendName": "", "macroTheme": "", "cnSymbols": []},
            ],
        ))
        monkeypatch.setattr(qa, "lookup_by_ts_codes", lambda codes: {"600519.SH": "白酒", "300750.SZ": "半导体"})
        monkeypatch.setattr(qa, "fetch_theme_win_rates", lambda **kw: {"T1": {"wins": 1, "total": 5, "winRate": 0.2}})
        out = qa.get_auto_qa_stats(since_days=7, config=cfg)
        assert out["sinceDays"] == 7
        assert out["themesCovered"] == 1
        assert out["lowWinRateThemes"][0]["theme"] == "T1"
        assert out["recentPenalties"][0]["symbol"] == "CN:600519"
        assert out["recentPenalties"][0]["penalty"] == 0.6
        assert out["config"]["minWinRate"] == 0.3


class TestNameAmbiguity:
    def test_chars(self) -> None:
        assert not qa._names_share_significant_chars("", "b")
        assert not qa._names_share_significant_chars("a", "")
        assert not qa._names_share_significant_chars("same", "same")
        assert qa._names_share_significant_chars("东方财富", "东方财富网")
        assert qa._names_share_significant_chars("中远海控", "远海控")
        assert not qa._names_share_significant_chars("贵州茅台", "宁德时代")

    def test_search_ambiguous(self) -> None:
        assert not qa.name_search_is_ambiguous(candidates=[])
        assert not qa.name_search_is_ambiguous(candidates=[{"name": "a"}, {"name": "b"}], min_candidates=3)
        assert not qa.name_search_is_ambiguous(candidates=[{"name": "a"}, "not-dict", {"name": "b"}])
        cands = [{"name": "x", "score": 0.9}, {"name": "y", "score": 0.85}, {"name": "z", "score": 0.1}]
        assert qa.name_search_is_ambiguous(candidates=cands, gap_threshold=0.1)
        assert not qa.name_search_is_ambiguous(candidates=cands, gap_threshold=0.02)
        no_score = [{"name": "东方财富"}, {"name": "东方财富网"}, {"name": "别的"}]
        assert qa.name_search_is_ambiguous(candidates=no_score)
        no_score2 = [{"name": "贵州茅台"}, {"name": "宁德时代"}, {"name": "隆基绿能"}]
        assert not qa.name_search_is_ambiguous(candidates=no_score2)
        empty_name = [{"name": ""}, {"name": "a"}, {"name": "b"}]
        assert not qa.name_search_is_ambiguous(candidates=empty_name)
