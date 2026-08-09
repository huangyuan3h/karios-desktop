"""Tests for TIP-009 alpha mapping auto-QA + data-driven theme→industry map.

These tests stay offline — they mock the DB layer to avoid requiring a live
Postgres. The integration surface (build_theme_industry_map / get_auto_qa_stats)
is exercised by the alpha_radar_pipeline tests on a real DB.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from data_sync_service.service.alpha_radar_qa import (
    AutoQaConfig,
    _names_share_significant_chars,
    compute_auto_qa_penalty,
    compute_auto_qa_penalty_for_catalyst,
    fetch_theme_win_rates,
    get_auto_qa_stats,
    name_search_is_ambiguous,
)

# ---------------------------------------------------------------------------
# name_search_is_ambiguous
# ---------------------------------------------------------------------------


def test_name_search_is_ambiguous_too_few_candidates():
    assert not name_search_is_ambiguous(candidates=[])
    assert not name_search_is_ambiguous(candidates=[{"name": "A"}, {"name": "B"}])


def test_name_search_is_ambiguous_numeric_score_gap():
    candidates = [
        {"name": "北方华创", "score": 0.95},
        {"name": "北方国际", "score": 0.90},
        {"name": "北方稀土", "score": 0.30},
    ]
    # gap 0.05 < 0.15 → ambiguous
    assert name_search_is_ambiguous(candidates=candidates)
    candidates[1]["score"] = 0.70
    # gap 0.25 > 0.15 → not ambiguous
    assert not name_search_is_ambiguous(candidates=candidates)


def test_name_search_is_ambiguous_name_prefix_fallback():
    # No numeric scores, but multiple candidates share the "北方" prefix
    candidates = [
        {"name": "北方华创"},
        {"name": "北方国际"},
        {"name": "北方稀土"},
    ]
    assert name_search_is_ambiguous(candidates=candidates)


def test_name_search_is_ambiguous_unique_top1():
    candidates = [
        {"name": "贵州茅台"},
        {"name": "五粮液"},
        {"name": "山西汾酒"},
    ]
    assert not name_search_is_ambiguous(candidates=candidates)


def test_names_share_significant_chars_substring():
    assert _names_share_significant_chars("北方华创", "北方国际")
    assert not _names_share_significant_chars("贵州茅台", "五粮液")
    assert not _names_share_significant_chars("北方华创", "南方华创")


# ---------------------------------------------------------------------------
# compute_auto_qa_penalty_for_catalyst (industry mismatch)
# ---------------------------------------------------------------------------


def test_compute_auto_qa_penalty_industry_mismatch(tmp_path):
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(
        json.dumps(
            {
                "themes": {"HBM 涨价": ["半导体", "电子"]},
                "unmapped_themes": ["某某概念"],
                "stats": {},
            },
            ensure_ascii=False,
        )
    )
    items = [
        {
            "symbol": "CN:600036",
            "name": "招商银行",
            "catalystScore": 95.0,
            "articles": [{"macroTheme": "HBM 涨价"}],
        }
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={"600036.SH": "银行"},
    ):
        result = compute_auto_qa_penalty_for_catalyst(
            items, config=AutoQaConfig(seed_path=str(seed_path))
        )
    assert "CN:600036" in result
    info = result["CN:600036"]
    assert info["penalty"] == pytest.approx(0.6, abs=1e-6)
    assert "industry_mismatch" in info["signals"]
    assert info["industry"] == "银行"


def test_compute_auto_qa_penalty_industry_match(tmp_path):
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(
        json.dumps({"themes": {"HBM 涨价": ["半导体", "电子"]}}, ensure_ascii=False)
    )
    items = [
        {
            "symbol": "CN:002371",
            "name": "北方华创",
            "catalystScore": 95.0,
            "articles": [{"macroTheme": "HBM 涨价"}],
        }
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={"002371.SH": "半导体"},
    ):
        result = compute_auto_qa_penalty_for_catalyst(
            items, config=AutoQaConfig(seed_path=str(seed_path))
        )
    assert result["CN:002371"]["penalty"] == 0.0


def test_compute_auto_qa_penalty_name_ambiguity(tmp_path):
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(json.dumps({"themes": {}, "unmapped_themes": []}))
    items = [
        {
            "symbol": "CN:600519",
            "name": "贵州茅台",
            "catalystScore": 92.0,
            "articles": [{"macroTheme": "HBM 涨价"}],
            "nameAmbiguous": True,
        }
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={},
    ):
        result = compute_auto_qa_penalty_for_catalyst(
            items, config=AutoQaConfig(seed_path=str(seed_path))
        )
    # No industry mismatch (industry unknown), but name_ambiguous → 0.4
    assert result["CN:600519"]["penalty"] == pytest.approx(0.4, abs=1e-6)
    assert "name_ambiguous" in result["CN:600519"]["signals"]


def test_compute_auto_qa_penalty_unmapped_theme_no_penalty(tmp_path):
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(json.dumps({"themes": {}, "unmapped_themes": ["新主题"]}))
    items = [
        {
            "symbol": "CN:000001",
            "name": "平安银行",
            "catalystScore": 90.0,
            "articles": [{"macroTheme": "新主题"}],
        }
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={"000001.SZ": "银行"},
    ):
        result = compute_auto_qa_penalty_for_catalyst(
            items, config=AutoQaConfig(seed_path=str(seed_path))
        )
    # Theme not in seed map → no industry mismatch signal
    assert result["CN:000001"]["penalty"] == 0.0


def test_compute_auto_qa_penalty_per_symbol_helper(tmp_path):
    """compute_auto_qa_penalty (single-symbol) wires the same signal."""
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(json.dumps({"themes": {"HBM 涨价": ["半导体"]}}))
    with patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={"600036.SH": "银行"},
    ):
        result = compute_auto_qa_penalty(
            symbol="CN:600036",
            macro_theme="HBM 涨价",
            confidence=0.85,
            config=AutoQaConfig(seed_path=str(seed_path)),
        )
    assert result["penalty"] == pytest.approx(0.6, abs=1e-6)


# ---------------------------------------------------------------------------
# fetch_theme_win_rates
# ---------------------------------------------------------------------------


def test_fetch_theme_win_rates_filters_by_min_trades():
    trades = [
        # theme "A" — 4 trades, 1 win → 25% (< 30% floor after min trades ≥ 3)
        {"status": "closed", "closeDate": "2026-07-01", "pnlPct": 1.0, "whyAtEntry": '{"macroTheme": "A"}'},
        {"status": "closed", "closeDate": "2026-07-02", "pnlPct": -2.0, "whyAtEntry": '{"macroTheme": "A"}'},
        {"status": "closed", "closeDate": "2026-07-03", "pnlPct": -3.0, "whyAtEntry": '{"macroTheme": "A"}'},
        {"status": "closed", "closeDate": "2026-07-04", "pnlPct": -1.5, "whyAtEntry": '{"macroTheme": "A"}'},
        # theme "B" — 3 trades, 2 wins → 66% (not low)
        {"status": "closed", "closeDate": "2026-07-01", "pnlPct": 5.0, "whyAtEntry": '{"macroTheme": "B"}'},
        {"status": "closed", "closeDate": "2026-07-02", "pnlPct": 2.0, "whyAtEntry": '{"macroTheme": "B"}'},
        {"status": "closed", "closeDate": "2026-07-03", "pnlPct": -1.0, "whyAtEntry": '{"macroTheme": "B"}'},
        # theme "C" — 1 trade, 1 win → excluded (insufficient)
        {"status": "closed", "closeDate": "2026-07-01", "pnlPct": 10.0, "whyAtEntry": '{"macroTheme": "C"}'},
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.list_paper_trades",
        return_value=trades,
    ), patch(
        "data_sync_service.service.alpha_radar_qa.ensure_paper_tables",
        return_value=None,
    ):
        rates = fetch_theme_win_rates(since_days=60, min_trades=3)
    assert "A" in rates
    assert rates["A"]["total"] == 4
    assert rates["A"]["winRate"] == pytest.approx(0.25, abs=1e-6)
    assert "B" in rates
    assert rates["B"]["winRate"] == pytest.approx(0.667, abs=1e-6)
    assert "C" not in rates  # insufficient trades


# ---------------------------------------------------------------------------
# get_auto_qa_stats (offline patch path)
# ---------------------------------------------------------------------------


def test_get_auto_qa_stats_combines_penalties_and_low_win(tmp_path):
    seed_path = tmp_path / "theme_industry_map.json"
    seed_path.write_text(
        json.dumps({"themes": {"HBM 涨价": ["半导体"]}}, ensure_ascii=False)
    )
    trends = [
        {
            "id": "t1",
            "trendName": "Memory surge",
            "macroTheme": "HBM 涨价",
            "cnSymbols": [{"symbol": "CN:600036", "name": "招商银行"}],
        }
    ]
    with patch(
        "data_sync_service.service.alpha_radar_qa.fetch_trends",
        return_value=(1, trends),
    ), patch(
        "data_sync_service.service.alpha_radar_qa.lookup_by_ts_codes",
        return_value={"600036.SH": "银行"},
    ), patch(
        "data_sync_service.service.alpha_radar_qa.fetch_theme_win_rates",
        return_value={"HBM 涨价": {"wins": 1, "total": 5, "winRate": 0.20}},
    ):
        stats = get_auto_qa_stats(since_days=7, limit=10, config=AutoQaConfig(seed_path=str(seed_path)))

    assert stats["themesCovered"] == 1
    assert len(stats["recentPenalties"]) == 1
    assert stats["recentPenalties"][0]["symbol"] == "CN:600036"
    assert stats["recentPenalties"][0]["expectedIndustries"] == ["半导体"]
    assert any(t["theme"] == "HBM 涨价" for t in stats["lowWinRateThemes"])