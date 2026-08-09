"""Tests for news_enrich service — Tier 0 pre-filter, relevance scoring,
prompt structure, and per-item failure handling."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import news_enrich

# ---------------------------------------------------------------------------
# Tier 0: pre-filter
# ---------------------------------------------------------------------------


class TestPreFilter:
    def test_tier_a_source_always_passes(self):
        for sid in ("cls-telegraph", "wallstreetcn-global", "jin10-flash", "cls-depth", "csrc-news"):
            item = {"title": "Random unrelated title", "summary": "", "source_id": sid}
            assert news_enrich._passes_pre_filter(item) is True, sid

    def test_noise_title_dropped(self):
        item = {
            "title": "本周回顾: 股市震荡",
            "summary": "summary text",
            "source_id": "unknown-source",
        }
        assert news_enrich._passes_pre_filter(item) is False

    def test_noise_patterns_dropped(self):
        cases = [
            "2026年6月A股月报",
            "Year-to-date returns across sectors",
            "上半年回顾与展望",
            "明星八卦影响市场情绪",
            "比特币突破10万美元",
        ]
        for title in cases:
            item = {"title": title, "summary": "", "source_id": "other"}
            assert news_enrich._passes_pre_filter(item) is False, title

    def test_market_relevant_title_passes(self):
        item = {
            "title": "美联储宣布维持利率不变",
            "summary": "",
            "source_id": "reuters-cn",
        }
        assert news_enrich._passes_pre_filter(item) is True

    def test_tech_relevant_title_passes(self):
        item = {
            "title": "TSMC announces new semiconductor fab",
            "summary": "",
            "source_id": "nikkei",
        }
        assert news_enrich._passes_pre_filter(item) is True

    def test_empty_title_fails(self):
        item = {"title": "", "summary": "noise", "source_id": "other"}
        assert news_enrich._passes_pre_filter(item) is False

    def test_irrelevant_title_without_keywords_fails(self):
        item = {
            "title": "City opens new park in downtown",
            "summary": "",
            "source_id": "city-news",
        }
        assert news_enrich._passes_pre_filter(item) is False

    def test_chinese_ticker_match_passes(self):
        item = {
            "title": "贵州茅台业绩超预期",
            "summary": "",
            "source_id": "other",
        }
        assert news_enrich._passes_pre_filter(item) is True


# ---------------------------------------------------------------------------
# Tier 1: relevance_score (computed in Python, not by LLM)
# ---------------------------------------------------------------------------


class TestRelevanceScore:
    def setup_method(self):
        news_enrich._WATCHLIST_CACHE = None

    def teardown_method(self):
        news_enrich._WATCHLIST_CACHE = None

    def test_no_watchlist_uses_importance_only(self):
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=[]):
            assert news_enrich._compute_relevance(0, []) == 0
            assert news_enrich._compute_relevance(3, []) == 45  # 3 × 15
            assert news_enrich._compute_relevance(5, []) == 75  # 5 × 15

    def test_watchlist_match_adds_30_per_ticker(self):
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["CN:600519"]):
            score = news_enrich._compute_relevance(3, ["600519"])
            assert score == 45 + 30  # 75

    def test_watchlist_match_strips_market_prefix(self):
        """Watchlist stores 'CN:600519', LLM returns '600519'. Both should match."""
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["HK:00700"]):
            score = news_enrich._compute_relevance(2, ["00700"])
            assert score == 30 + 30  # base 30 + boost 30

    def test_watchlist_boost_capped(self):
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["CN:1", "CN:2", "CN:3", "CN:4"]):
            # 4 matches × 30 = 120, capped at 60
            score = news_enrich._compute_relevance(3, ["1", "2", "3", "4"])
            assert score == min(45 + 60, 100)

    def test_relevance_capped_at_100(self):
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["CN:600519", "HK:00700"]):
            # importance=5 (75) + 2 × 30 boost (60) = 135, cap at 100
            score = news_enrich._compute_relevance(5, ["600519", "00700"])
            assert score == 100

    def test_non_watchlist_ticker_no_boost(self):
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["CN:600519"]):
            # 999999 not in watchlist → no boost
            score = news_enrich._compute_relevance(2, ["999999"])
            assert score == 30  # 2 × 15


# ---------------------------------------------------------------------------
# Tier 1: prompt structure
# ---------------------------------------------------------------------------


class TestPromptStructure:
    def test_prompt_does_not_mention_relevance_formula(self):
        """Relevance is computed in Python now; the prompt should not
        instruct the LLM to apply +30/+50 bonuses."""
        items = [{"id": "x1", "title": "Some news", "summary": "Sum", "source_id": "cls-telegraph"}]
        prompt = news_enrich._build_prompt(items)
        assert "+30" not in prompt
        assert "+50" not in prompt
        assert "watchlist" not in prompt.lower()

    def test_prompt_includes_required_keys(self):
        items = [{"id": "x1", "title": "Fed holds rates", "summary": "Detail"}]
        prompt = news_enrich._build_prompt(items)
        for key in ("id", "tickers", "sectors", "eventType", "importance", "aiSummary", "actionability"):
            assert key in prompt, key

    def test_prompt_truncates_summary_at_200(self):
        long_summary = "x" * 1000
        items = [{"id": "x1", "title": "t", "summary": long_summary}]
        prompt = news_enrich._build_prompt(items)
        # 200 chars after "summary: " plus the label itself
        assert "x" * 250 not in prompt

    def test_prompt_forbids_thinking_and_fences(self):
        """Reasoning models (MiniMax-M3) wrap answers in <think>/```json;
        the prompt must demand pure JSON so parsing stays reliable."""
        items = [{"id": "x1", "title": "Fed holds rates"}]
        prompt = news_enrich._build_prompt(items)
        assert "no thinking" in prompt
        assert "Output ONLY the JSON array" in prompt
        assert "code fences" in prompt

    def test_prompt_handles_missing_summary(self):
        items = [{"id": "x1", "title": "Just a title"}]
        prompt = news_enrich._build_prompt(items)
        assert "Just a title" in prompt


# ---------------------------------------------------------------------------
# Tier 1: response parsing — markdown fence tolerance
# ---------------------------------------------------------------------------


class TestResponseParsing:
    def test_clean_array(self):
        raw = '[{"id": "a", "importance": 3}]'
        out = news_enrich._parse_llm_response(raw, ["a"])
        assert len(out) == 1
        assert out[0]["id"] == "a"

    def test_markdown_fence_stripped_at_ai_service(self):
        """ai-service now strips ```json fences before returning; we only
        see the inner JSON. But be defensive in case of regression."""
        raw = '```json\n[{"id": "a", "importance": 3}]\n```'
        out = news_enrich._parse_llm_response(raw, ["a"])
        assert len(out) == 1
        assert out[0]["id"] == "a"

    def test_dict_with_items_wrapper(self):
        raw = '{"items": [{"id": "a", "importance": 3}]}'
        out = news_enrich._parse_llm_response(raw, ["a"])
        assert len(out) == 1
        assert out[0]["id"] == "a"

    def test_pads_short_response(self):
        """LLM returns 1 entry for 3 items — pad the rest with id-only."""
        raw = '[{"id": "a", "importance": 3}]'
        out = news_enrich._parse_llm_response(raw, ["a", "b", "c"])
        assert len(out) == 3
        assert out[0]["id"] == "a"
        assert out[1] == {"id": "b"}
        assert out[2] == {"id": "c"}

    def test_truncates_long_response(self):
        raw = '[{"id": "a"}, {"id": "b"}, {"id": "c"}]'
        out = news_enrich._parse_llm_response(raw, ["a"])
        assert len(out) == 1

    def test_empty_response_returns_id_padded_list(self):
        out = news_enrich._parse_llm_response("", ["a", "b"])
        assert out == [{"id": "a"}, {"id": "b"}]

    def test_invalid_json_returns_id_padded_list(self):
        out = news_enrich._parse_llm_response("not json at all", ["a", "b"])
        assert out == [{"id": "a"}, {"id": "b"}]

    def test_json_embedded_in_prose(self):
        raw = 'Here is the JSON:\n[{"id": "a", "importance": 3}]\nDone.'
        out = news_enrich._parse_llm_response(raw, ["a"])
        assert len(out) == 1
        assert out[0]["id"] == "a"


# ---------------------------------------------------------------------------
# Tier 2: per-item failure handling
# ---------------------------------------------------------------------------


class TestValidateEntry:
    def test_importance_clamped(self):
        out = news_enrich._validate_entry({"id": "a", "importance": 99})
        assert out["importance"] == 0  # falls back to 0

    def test_importance_zero_clears_ai_summary(self):
        """Tier 2 early-exit: importance=0 items don't get ai_summary
        (saves brief scoring time)."""
        out = news_enrich._validate_entry(
            {"id": "a", "importance": 0, "aiSummary": "Some summary"}
        )
        assert out["importance"] == 0
        assert out["ai_summary"] == ""

    def test_importance_nonzero_keeps_ai_summary(self):
        out = news_enrich._validate_entry(
            {"id": "a", "importance": 3, "aiSummary": "Fed holds rates"}
        )
        assert out["ai_summary"] == "Fed holds rates"

    def test_invalid_event_type_falls_back(self):
        out = news_enrich._validate_entry({"id": "a", "eventType": "garbage"})
        assert out["event_type"] == "other"

    def test_invalid_actionability_falls_back(self):
        out = news_enrich._validate_entry({"id": "a", "actionability": "garbage"})
        assert out["actionability"] == "informational"

    def test_tickers_normalized_to_list_of_strings(self):
        out = news_enrich._validate_entry({"id": "a", "tickers": ["600519", 123, None, ""]})
        assert out["tickers"] == ["600519", "123"]

    def test_relevance_computed_from_importance_and_tickers(self):
        """relevance should be Python-computed, not from the LLM."""
        with patch.object(news_enrich, "_get_watchlist_symbols", return_value=["CN:600519"]):
            out = news_enrich._validate_entry(
                {"id": "a", "importance": 3, "tickers": ["600519"], "relevanceScore": 999}
            )
            # LLM said 999 but we ignore it and compute 45 + 30 = 75
            assert out["relevance_score"] == 75


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_default_model_is_cheap():
    """Default model should be deepseek-v4-flash, not gpt-4o-mini."""
    # Re-import to get current module-level default (env var override may apply)
    import importlib

    importlib.reload(news_enrich)
    assert news_enrich.ENRICHMENT_MODEL == "deepseek-v4-flash"


def test_max_retries_is_one():
    import importlib

    importlib.reload(news_enrich)
    assert news_enrich.MAX_RETRIES == 1


def test_tier_a_sources_have_high_signal():
    """Sanity check: Tier-A sources must be a small, curated set."""
    assert len(news_enrich.TIER_A_SOURCE_IDS) <= 6
    for sid in news_enrich.TIER_A_SOURCE_IDS:
        assert sid  # non-empty
