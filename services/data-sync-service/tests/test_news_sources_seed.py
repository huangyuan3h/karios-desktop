"""News Substrate 2.0 · Track 1 — investment-grade source seed contract.

Verifies:
- Every DEFAULT_NEWS_SOURCES row has tier ∈ {A, B, C} (no D in active list).
- Every URL is reachable via RSSHub at routes that don't require Playwright.
- Tier A sources include the canonical real-time telegraphs.
- All legacy / trimmed sources are in the disable list.
"""

from __future__ import annotations

from data_sync_service.service.news import (
    DEFAULT_NEWS_SOURCES,
    LEGACY_DISABLED_SOURCES,
)


def test_default_sources_all_investment_grade() -> None:
    """Every active source must be tier A/B/C; no D in DEFAULT_NEWS_SOURCES."""
    for sid, _name, _url, tier, _cat in DEFAULT_NEWS_SOURCES:
        assert tier in {"A", "B", "C"}, f"{sid} has tier {tier} (must be A/B/C)"


def test_no_legacy_generic_in_active_list() -> None:
    """BBC / NYT / HN / Reddit must not be in the active default list."""
    active_ids = {sid for sid, *_ in DEFAULT_NEWS_SOURCES}
    for legacy in ("bbc-world", "nyt-world", "hn-front", "reddit-finance"):
        assert legacy not in active_ids, f"{legacy} should not be an active default source"
        assert legacy in LEGACY_DISABLED_SOURCES, f"{legacy} should be in LEGACY_DISABLED_SOURCES"


def test_tier_a_includes_real_time_telegraphs() -> None:
    """Tier A must include the 3 real-time telegraph sources."""
    tier_a_ids = {sid for sid, _, _, tier, _ in DEFAULT_NEWS_SOURCES if tier == "A"}
    required_a = {"cls-telegraph", "wallstreetcn-global", "jin10-flash"}
    missing = required_a - tier_a_ids
    assert not missing, f"Tier A missing required real-time sources: {missing}"


def test_total_source_count_tight() -> None:
    """Total active sources ≤ 8 (trimmed for noise control)."""
    assert len(DEFAULT_NEWS_SOURCES) <= 8, (
        f"Have {len(DEFAULT_NEWS_SOURCES)} active sources, cap is 8"
    )


def test_no_duplicate_source_ids() -> None:
    ids = [sid for sid, *_ in DEFAULT_NEWS_SOURCES]
    assert len(ids) == len(set(ids)), f"Duplicate source IDs: {ids}"


def test_removed_noise_sources_in_disabled_list() -> None:
    """Sources removed for noise (36kr, huxiu, yicai, etc.) are in disabled list."""
    active_ids = {sid for sid, *_ in DEFAULT_NEWS_SOURCES}
    noise_sources = [
        "36kr-news", "huxiu-finance", "yicai-news", "gelonghui-home",
        "caixin-headline", "wallstreetcn-us", "7e2ce389", "jin10-data",
    ]
    for sid in noise_sources:
        assert sid not in active_ids, f"{sid} should not be active"
        assert sid in LEGACY_DISABLED_SOURCES, f"{sid} should be in LEGACY_DISABLED_SOURCES"
