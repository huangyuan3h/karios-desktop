"""Tests for News Substrate 2.0 · Track 3 — Morning Brief selection logic."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from data_sync_service.service.morning_brief import (
    _assign_category,
    _freshness_bonus,
    _is_excluded,
    _score_item,
    _watchlist_boost,
    select_brief_items,
)


def test_freshness_bonus_very_recent() -> None:
    """Items < 2h old should get freshness bonus 100."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    assert _freshness_bonus(recent, recent) == 100


def test_freshness_bonus_6h() -> None:
    """Items 2–6h old should get freshness bonus 70."""
    now = datetime.now(UTC)
    ref = (now - timedelta(hours=4)).isoformat()
    assert _freshness_bonus(ref, ref) == 70


def test_freshness_bonus_12h() -> None:
    """Items 6–12h old should get freshness bonus 40."""
    now = datetime.now(UTC)
    ref = (now - timedelta(hours=8)).isoformat()
    assert _freshness_bonus(ref, ref) == 40


def test_freshness_bonus_old() -> None:
    """Items > 12h old should get freshness bonus 10."""
    now = datetime.now(UTC)
    ref = (now - timedelta(hours=24)).isoformat()
    assert _freshness_bonus(ref, ref) == 10


def test_freshness_bonus_none() -> None:
    """Items with no published_at should use fetched_at."""
    now = datetime.now(UTC)
    recent_fetched = (now - timedelta(hours=1)).isoformat()
    assert _freshness_bonus(None, recent_fetched) == 100

    old_fetched = (now - timedelta(hours=24)).isoformat()
    assert _freshness_bonus(None, old_fetched) == 10


def test_score_item_combines_all_factors() -> None:
    """Score formula: importance × 0.3 + relevance × 0.3 + freshness × 0.2 + boost × 0.2."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    item = {
        "importance": 5,
        "relevanceScore": 80,
        "publishedAt": recent,
        "fetchedAt": recent,
        "tickers": [],
        "sectors": [],
    }
    held_symbols: set[str] = set()
    held_sectors: set[str] = set()
    score = _score_item(item, held_symbols, held_sectors)
    # freshness = 100, boost = 0
    # importance × 0.3 = 1.5, relevance × 0.3 = 24.0, freshness × 0.2 = 20.0, boost × 0.2 = 0
    assert abs(score - 45.5) < 0.1


def test_watchlist_boost_held_ticker() -> None:
    """Items mentioning a held ticker should get boost 50."""
    item = {"tickers": ["600519.SH"], "sectors": ["白酒"]}
    held = {"600519.SH", "600519"}
    assert _watchlist_boost(item, held, set()) == 50


def test_watchlist_boost_bare_ticker() -> None:
    """Items matching bare ticker (without suffix) should get boost 50."""
    item = {"tickers": ["600519.SH"], "sectors": []}
    held = {"600519"}
    assert _watchlist_boost(item, held, set()) == 50


def test_watchlist_boost_sector_match() -> None:
    """Items matching a held sector should get boost 20."""
    item = {"tickers": ["000001.SZ"], "sectors": ["银行"]}
    held_symbols: set[str] = set()
    held_sectors = {"银行"}
    assert _watchlist_boost(item, held_symbols, held_sectors) == 20


def test_watchlist_boost_no_match() -> None:
    """Items with no overlap should get boost 0."""
    item = {"tickers": ["000001.SZ"], "sectors": ["银行"]}
    held_symbols: set[str] = set()
    held_sectors: set[str] = set()
    assert _watchlist_boost(item, held_symbols, held_sectors) == 0


def test_assign_category_watchlist() -> None:
    """Items mentioning held ticker should be category 'watchlist'."""
    item = {"tickers": ["600519.SH"], "sectors": [], "title": "test", "aiSummary": ""}
    assert _assign_category(item, {"600519.SH"}) == "watchlist"


def test_assign_category_risk() -> None:
    """Items with risk keywords should be category 'risk'."""
    item = {"tickers": [], "sectors": [], "title": "美方制裁实体清单", "aiSummary": ""}
    assert _assign_category(item, set()) == "risk"


def test_assign_category_macro() -> None:
    """Items with macro keywords should be category 'macro'."""
    item = {"tickers": [], "sectors": [], "title": "央行降准50个基点", "aiSummary": ""}
    assert _assign_category(item, set()) == "macro"


def test_assign_category_sector() -> None:
    """Items with sector keywords should be category 'sector'."""
    item = {"tickers": [], "sectors": ["新能源"], "title": "新能源板块大涨", "aiSummary": ""}
    assert _assign_category(item, set()) == "sector"


def test_is_excluded_monthly_review() -> None:
    """Items with backward-looking patterns should be excluded."""
    assert _is_excluded({"title": "7月A股月度总结"}) is True
    assert _is_excluded({"title": "上半年回顾与展望"}) is True
    assert _is_excluded({"title": "今日A股大涨"}) is False


def test_select_brief_items_filters_enriched_only() -> None:
    """Only items with enrichment_status='done' should be selected."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    older = (now - timedelta(hours=10)).isoformat()

    mock_items = [
        {
            "id": "item-1",
            "title": "High importance enriched",
            "sourceId": "src-a",
            "link": "http://example.com/1",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 5,
            "relevanceScore": 90,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        },
        {
            "id": "item-2",
            "title": "Not enriched yet",
            "sourceId": "src-a",
            "link": "http://example.com/2",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 5,
            "relevanceScore": 90,
            "enrichmentStatus": None,
            "tickers": [],
            "sectors": [],
        },
        {
            "id": "item-3",
            "title": "Failed enrichment",
            "sourceId": "src-a",
            "link": "http://example.com/3",
            "publishedAt": older,
            "fetchedAt": older,
            "importance": 3,
            "relevanceScore": 50,
            "enrichmentStatus": "failed",
            "tickers": [],
            "sectors": [],
        },
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(3, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=(set(), set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) == 1
    assert result[0]["id"] == "item-1"
    assert "score" in result[0]
    assert "category" in result[0]


def test_select_brief_items_filters_historical() -> None:
    """Items with actionability='historical' should be excluded."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()

    mock_items = [
        {
            "id": "item-1",
            "title": "Actionable news",
            "sourceId": "src-a",
            "link": "http://example.com/1",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 4,
            "relevanceScore": 70,
            "enrichmentStatus": "done",
            "actionability": "actionable",
            "tickers": [],
            "sectors": [],
        },
        {
            "id": "item-2",
            "title": "Historical review",
            "sourceId": "src-a",
            "link": "http://example.com/2",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 3,
            "relevanceScore": 50,
            "enrichmentStatus": "done",
            "actionability": "historical",
            "tickers": [],
            "sectors": [],
        },
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(2, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=(set(), set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) == 1
    assert result[0]["id"] == "item-1"


def test_select_brief_items_filters_excluded_patterns() -> None:
    """Items with excluded title patterns should be removed."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()

    mock_items = [
        {
            "id": "item-1",
            "title": "今日A股市场分析",
            "sourceId": "src-a",
            "link": "http://example.com/1",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 4,
            "relevanceScore": 70,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        },
        {
            "id": "item-2",
            "title": "7月A股月度总结与回顾",
            "sourceId": "src-a",
            "link": "http://example.com/2",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 3,
            "relevanceScore": 50,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        },
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(2, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=(set(), set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) == 1
    assert result[0]["id"] == "item-1"


def test_select_brief_items_returns_max_7() -> None:
    """Brief should contain at most 7 items."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    mock_items = [
        {
            "id": f"item-{i}",
            "title": f"Item {i}",
            "sourceId": "src-a",
            "link": f"http://example.com/{i}",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": i % 6,
            "relevanceScore": (i * 10) % 100,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        }
        for i in range(15)
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(15, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=(set(), set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) <= 7


def test_select_brief_items_sorted_by_score_desc() -> None:
    """Items should be sorted by score descending."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    mock_items = [
        {
            "id": "low",
            "title": "Low",
            "sourceId": "src-a",
            "link": "http://example.com/low",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 1,
            "relevanceScore": 10,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        },
        {
            "id": "high",
            "title": "High",
            "sourceId": "src-a",
            "link": "http://example.com/high",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 5,
            "relevanceScore": 90,
            "enrichmentStatus": "done",
            "tickers": [],
            "sectors": [],
        },
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(2, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=(set(), set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) == 2
    assert result[0]["id"] == "high"
    assert result[1]["id"] == "low"
    assert result[0]["score"] >= result[1]["score"]


def test_select_brief_items_watchlist_boost() -> None:
    """Items mentioning held tickers should score higher."""
    now = datetime.now(UTC)
    recent = (now - timedelta(hours=1)).isoformat()
    mock_items = [
        {
            "id": "held",
            "title": "贵州茅台发布业绩",
            "sourceId": "src-a",
            "link": "http://example.com/held",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 3,
            "relevanceScore": 50,
            "enrichmentStatus": "done",
            "tickers": ["600519.SH"],
            "sectors": ["白酒"],
        },
        {
            "id": "other",
            "title": "其他股票新闻",
            "sourceId": "src-a",
            "link": "http://example.com/other",
            "publishedAt": recent,
            "fetchedAt": recent,
            "importance": 3,
            "relevanceScore": 50,
            "enrichmentStatus": "done",
            "tickers": ["000001.SZ"],
            "sectors": ["银行"],
        },
    ]

    with patch(
        "data_sync_service.service.morning_brief.fetch_items",
        return_value=(2, mock_items),
    ), patch(
        "data_sync_service.service.morning_brief._load_watchlist_context",
        return_value=({"600519.SH"}, set()),
    ):
        result = select_brief_items(hours=24)

    assert len(result) == 2
    assert result[0]["id"] == "held"
    assert result[0]["category"] == "watchlist"
