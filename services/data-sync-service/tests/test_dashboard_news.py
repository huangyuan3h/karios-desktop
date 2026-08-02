"""Tests for dashboard news selection — sort enriched by relevance, skip noise."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import dashboard


def _mk(
    id_: str,
    *,
    importance: int | None = None,
    relevance: int | None = None,
    actionability: str | None = None,
    tickers: list[str] | None = None,
    title: str = "t",
) -> dict:
    return {
        "id": id_,
        "sourceId": "x",
        "title": title,
        "link": "",
        "publishedAt": "2026-08-02T10:00:00+00:00",
        "fetchedAt": "2026-08-02T10:00:00+00:00",
        "importance": importance,
        "relevanceScore": relevance,
        "actionability": actionability,
        "tickers": tickers or [],
    }


def test_drops_importance_zero_noise():
    items = [
        _mk("noise", importance=0, relevance=0),
        _mk("ok", importance=2, relevance=30),
    ]
    with patch.object(dashboard, "fetch_items", return_value=(2, items)):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    ids = [it["id"] for it in result["items"]]
    assert "noise" not in ids
    assert "ok" in ids


def test_enriched_come_before_unenriched():
    items = [
        _mk("unenriched-old", importance=None),
        _mk("enriched-low", importance=1, relevance=15),
    ]
    with patch.object(dashboard, "fetch_items", return_value=(2, items)):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    ids = [it["id"] for it in result["items"]]
    assert ids == ["enriched-low", "unenriched-old"]


def test_enriched_sorted_by_relevance_then_importance():
    items = [
        _mk("a", importance=3, relevance=45),
        _mk("b", importance=3, relevance=75),  # highest
        _mk("c", importance=4, relevance=45),  # same rel as a but higher importance
    ]
    with patch.object(dashboard, "fetch_items", return_value=(3, items)):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    ids = [it["id"] for it in result["items"]]
    assert ids == ["b", "c", "a"]


def test_returns_score_fields_to_frontend():
    items = [_mk("hit", importance=4, relevance=90, actionability="actionable", tickers=["600519"])]
    with patch.object(dashboard, "fetch_items", return_value=(1, items)):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    item = result["items"][0]
    assert item["importance"] == 4
    assert item["relevanceScore"] == 90
    assert item["actionability"] == "actionable"
    assert item["tickers"] == ["600519"]


def test_respects_limit_after_dedup():
    items = (
        [_mk(f"e{i}", importance=3, relevance=50 - i) for i in range(10)]  # 10 enriched
        + [_mk("n", importance=0, relevance=0)]  # 1 noise → dropped
        + [_mk(f"u{i}", importance=None) for i in range(5)]  # 5 unenriched
    )
    with patch.object(dashboard, "fetch_items", return_value=(16, items)):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    assert len(result["items"]) == 10
    # All 10 should be enriched (highest relevance first), none unenriched
    assert all(it["id"].startswith("e") for it in result["items"])


def test_empty_db_returns_empty():
    with patch.object(dashboard, "fetch_items", return_value=(0, [])):
        with patch.object(dashboard, "ensure_news_tables", return_value=None):
            result = dashboard._news_items(hours=24, limit=10)
    assert result["items"] == []
    assert result["total"] == 0
