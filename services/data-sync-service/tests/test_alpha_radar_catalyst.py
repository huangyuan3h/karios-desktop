"""Tests for Alpha Radar catalyst stock aggregation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from data_sync_service.service.alpha_radar_catalyst import (
    aggregate_catalyst_stocks,
    article_contribution,
    compute_stock_catalyst_score,
    event_at_for_trend,
)


def _now() -> datetime:
    return datetime(2026, 5, 28, 12, 0, 0, tzinfo=UTC)


def _trend(
    *,
    trend_id: str,
    document_id: str,
    symbol: str = "603986",
    name: str = "GigaDevice",
    confidence: float = 0.4,
    urgency: str = "A",
    catalyst_grade: str | None = None,
    macro_theme: str | None = None,
    days_ago: float = 1.0,
    title: str = "News headline",
) -> dict:
    event_at = _now() - timedelta(days=days_ago)
    return {
        "id": trend_id,
        "documentId": document_id,
        "trendName": "Memory demand surge",
        "macroTheme": macro_theme or "Memory demand surge",
        "catalystGrade": catalyst_grade or urgency,
        "catalyst": "AI memory demand rising",
        "globalTarget": "NVDA",
        "urgencyLevel": urgency,
        "documentTitle": title,
        "documentUrl": f"https://example.com/{document_id}",
        "documentSummary": f"Summary for {title}",
        "documentPublishedAt": event_at.isoformat(),
        "documentFetchedAt": event_at.isoformat(),
        "cnSymbols": [
            {
                "symbol": f"CN:{symbol}",
                "name": name,
                "confidence": confidence,
                "rationale": "test",
            }
        ],
    }


def test_one_high_confidence_beats_many_low():
    now = _now()
    strong = [_trend(trend_id="t1", document_id="d1", confidence=0.9, urgency="S", days_ago=0)]
    weak = [
        _trend(
            trend_id=f"t{i}",
            document_id=f"d{i}",
            confidence=0.15,
            urgency="C",
            days_ago=0,
        )
        for i in range(2, 7)
    ]
    strong_rows = aggregate_catalyst_stocks(strong, now=now)
    weak_rows = aggregate_catalyst_stocks(weak, now=now)
    assert strong_rows[0]["catalystScore"] > weak_rows[0]["catalystScore"]


def test_dedupe_same_document_keeps_highest_contribution():
    now = _now()
    trends = [
        _trend(trend_id="t1", document_id="d1", confidence=0.3, urgency="B"),
        _trend(trend_id="t2", document_id="d1", confidence=0.8, urgency="A"),
    ]
    rows = aggregate_catalyst_stocks(trends, now=now)
    assert len(rows) == 1
    assert rows[0]["articleCount"] == 1
    assert rows[0]["articles"][0]["relevance"] == 0.8


def test_score_not_linear_with_article_count():
    single = [0.6]
    many_weak = [0.6, 0.05, 0.05, 0.05, 0.05, 0.05]
    single_score = compute_stock_catalyst_score(single)
    many_score = compute_stock_catalyst_score(many_weak)
    assert many_score < single_score + 25


def test_recency_reduces_contribution():
    now = _now()
    fresh = article_contribution(
        confidence=0.8,
        urgency_level="A",
        event_at=now - timedelta(days=1),
        now=now,
    )
    stale = article_contribution(
        confidence=0.8,
        urgency_level="A",
        event_at=now - timedelta(days=25),
        now=now,
    )
    assert fresh > stale


def test_event_at_prefers_published_over_fetched():
    trend = {
        "documentPublishedAt": "2026-05-20T00:00:00+00:00",
        "documentFetchedAt": "2026-05-10T00:00:00+00:00",
    }
    event_at = event_at_for_trend(trend)
    assert event_at is not None
    assert event_at.day == 20


def test_aggregate_sorts_by_catalyst_score_desc():
    now = _now()
    trends = [
        _trend(trend_id="t1", document_id="d1", symbol="111111", confidence=0.3),
        _trend(trend_id="t2", document_id="d2", symbol="222222", confidence=0.9, urgency="S"),
    ]
    rows = aggregate_catalyst_stocks(trends, now=now)
    assert rows[0]["symbol"] == "222222"
    assert rows[0]["catalystScore"] >= rows[1]["catalystScore"]


def test_aggregate_includes_macro_theme_and_catalyst_grade_in_articles():
    now = _now()
    trends = [
        _trend(
            trend_id="t1",
            document_id="d1",
            macro_theme="HBM Supply Chain",
            catalyst_grade="S",
            urgency="B",
        )
    ]
    rows = aggregate_catalyst_stocks(trends, now=now)
    article = rows[0]["articles"][0]
    assert article["macroTheme"] == "HBM Supply Chain"
    assert article["catalystGrade"] == "S"
    assert article["catalyst"] == "AI memory demand rising"
