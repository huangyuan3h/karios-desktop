"""Tests for Alpha Radar topic filter."""

from __future__ import annotations

from data_sync_service.service.alpha_radar_filter import (
    filter_feed_items,
    passes_topic_filter,
)


def test_trusted_source_skips_include_requirement() -> None:
    assert passes_topic_filter(
        title="Weekly Update",
        summary="Strategy notes without chip keywords.",
        source_id="stratechery",
    )


def test_exclude_biomedical() -> None:
    assert not passes_topic_filter(
        title="Lung cancer precision medicine trial",
        summary="Clinical results",
        source_id="mit-tech-review",
    )


def test_include_semiconductor_keyword() -> None:
    assert passes_topic_filter(
        title="New GPU datacenter deployment",
        summary="Hyperscaler capex rises",
        source_id="mit-tech-review",
    )


def test_filter_feed_items_stats() -> None:
    items = [
        {"title": "HBM supply tightens", "summary": "memory semiconductor"},
        {"title": "Celebrity gossip", "summary": "fashion"},
    ]
    kept, stats = filter_feed_items(items, source_id="mit-tech-review")
    assert len(kept) == 1
    assert stats["fetched"] == 2
    assert stats["filteredOut"] == 1
    assert stats["stored"] == 1
