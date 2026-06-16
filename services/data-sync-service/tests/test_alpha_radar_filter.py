"""Tests for Alpha Radar topic filter."""

from __future__ import annotations

import os
from unittest.mock import patch

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


def test_cls_policy_filter() -> None:
    assert passes_topic_filter(
        title="【发改委】下达1万亿超长期特别国债用于设备更新",
        summary="",
        source_id="cls-policy",
    )
    assert not passes_topic_filter(
        title="某地方补贴100万支持创业",
        summary="",
        source_id="cls-policy",
    )


def test_cycle_reversal_filter() -> None:
    assert passes_topic_filter(
        title="铜价突破历史新高，现货提价",
        summary="全球铜供给紧缺",
        source_id="eastmoney-copper",
    )
    assert not passes_topic_filter(
        title="日常设备检修公告",
        summary="",
        source_id="eastmoney-copper",
    )


def test_gov_curated_source_skips_keyword_gate() -> None:
    assert passes_topic_filter(
        title="2026年6月4日国内成品油价格调整",
        summary="",
        source_id="gov-ndrc-policy",
    )


def test_commodity_filter_requires_move_signal() -> None:
    assert passes_topic_filter(
        title="铜价突破历史新高",
        summary="全球铜供给紧缺",
        source_id="wallstreetcn-commodity",
    )
    assert not passes_topic_filter(
        title="铜价微幅波动0.5%",
        summary="日常报价",
        source_id="wallstreetcn-commodity",
    )


def test_consensus_filter() -> None:
    assert passes_topic_filter(
        title="光伏行业深度：产能出清与历史底部",
        summary="",
        source_id="cls-depth",
    )
    assert passes_topic_filter(
        title="【明日主题前瞻】AI硬件迭代推动PCB价值量攀升",
        summary="",
        source_id="cls-depth",
    )


def test_filter_strict_off_allows_chinese_source_without_keywords() -> None:
    with patch.dict(os.environ, {"ALPHA_RADAR_FILTER_STRICT": "0"}):
        assert passes_topic_filter(
            title="普通标题",
            summary="",
            source_id="cls-policy",
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
