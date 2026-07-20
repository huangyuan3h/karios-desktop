"""Tests for hybrid A-share symbol resolution."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.requires_postgres

from unittest.mock import patch

from data_sync_service.service.alpha_radar_symbol_resolve import (
    _normalize_ticker,
    resolve_a_share_mapping,
)


def test_normalize_ticker_variants():
    assert _normalize_ticker("600000") == "600000"
    assert _normalize_ticker("CN:600000") == "600000"
    assert _normalize_ticker("SH600000") == "600000"


def test_resolve_a_share_mapping_unresolved_name():
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.search_cn_candidates",
        return_value=[],
    ):
        resolved, unresolved = resolve_a_share_mapping(["不存在的公司名"])
    assert resolved == []
    assert unresolved == ["不存在的公司名"]


def test_resolve_a_share_mapping_by_ticker():
    with patch(
        "data_sync_service.service.alpha_radar_symbol_resolve.fetch_market_stocks",
        return_value=(
            1,
            [{"symbol": "CN:600000", "ticker": "600000", "name": "Test Co"}],
        ),
    ):
        resolved, unresolved = resolve_a_share_mapping(["600000"], logic_summary="测试逻辑")
    assert len(resolved) == 1
    assert resolved[0]["symbol"] == "CN:600000"
    assert unresolved == []
