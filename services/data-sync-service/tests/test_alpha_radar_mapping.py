"""Tests for Alpha Radar A-share mapping helpers."""

from __future__ import annotations

import pytest

from data_sync_service.service.alpha_radar_mapping import _normalize_keyword, search_cn_candidates


def test_normalize_keyword_strips_a_share_suffix():
    assert _normalize_keyword("液冷 A股") == "液冷"
    assert _normalize_keyword("RNA甲基化") == "RNA甲基化"


@pytest.mark.requires_postgres
def test_search_cn_candidates_returns_list():
    # Should not raise even when DB is empty in CI.
    out = search_cn_candidates(["通信", "600050"])
    assert isinstance(out, list)
