"""Tests for Alpha Radar risk fusion helpers."""

from __future__ import annotations

from data_sync_service.service.alpha_radar_risk import (
    build_mainline_score_map,
    compute_risk_status,
    keyword_matches_industry,
)


def test_keyword_matches_industry_partial():
    assert keyword_matches_industry(["液冷", "CDU"], "液冷设备")
    assert keyword_matches_industry(["通信"], "通信设备")
    assert not keyword_matches_industry(["半导体"], "通信设备")


def test_compute_risk_status_armed():
    status = compute_risk_status(
        keywords=["液冷", "数据中心"],
        hot_industry_names=["液冷设备"],
        mainline_by_industry={"液冷设备": 85.0},
        mainline_threshold=80.0,
    )
    assert status == "armed"


def test_compute_risk_status_waiting():
    status = compute_risk_status(
        keywords=["液冷"],
        hot_industry_names=["液冷设备"],
        mainline_by_industry={"液冷设备": 70.0},
        mainline_threshold=80.0,
    )
    assert status == "waiting_v2_flow"


def test_build_mainline_score_map():
    payload = {
        "allScores": [
            {"industryName": "通信", "totalScore": 82.5},
            {"industryName": "银行", "totalScore": 55.0},
        ]
    }
    m = build_mainline_score_map(payload)
    assert m["通信"] == 82.5
    assert m["银行"] == 55.0
