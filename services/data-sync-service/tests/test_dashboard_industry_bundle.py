"""Dashboard industry bundle batch read tests (OPT-026)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.dashboard import _build_industry_bundle
from data_sync_service.service.industry_fund_flow_read import build_dashboard_industry_bundle

FIXTURE_ROWS = [
    {"date": "2024-06-14", "industry_code": "c1", "industry_name": "电子", "net_inflow": 10.0},
    {"date": "2024-06-15", "industry_code": "c1", "industry_name": "电子", "net_inflow": 20.0},
    {"date": "2024-06-14", "industry_code": "c2", "industry_name": "计算机", "net_inflow": 30.0},
    {"date": "2024-06-15", "industry_code": "c2", "industry_name": "计算机", "net_inflow": -5.0},
]


def test_build_dashboard_industry_bundle_shape() -> None:
    dates = ["2024-06-14", "2024-06-15"]
    out = build_dashboard_industry_bundle(as_of_date="2024-06-15", dates=dates, rows=FIXTURE_ROWS)
    assert out["asOfDate"] == "2024-06-15"
    assert out["days"] == 5
    assert out["topK"] == 5
    assert out["dates"] == dates
    assert len(out["topByDate"]) == 2
    assert out["topByDate"][1]["top"][0] == "电子"
    assert "flow5d" in out
    assert "flow5dOut" in out
    assert "dailyRankings" in out
    assert len(out["flow5d"]["top"]) <= 10


def test_build_industry_bundle_single_batch_read() -> None:
    dates = ["2024-06-14", "2024-06-15"]
    with (
        patch(
            "data_sync_service.service.dashboard.get_dates_upto",
            return_value=dates,
        ) as mock_dates,
        patch(
            "data_sync_service.service.dashboard.get_rows_for_dates",
            return_value=FIXTURE_ROWS,
        ) as mock_rows,
    ):
        out = _build_industry_bundle(as_of_date="2024-06-15")

    mock_dates.assert_called_once_with("2024-06-15", 5)
    mock_rows.assert_called_once_with(dates)
    assert out["asOfDate"] == "2024-06-15"
    assert out["flow5d"]["dates"] == dates
