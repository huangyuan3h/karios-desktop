from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.trendok import (  # type: ignore[import-not-found]
    _build_industry_flow_context,
    _industry_flow_score_adjustment,
)


def _ctx() -> dict:
    return {
        "ok": True,
        "top_today_3": {"C"},
        "top_today_5": {"C", "D", "E"},
        "top_yesterday_3": {"F"},
        "top_5d_3": {"A"},
        "bottom_5d_5": {"B"},
        "net_today": {
            "F": -2.0e8,
            "G": -2.0e8,
            "H": -2.0e8,
        },
        "net_yesterday": {
            "G": -2.0e8,
        },
    }


def test_industry_flow_top_5d_bonus() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("A", _ctx())
    assert delta == 10.0
    assert parts["industry_flow_5d_top3"] == 10.0


def test_industry_flow_bottom_5d_penalty() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("B", _ctx())
    assert delta == -20.0
    assert parts["industry_flow_5d_bottom5"] == -20.0


def test_hotspot_today_top3() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("C", _ctx())
    assert delta == 5.0
    assert parts["hotspots_today_top3"] == 5.0


def test_hotspot_today_top4_5() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("D", _ctx())
    assert delta == 3.0
    assert parts["hotspots_today_top4_5"] == 3.0


def test_hotspot_falloff_big_outflow() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("F", _ctx())
    assert delta == -15.0
    assert parts["hotspot_falloff_big_outflow"] == -15.0


def test_hotspot_absent_two_day_outflow() -> None:
    delta, parts, _ = _industry_flow_score_adjustment("G", _ctx())
    assert delta == -10.0
    assert parts["hotspot_absent_2d_big_outflow"] == -10.0


def test_build_industry_flow_context_uses_batch_read_only() -> None:
    dates_5 = ["2024-01-19", "2024-01-20"]
    batch_rows = [
        {"date": "2024-01-20", "industry_name": "电子", "net_inflow": 10.0},
    ]
    with (
        patch(
            "data_sync_service.service.trendok._pick_flow_as_of_date",
            return_value="2024-01-20",
        ),
        patch(
            "data_sync_service.service.trendok.get_dates_upto",
            return_value=dates_5,
        ) as mock_dates,
        patch(
            "data_sync_service.service.trendok.get_rows_for_dates",
            return_value=batch_rows,
        ) as mock_batch,
        patch(
            "data_sync_service.db.industry_fund_flow.get_rows_by_date",
        ) as mock_by_date,
        patch(
            "data_sync_service.db.industry_fund_flow.get_sum_by_industry_for_dates",
        ) as mock_sum,
    ):
        ctx = _build_industry_flow_context("2024-01-20")

    mock_dates.assert_called_once_with("2024-01-20", 5)
    mock_batch.assert_called_once_with(dates_5)
    mock_by_date.assert_not_called()
    mock_sum.assert_not_called()
    assert ctx["ok"] is True
    assert ctx["today"] == "2024-01-20"

