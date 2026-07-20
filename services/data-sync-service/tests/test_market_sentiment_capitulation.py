from __future__ import annotations

from datetime import date
from unittest.mock import patch

from data_sync_service.service.market_sentiment import (
    CAPITULATION_DOWN_THRESHOLD,
    CAPITULATION_FLOW_THRESHOLD_YI,
    CAPITULATION_IV_THRESHOLD,
    check_capitulation_bottom,
)


def test_capitulation_not_triggered_when_breadth_below_threshold() -> None:
    with patch(
        "data_sync_service.db.macro_daily.get_latest_row",
        return_value={"close": 25.0},
    ), patch(
        "data_sync_service.db.etf_fund_flow.get_last_trade_date",
        return_value=date(2026, 6, 25),
    ), patch(
        "data_sync_service.db.etf_fund_flow.fetch_row",
        return_value={
            "main_net_inflow": 3_000_000_000.0,
            "super_large_net_inflow": 0.0,
        },
    ):
        out = check_capitulation_bottom(down=CAPITULATION_DOWN_THRESHOLD - 1, as_of=date(2026, 6, 25))

    assert out["triggered"] is False
    assert "breadth_down=" in out["rule"]


def test_capitulation_not_triggered_when_iv_below_threshold() -> None:
    with patch(
        "data_sync_service.db.macro_daily.get_latest_row",
        return_value={"close": CAPITULATION_IV_THRESHOLD},
    ), patch(
        "data_sync_service.db.etf_fund_flow.get_last_trade_date",
        return_value=date(2026, 6, 25),
    ), patch(
        "data_sync_service.db.etf_fund_flow.fetch_row",
        return_value={
            "main_net_inflow": 3_000_000_000.0,
            "super_large_net_inflow": 0.0,
        },
    ):
        out = check_capitulation_bottom(down=CAPITULATION_DOWN_THRESHOLD, as_of=date(2026, 6, 25))

    assert out["triggered"] is False
    assert out["raw"]["ivPct"] == CAPITULATION_IV_THRESHOLD


def test_capitulation_not_triggered_when_etf_flow_below_threshold() -> None:
    flow_yi = CAPITULATION_FLOW_THRESHOLD_YI - 1.0
    with patch(
        "data_sync_service.db.macro_daily.get_latest_row",
        return_value={"close": 22.0},
    ), patch(
        "data_sync_service.db.etf_fund_flow.get_last_trade_date",
        return_value=date(2026, 6, 25),
    ), patch(
        "data_sync_service.db.etf_fund_flow.fetch_row",
        return_value={
            "main_net_inflow": flow_yi * 1e8,
            "super_large_net_inflow": 0.0,
        },
    ):
        out = check_capitulation_bottom(down=CAPITULATION_DOWN_THRESHOLD, as_of=date(2026, 6, 25))

    assert out["triggered"] is False
    assert out["raw"]["mainFlowYi"] == flow_yi


def test_capitulation_triggered_when_all_three_conditions_met() -> None:
    with patch(
        "data_sync_service.db.macro_daily.get_latest_row",
        return_value={"close": 22.5},
    ), patch(
        "data_sync_service.db.etf_fund_flow.get_last_trade_date",
        return_value=date(2026, 6, 25),
    ), patch(
        "data_sync_service.db.etf_fund_flow.fetch_row",
        return_value={
            "main_net_inflow": 0.0,
            "super_large_net_inflow": (CAPITULATION_FLOW_THRESHOLD_YI + 1.0) * 1e8,
        },
    ):
        out = check_capitulation_bottom(down=3600, as_of=date(2026, 6, 25))

    assert out["triggered"] is True
    assert out["raw"]["ivPct"] == 22.5
    assert out["raw"]["superLargeFlowYi"] == CAPITULATION_FLOW_THRESHOLD_YI + 1.0
    assert "capitulation_v_bottom" in out["rule"]
