from __future__ import annotations

from datetime import date
from unittest.mock import patch

from data_sync_service.service.market_sentiment import (
    FTD_INDEX_CHG_THRESHOLD,
    check_follow_through_day,
)


def test_ftd_not_triggered_without_capitulation_history() -> None:
    with patch(
        "data_sync_service.service.market_sentiment._capitulation_in_lookback",
        return_value=False,
    ):
        out = check_follow_through_day(
            as_of=date(2026, 6, 27),
            index_chg_max_pct=FTD_INDEX_CHG_THRESHOLD + 0.5,
            today_turnover_cny=2.0e12,
            prev_turnover_cny=1.5e12,
        )
    assert out["triggered"] is False
    assert "capitulation_10d:False" in out["rule"]


def test_ftd_not_triggered_when_index_gain_insufficient() -> None:
    with patch(
        "data_sync_service.service.market_sentiment._capitulation_in_lookback",
        return_value=True,
    ):
        out = check_follow_through_day(
            as_of=date(2026, 6, 27),
            index_chg_max_pct=FTD_INDEX_CHG_THRESHOLD,
            today_turnover_cny=2.0e12,
            prev_turnover_cny=1.5e12,
        )
    assert out["triggered"] is False
    assert "index_chg=" in out["rule"]


def test_ftd_not_triggered_when_turnover_not_higher() -> None:
    with patch(
        "data_sync_service.service.market_sentiment._capitulation_in_lookback",
        return_value=True,
    ):
        out = check_follow_through_day(
            as_of=date(2026, 6, 27),
            index_chg_max_pct=FTD_INDEX_CHG_THRESHOLD + 1.0,
            today_turnover_cny=1.4e12,
            prev_turnover_cny=1.5e12,
        )
    assert out["triggered"] is False
    assert "turnover=" in out["rule"]


def test_ftd_triggered_when_all_conditions_met() -> None:
    with patch(
        "data_sync_service.service.market_sentiment._capitulation_in_lookback",
        return_value=True,
    ):
        out = check_follow_through_day(
            as_of=date(2026, 6, 27),
            index_chg_max_pct=FTD_INDEX_CHG_THRESHOLD + 0.2,
            today_turnover_cny=1.8e12,
            prev_turnover_cny=1.5e12,
        )
    assert out["triggered"] is True
    assert out["raw"]["capitulationInLookback"] is True
    assert "follow_through_day" in out["rule"]


def test_ftd_overrides_capitulation_in_compute() -> None:
    from data_sync_service.service.market_sentiment import compute_cn_sentiment_for_date

    with patch(
        "data_sync_service.service.market_sentiment.fetch_cn_market_breadth_eod",
        return_value={
            "up_count": 3200,
            "down_count": 800,
            "flat_count": 100,
            "up_down_ratio": 4.0,
            "total_turnover_cny": 1.8e12,
            "total_volume": 1.0,
        },
    ), patch(
        "data_sync_service.service.market_sentiment._prev_open_date",
        return_value=date(2026, 6, 26),
    ), patch(
        "data_sync_service.service.market_sentiment._close_limit_up_pool_codes",
        return_value=[],
    ), patch(
        "data_sync_service.service.market_sentiment._failed_limitup_rate_from_db",
        return_value=(10.0, 5, 2),
    ), patch(
        "data_sync_service.service.market_sentiment.check_capitulation_bottom",
        return_value={"triggered": True, "rule": "cap", "raw": {}},
    ), patch(
        "data_sync_service.service.market_sentiment.check_follow_through_day",
        return_value={"triggered": True, "rule": "ftd_rule", "raw": {"ok": True}},
    ):
        out = compute_cn_sentiment_for_date("2026-06-27")

    assert out["riskMode"] == "confirmed_uptrend"
    assert "ftd_rule" in out["rules"]
