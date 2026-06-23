from __future__ import annotations

from datetime import date, timedelta
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
            "data_sync_service.service.trendok.trade_dates_upto",
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

    assert mock_dates.call_args[0] == ("2024-01-20", 5)
    mock_batch.assert_called_once_with(dates_5)
    mock_by_date.assert_not_called()
    mock_sum.assert_not_called()
    assert ctx["ok"] is True
    assert ctx["today"] == "2024-01-20"


def _strong_bars_with_weak_last5_volume() -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    start = date(2025, 1, 1)
    for i in range(70):
        close = 20.0 + i * 0.45 + max(0, i - 50) * 0.2
        open_p = close - 0.25
        high = close + 0.45
        low = close - 0.55
        vol = 100.0 if i >= 65 else 1000.0
        out.append(
            (
                (start + timedelta(days=i)).isoformat(),
                f"{open_p:.3f}",
                f"{high:.3f}",
                f"{low:.3f}",
                f"{close:.3f}",
                f"{vol:.3f}",
            )
        )
    return out


def test_positive_industry_bonus_requires_full_trendok_and_volume() -> None:
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(95.756, {"base": 95.756}),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_strong_bars_with_weak_last5_volume(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "top_yesterday_3": set(),
                "top_5d_3": {"半导体"},
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    assert res["checks"]["volumeSurge"] is False
    assert res["trendOk"] is False
    assert "industry_flow_5d_top3" not in res["scoreParts"]
    assert res["values"]["industryFlowReasons"] == ["industry_flow_5d_top3"]
    assert res["score"] == 79.0
    assert round(res["score"]) < 80


def _strong_bars_with_low_but_passing_volume_ratio() -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    start = date(2025, 1, 1)
    for i in range(70):
        close = 20.0 + i * 0.08 + (0.15 if i % 3 else -0.15) + max(0, i - 50) * 0.02
        open_p = close - 0.1
        high = close + 0.3
        low = close - 0.4
        vol = 1100.0 if i >= 65 else 1000.0
        out.append(
            (
                (start + timedelta(days=i)).isoformat(),
                f"{open_p:.3f}",
                f"{high:.3f}",
                f"{low:.3f}",
                f"{close:.3f}",
                f"{vol:.3f}",
            )
        )
    return out


def test_low_volume_ratio_caps_after_positive_industry_bonus() -> None:
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(95.0, {"base": 95.0}),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_strong_bars_with_low_but_passing_volume_ratio(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "top_yesterday_3": set(),
                "top_5d_3": {"半导体"},
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    assert res["checks"]["volumeSurge"] is True
    assert res["values"]["volumeRatio"] < 1.2
    assert res["trendOk"] is False
    assert res["score"] == 79.0
    assert res["scoreParts"]["industry_flow_5d_top3"] == 10.0
    assert res["scoreParts"]["low_volume_ratio_cap"] == 79.0


def test_negative_industry_penalty_applies_even_when_trendok_fails() -> None:
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(95.756, {"base": 95.756}),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_strong_bars_with_weak_last5_volume(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": {"半导体"},
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    assert res["checks"]["volumeSurge"] is False
    assert res["trendOk"] is False
    assert res["scoreParts"]["industry_flow_5d_bottom5"] == -20.0
    assert res["score"] < 79.0

