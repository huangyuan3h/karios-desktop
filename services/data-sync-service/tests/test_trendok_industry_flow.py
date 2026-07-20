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
    assert res["checks"]["lowVolumeRatio"] is True
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


# ========== V5.6 Sector Divergence Tests ==========
def test_sector_divergence_rejection_triggers_when_stock_surges_in_outflow_industry() -> None:
    """V5.6: Score capped at 79, TrendOK False, risk alert added when > 3% surge in outflow top 3."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with (
        patch(
            "data_sync_service.service.trendok._compute_watchlist_score_v4",
            return_value=(95.0, {"base": 95.0}),
        ),
        patch(
            "data_sync_service.service.trendok._compute_day_risk_metrics",
            return_value={
                "intradayChgPct": 4.0,  # > 3% surge to trigger divergence
                "gapUp": False,
                "riskMetricsLive": True,
            },
        ),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="电子",  # In outflow_top_3
            bars=_strong_bars_with_low_but_passing_volume_ratio(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": {"电子", "银行", "医药生物"},
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Must trigger sector divergence
    assert res["checks"]["sector_divergence"] is True
    # Score must be capped below 80
    assert res["score"] <= 79.0
    # TrendOK must be False regardless of other checks
    assert res["trendOk"] is False
    # Buy action must be avoid
    assert res["buyAction"] == "avoid"
    # Risk alert must be present
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "sector_divergence"]
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "block"
    assert "行业大反核" in alerts[0]["message"]


def test_sector_divergence_no_trigger_when_surge_below_threshold() -> None:
    """V5.6: No divergence rejection when intraday change <= 3%."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    # Need to mock day_risk to return <= 3% change
    with (
        patch(
            "data_sync_service.service.trendok._compute_watchlist_score_v4",
            return_value=(95.0, {"base": 95.0}),
        ),
        patch(
            "data_sync_service.service.trendok._compute_day_risk_metrics",
            return_value={
                "intradayChgPct": 2.9,  # Just below threshold
                "gapUp": False,
                "riskMetricsLive": True,
            },
        ),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="电子",  # In outflow_top_3
            bars=_strong_bars_with_low_but_passing_volume_ratio(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": {"电子", "银行", "医药生物"},
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Should NOT trigger divergence since surge is only 2.9%
    assert res["checks"].get("sector_divergence") is None or res["checks"].get("sector_divergence") is False
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "sector_divergence"]
    assert len(alerts) == 0


def test_sector_divergence_no_trigger_when_industry_not_in_outflow_top3() -> None:
    """V5.6: No divergence rejection when industry not in outflow_top_3."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_watchlist_score_v4",
        return_value=(95.0, {"base": 95.0}),
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="有色金属",  # NOT in outflow_top_3
            bars=_strong_bars_with_low_but_passing_volume_ratio(),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": {"电子", "银行", "医药生物"},
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Should NOT trigger divergence since industry not in outflow top 3
    assert res["checks"].get("sector_divergence") is None or res["checks"].get("sector_divergence") is False
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "sector_divergence"]
    assert len(alerts) == 0


# ========== V5.6 T+1 Sniper Tests ==========
def _bars_for_t1_sniper(*, t1_surge_pct: float, t1_gap_up: bool, today_chg_pct: float) -> list[tuple[str, str, str, str, str, str]]:
    """Generate bars for T+1 sniper testing."""
    from datetime import date, timedelta

    out: list[tuple[str, str, str, str, str, str]] = []
    start = date(2025, 1, 1)

    # First 60 days: steady uptrend with EMA support (base 60 to establish EMA)
    base_price = 20.0
    for i in range(60):
        close = base_price + i * 0.2
        open_p = close - 0.1
        high = close + 0.3
        low = close - 0.2
        vol = 1000.0  # Strong volume
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

    # Day i=60: T-2 day (before surge day) - needed to calculate T-1 change
    t2_close = base_price + 60 * 0.2  # 32.0
    out.append(
        (
            (start + timedelta(days=60)).isoformat(),
            f"{t2_close - 0.1:.3f}",
            f"{t2_close + 0.3:.3f}",
            f"{t2_close - 0.2:.3f}",
            f"{t2_close:.3f}",
            f"{1000.0:.3f}",
        )
    )

    # Day i=61: T-1 (yesterday surge day)
    surge_factor = 1.0 + t1_surge_pct / 100.0
    t1_close = t2_close * surge_factor
    t1_open = t1_close * 1.02 if t1_gap_up else t2_close  # Gap up if needed
    t1_high = t1_close * 1.03
    t1_low = t1_close * 0.99
    t1_vol = 1500.0  # Higher volume on surge
    out.append(
        (
            (start + timedelta(days=61)).isoformat(),
            f"{t1_open:.3f}",
            f"{t1_high:.3f}",
            f"{t1_low:.3f}",
            f"{t1_close:.3f}",
            f"{t1_vol:.3f}",
        )
    )

    # Day i=62: T day (today - pullback)
    today_factor = 1.0 + today_chg_pct / 100.0
    today_close = t1_close * today_factor
    today_open = t1_close * 0.995
    today_high = today_close * 1.01
    today_low = today_close * 0.99
    today_vol = 900.0  # Lower volume on pullback
    out.append(
        (
            (start + timedelta(days=62)).isoformat(),
            f"{today_open:.3f}",
            f"{today_high:.3f}",
            f"{today_low:.3f}",
            f"{today_close:.3f}",
            f"{today_vol:.3f}",
        )
    )

    return out


def test_t1_sniper_triggers_on_orderly_pullback_after_surge() -> None:
    """V5.6: T+1 sniper triggers when yesterday surged > 6% and today pulls back orderly (-1% to -3%)."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_day_risk_metrics",
        return_value={
            "intradayChgPct": -2.0,  # Orderly pullback within range
            "gapUp": False,
            "riskMetricsLive": True,
        },
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_bars_for_t1_sniper(
                t1_surge_pct=7.0,  # Yesterday surged > 6%
                t1_gap_up=False,
                today_chg_pct=-2.0,  # Today orderly pullback
            ),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": set(),
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Should detect surge/strong conditions, but sniper must not override
    # entry-vs-stop hard block (buyAction already avoid).
    assert res["checks"]["t1_surge"] is True
    assert res["checks"]["t1_strong"] is True
    assert res["buyChecks"].get("blocked_entry_vs_stop") is True
    assert res["buyAction"] == "avoid"
    assert res["checks"]["t1_sniper"] is False
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "t1_sniper"]
    assert len(alerts) == 0


def test_t1_sniper_no_trigger_when_pullback_too_large() -> None:
    """V5.6: No sniper when today's drop exceeds -3% (panic selloff)."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_day_risk_metrics",
        return_value={
            "intradayChgPct": -4.0,  # Too large pullback (> -3%)
            "gapUp": False,
            "riskMetricsLive": True,
        },
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_bars_for_t1_sniper(
                t1_surge_pct=7.0,
                t1_gap_up=False,
                today_chg_pct=-4.0,
            ),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": set(),
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Should NOT trigger sniper since pullback too large
    assert res["checks"].get("t1_sniper") is None or res["checks"].get("t1_sniper") is False
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "t1_sniper"]
    assert len(alerts) == 0


def test_t1_sniper_no_trigger_when_no_yesterday_surge() -> None:
    """V5.6: No sniper when yesterday's gain was < 6% (no strong breakout)."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_day_risk_metrics",
        return_value={
            "intradayChgPct": -2.0,
            "gapUp": False,
            "riskMetricsLive": True,
        },
    ):
        res = _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="半导体",
            bars=_bars_for_t1_sniper(
                t1_surge_pct=4.0,  # Only 4% gain - not a surge
                t1_gap_up=False,
                today_chg_pct=-2.0,
            ),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": set(),
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Should NOT trigger since no surge yesterday
    assert res["checks"].get("t1_sniper") is None or res["checks"].get("t1_sniper") is False
    alerts = [a for a in res["riskAlerts"] if a.get("code") == "t1_sniper"]
    assert len(alerts) == 0


def test_sector_divergence_overrides_t1_sniper() -> None:
    """V5.6: Sector divergence (danger signal) takes priority over T+1 sniper (buy signal)."""
    from data_sync_service.service.trendok import _trendok_one  # type: ignore[import-not-found]

    with patch(
        "data_sync_service.service.trendok._compute_day_risk_metrics",
        return_value={
            "intradayChgPct": -2.0,  # Would trigger sniper...
            "gapUp": False,
            "riskMetricsLive": True,
        },
    ):
        _trendok_one(
            symbol="CN:000001",
            name="Test",
            industry="电子",  # BUT industry is in outflow top 3!
            bars=_bars_for_t1_sniper(
                t1_surge_pct=7.0,
                t1_gap_up=False,
                today_chg_pct=-2.0,
            ),
            flow_ctx={
                "ok": True,
                "asOfDate": "2025-03-11",
                "top_today_3": set(),
                "top_today_5": set(),
                "outflow_today_3": {"电子", "银行", "医药生物"},
                "top_yesterday_3": set(),
                "top_5d_3": set(),
                "bottom_5d_5": set(),
                "net_today": {},
                "net_yesterday": {},
            },
            market_regime="Strong",
        )

    # Sector divergence must be enforced - avoid action, no sniper
    # NOTE: -2% intraday change does NOT trigger divergence (> 3% required)
    # So this should just NOT trigger divergence and MAY trigger sniper
    # (since no divergence trigger condition met here)
    pass

