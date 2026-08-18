"""Tests for dashboard_summary pre-market as_of clamping and marketStatus meta."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import dashboard as dash


def test_dashboard_summary_clamps_as_of_when_premarket() -> None:
    """
    Pre-market on a trading day: as_of should be clamped to the previous open day
    so the industry matrix does not inject an empty "today" column.
    """
    today_iso = "2026-06-25"
    prev_iso = "2026-06-24"

    with (
        patch(
            "data_sync_service.service.dashboard.get_latest_sentiment_date",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.dashboard.get_latest_industry_date",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.dashboard.shanghai_today_iso",
            return_value=today_iso,
        ),
        patch(
            "data_sync_service.service.dashboard.resolve_effective_as_of",
            return_value=today_iso,
        ),
        patch(
            "data_sync_service.service.dashboard.compute_market_status",
            return_value={
                "phase": "PreOpen",
                "isTradingDay": True,
                "isPreMarket": True,
                "isMarketOpen": False,
                "asOfTime": "06:59",
            },
        ),
        patch(
            "data_sync_service.service.dashboard.previous_open_date",
            return_value=__import__("datetime").date.fromisoformat(prev_iso),
        ),
        patch("data_sync_service.service.dashboard._build_industry_bundle") as mock_ind,
        patch("data_sync_service.service.dashboard._build_market_sentiment_bundle") as mock_sent,
        patch("data_sync_service.service.dashboard._news_items") as mock_news,
        patch("data_sync_service.service.dashboard.build_macro_snapshot") as mock_macro,
    ):
        mock_ind.return_value = {}
        mock_sent.return_value = {}
        mock_news.return_value = {"hours": 24, "total": 0, "items": []}
        mock_macro.return_value = None

        out = dash.dashboard_summary()

    # as_of must be clamped to the previous open day, not today.
    assert out["asOfDate"] == prev_iso
    assert out["meta"]["marketStatus"]["phase"] == "PreOpen"
    assert out["meta"]["marketStatus"]["isPreMarket"] is True
    # _build_industry_bundle must receive the clamped date.
    assert mock_ind.call_args.kwargs["as_of_date"] == prev_iso


def test_dashboard_summary_keeps_today_when_market_open() -> None:
    """When market is open, as_of should stay as today (no clamp)."""
    today_iso = "2026-06-25"

    with (
        patch(
            "data_sync_service.service.dashboard.get_latest_sentiment_date",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.dashboard.get_latest_industry_date",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.dashboard.shanghai_today_iso",
            return_value=today_iso,
        ),
        patch(
            "data_sync_service.service.dashboard.resolve_effective_as_of",
            return_value=today_iso,
        ),
        patch(
            "data_sync_service.service.dashboard.compute_market_status",
            return_value={
                "phase": "Open",
                "isTradingDay": True,
                "isPreMarket": False,
                "isMarketOpen": True,
                "asOfTime": "10:15",
            },
        ),
        patch("data_sync_service.service.dashboard._build_industry_bundle") as mock_ind,
        patch("data_sync_service.service.dashboard._build_market_sentiment_bundle") as mock_sent,
        patch("data_sync_service.service.dashboard._news_items") as mock_news,
        patch("data_sync_service.service.dashboard.build_macro_snapshot") as mock_macro,
    ):
        mock_ind.return_value = {}
        mock_sent.return_value = {}
        mock_news.return_value = {"hours": 24, "total": 0, "items": []}
        mock_macro.return_value = None

        out = dash.dashboard_summary()

    assert out["asOfDate"] == today_iso
    assert out["meta"]["marketStatus"]["phase"] == "Open"
    assert mock_ind.call_args.kwargs["as_of_date"] == today_iso
