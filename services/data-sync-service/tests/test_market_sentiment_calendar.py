"""Market sentiment sync respects SSE trade calendar."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

from data_sync_service.service import market_sentiment as ms


def test_sync_cn_sentiment_skips_non_trading_day() -> None:
    with (
        patch.object(ms, "is_cn_trading_day", return_value=False),
        patch.object(ms, "list_days", return_value=[{"date": "2026-06-18", "upCount": 1}]),
        patch.object(ms, "compute_cn_sentiment_for_date") as mock_compute,
        patch.object(ms, "upsert_daily_rows") as mock_upsert,
    ):
        out = ms.sync_cn_sentiment(date_str="2026-06-19", force=True)

    assert out.get("skipped") is True
    assert out.get("reason") == "not_trading_day"
    mock_compute.assert_not_called()
    mock_upsert.assert_not_called()


def test_sync_cn_sentiment_no_stale_forward_fill_on_compute_error() -> None:
    with (
        patch.object(ms, "is_cn_trading_day", return_value=True),
        patch.object(ms, "compute_cn_sentiment_for_date", side_effect=RuntimeError("boom")),
        patch.object(
            ms,
            "list_days",
            return_value=[{"date": "2026-06-18", "upCount": 100, "downCount": 50}],
        ),
        patch.object(ms, "upsert_daily_rows") as mock_upsert,
    ):
        out = ms.sync_cn_sentiment(date_str="2026-06-22", force=True)

    assert out.get("reason") == "compute_failed"
    assert out.get("asOfDate") == "2026-06-18"
    mock_upsert.assert_not_called()


def test_sync_cn_industry_skips_non_trading_day() -> None:
    from data_sync_service.service import industry_fund_flow as iff

    with (
        patch.object(iff, "shanghai_today", return_value=date(2026, 6, 19)),
        patch.object(iff, "is_cn_trading_day", return_value=False),
        patch.object(iff, "fetch_cn_industry_fund_flow_eod") as mock_fetch,
        patch.object(iff, "upsert_daily_rows") as mock_upsert,
    ):
        out = iff.sync_cn_industry_fund_flow()

    assert out.get("skipped") is True
    assert out.get("reason") == "not_trading_day"
    mock_fetch.assert_not_called()
    mock_upsert.assert_not_called()
