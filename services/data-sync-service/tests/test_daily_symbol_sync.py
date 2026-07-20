"""Tests for per-symbol incremental daily sync (bars force path)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.requires_postgres

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from data_sync_service.service.daily import sync_daily_for_ts_code


def test_sync_daily_for_ts_code_missing_api_key() -> None:
    with patch("data_sync_service.service.daily.get_settings") as mock_settings:
        mock_settings.return_value.tu_share_api_key = ""
        result = sync_daily_for_ts_code("000001.SZ")
    assert result["ok"] is False
    assert "TU_SHARE_API_KEY" in str(result.get("error", ""))


def test_sync_daily_for_ts_code_skips_when_up_to_date() -> None:
    with (
        patch("data_sync_service.service.daily.get_settings") as mock_settings,
        patch("data_sync_service.service.daily.get_last_trade_date") as mock_last,
        patch("data_sync_service.service.daily._today_yyyymmdd", return_value="20260618"),
    ):
        mock_settings.return_value.tu_share_api_key = "test-key"
        mock_last.return_value = date(2026, 6, 18)
        result = sync_daily_for_ts_code("000001.SZ")
    assert result["ok"] is True
    assert result.get("skipped") is True
    assert result.get("updated") == 0


def test_sync_daily_for_ts_code_incremental_fetch() -> None:
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260618",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "pre_close": 10.0,
                "change": 0.5,
                "pct_chg": 5.0,
                "vol": 1000.0,
                "amount": 2000.0,
            }
        ]
    )
    pro = MagicMock()
    pro.daily.return_value = df

    with (
        patch("data_sync_service.service.daily.get_settings") as mock_settings,
        patch("data_sync_service.service.daily.get_last_trade_date") as mock_last,
        patch("data_sync_service.service.daily._today_yyyymmdd", return_value="20260618"),
        patch("data_sync_service.service.daily.ts.pro_api", return_value=pro),
        patch("data_sync_service.service.daily.upsert_from_dataframe", return_value=1) as mock_upsert,
    ):
        mock_settings.return_value.tu_share_api_key = "test-key"
        mock_last.return_value = date(2026, 6, 17)
        result = sync_daily_for_ts_code("000001.SZ")

    assert result["ok"] is True
    assert result.get("updated") == 1
    pro.daily.assert_called_once()
    call_kwargs = pro.daily.call_args.kwargs
    assert call_kwargs["ts_code"] == "000001.SZ"
    assert call_kwargs["start_date"] == "20260618"
    assert call_kwargs["end_date"] == "20260618"
    mock_upsert.assert_called_once()


def test_get_market_bars_force_triggers_sync() -> None:
    from data_sync_service.service.market_bars import get_market_bars

    with (
        patch("data_sync_service.service.market_bars.sync_daily_for_ts_code") as mock_sync,
        patch("data_sync_service.service.market_bars.fetch_last_bars", return_value=[]),
        patch("data_sync_service.service.market_bars._lookup_name", return_value="Ping An"),
    ):
        mock_sync.return_value = {"ok": True, "updated": 1}
        payload = get_market_bars("CN:000001", days=60, force=True)

    mock_sync.assert_called_once_with("000001.SZ")
    assert payload["symbol"] == "CN:000001"
    assert payload["bars"] == []
