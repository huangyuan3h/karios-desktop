import pytest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from data_sync_service.service.hk_basic import (
    _parse_iso_datetime,
    _is_same_utc_month,
    map_hk_basic_to_stock_basic_df,
)


def test_parse_iso_datetime_valid():
    result = _parse_iso_datetime("2024-01-15T10:30:00+00:00")
    assert result is not None
    assert result.year == 2024


def test_parse_iso_datetime_datetime():
    dt = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    result = _parse_iso_datetime(dt)
    assert result == dt


def test_parse_iso_datetime_none():
    assert _parse_iso_datetime(None) is None
    assert _parse_iso_datetime("") is None


def test_parse_iso_datetime_invalid():
    assert _parse_iso_datetime("invalid") is None


def test_is_same_utc_month_true():
    a = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    b = datetime(2024, 1, 20, 15, 45, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is True


def test_is_same_utc_month_false():
    a = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    b = datetime(2024, 2, 20, 15, 45, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is False


def test_is_same_utc_month_different_year():
    a = datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
    b = datetime(2025, 1, 20, 15, 45, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is False


def test_is_same_utc_month_with_timezone():
    a = datetime(2024, 1, 15, 22, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    b = datetime(2024, 1, 16, 2, 45, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is True


def test_map_hk_basic_to_stock_basic_df_empty():
    result = map_hk_basic_to_stock_basic_df(pd.DataFrame())
    assert len(result) == 0
    assert "ts_code" in result.columns


def test_map_hk_basic_to_stock_basic_df_valid():
    hk_df = pd.DataFrame({
        "ts_code": ["00700.HK", "00001.HK"],
        "name": ["腾讯控股", "长和"],
        "list_date": ["20040616", "19990101"],
        "delist_date": [None, None],
    })
    result = map_hk_basic_to_stock_basic_df(hk_df)
    assert len(result) == 2
    assert result["ts_code"].tolist() == ["00700.HK", "00001.HK"]
    assert result["symbol"].tolist() == ["00700", "00001"]
    assert result["market"].tolist() == ["HK", "HK"]