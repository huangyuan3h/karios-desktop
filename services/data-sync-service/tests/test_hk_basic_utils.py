import pytest
import pandas as pd
from datetime import datetime, timezone
from data_sync_service.service.hk_basic import (
    _parse_iso_datetime,
    _is_same_utc_month,
    map_hk_basic_to_stock_basic_df,
)


def test_parse_iso_datetime_none():
    assert _parse_iso_datetime(None) is None
    assert _parse_iso_datetime("") is None


def test_parse_iso_datetime_datetime():
    dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
    assert _parse_iso_datetime(dt) == dt


def test_parse_iso_datetime_string():
    result = _parse_iso_datetime("2024-01-15T10:30:00+00:00")
    assert result is not None
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_parse_iso_datetime_invalid_string():
    assert _parse_iso_datetime("invalid") is None


def test_is_same_utc_month_true():
    a = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    b = datetime(2024, 1, 31, 23, 59, 59, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is True


def test_is_same_utc_month_false_different_month():
    a = datetime(2024, 1, 15, tzinfo=timezone.utc)
    b = datetime(2024, 2, 1, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is False


def test_is_same_utc_month_false_different_year():
    a = datetime(2024, 1, 15, tzinfo=timezone.utc)
    b = datetime(2023, 1, 15, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is False


def test_is_same_utc_month_with_different_timezones():
    from zoneinfo import ZoneInfo
    a = datetime(2024, 1, 15, 23, 0, tzinfo=ZoneInfo("America/New_York"))
    b = datetime(2024, 1, 16, 4, 0, tzinfo=timezone.utc)
    assert _is_same_utc_month(a, b) is True


def test_map_hk_basic_to_stock_basic_df_empty():
    result = map_hk_basic_to_stock_basic_df(None)
    assert result.empty
    assert list(result.columns) == ["ts_code", "symbol", "name", "industry", "market", "list_date", "delist_date"]


def test_map_hk_basic_to_stock_basic_df_empty_dataframe():
    result = map_hk_basic_to_stock_basic_df(pd.DataFrame())
    assert result.empty


def test_map_hk_basic_to_stock_basic_df_basic():
    hk_df = pd.DataFrame({
        "ts_code": ["00700.HK", "00941.HK"],
        "name": ["腾讯控股", "中国移动"],
        "list_date": ["20040616", "19971023"],
        "delist_date": [None, None],
    })
    result = map_hk_basic_to_stock_basic_df(hk_df)
    assert len(result) == 2
    assert result["ts_code"].tolist() == ["00700.HK", "00941.HK"]
    assert result["symbol"].tolist() == ["00700", "00941"]
    assert result["name"].tolist() == ["腾讯控股", "中国移动"]
    assert result["market"].tolist() == ["HK", "HK"]
    assert result["industry"].tolist() == [None, None]


def test_map_hk_basic_to_stock_basic_df_missing_columns():
    hk_df = pd.DataFrame({"ts_code": ["00700.HK"]})
    result = map_hk_basic_to_stock_basic_df(hk_df)
    assert len(result) == 1
    assert pd.isna(result["name"].iloc[0])
    assert pd.isna(result["list_date"].iloc[0])