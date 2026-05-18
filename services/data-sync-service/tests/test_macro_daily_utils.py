import pytest
import pandas as pd
from datetime import date
from data_sync_service.service.macro_daily import (
    _today_yyyymmdd,
    _date_to_yyyymmdd,
    _normalize_us_daily_df,
    _normalize_fx_daily_df,
)


def test_today_yyyymmdd_format():
    result = _today_yyyymmdd()
    assert len(result) == 8
    assert result.isdigit()


def test_date_to_yyyymmdd():
    assert _date_to_yyyymmdd(date(2024, 1, 15)) == "20240115"
    assert _date_to_yyyymmdd(date(2023, 12, 31)) == "20231231"


def test_normalize_us_daily_df_none():
    assert _normalize_us_daily_df(None) is None


def test_normalize_us_daily_df_empty():
    assert _normalize_us_daily_df(pd.DataFrame()).empty


def test_normalize_us_daily_df_with_pct_change():
    df = pd.DataFrame({"pct_change": [1.5, 2.0]})
    result = _normalize_us_daily_df(df)
    assert "pct_chg" in result.columns
    assert result["pct_chg"].tolist() == [1.5, 2.0]


def test_normalize_us_daily_df_already_has_pct_chg():
    df = pd.DataFrame({"pct_chg": [1.5, 2.0]})
    result = _normalize_us_daily_df(df)
    assert "pct_chg" in result.columns


def test_normalize_fx_daily_df_none():
    assert _normalize_fx_daily_df(None) is None


def test_normalize_fx_daily_df_empty():
    assert _normalize_fx_daily_df(pd.DataFrame()).empty


def test_normalize_fx_daily_df_with_bid_columns():
    df = pd.DataFrame({
        "bid_close": [100.0, 101.0],
        "bid_open": [99.0, 100.0],
        "bid_high": [102.0, 103.0],
        "bid_low": [98.0, 99.0],
    })
    result = _normalize_fx_daily_df(df)
    assert "close" in result.columns
    assert "open" in result.columns
    assert "high" in result.columns
    assert "low" in result.columns
    assert result["close"].tolist() == [100.0, 101.0]


def test_normalize_fx_daily_df_preserves_other_columns():
    df = pd.DataFrame({
        "bid_close": [100.0],
        "other_column": [42],
    })
    result = _normalize_fx_daily_df(df)
    assert "other_column" in result.columns
    assert result["other_column"].tolist() == [42]