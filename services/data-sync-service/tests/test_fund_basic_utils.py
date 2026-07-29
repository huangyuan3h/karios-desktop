"""Tests for ETF fund_basic sync service."""

import pandas as pd

from data_sync_service.service.fund_basic import (
    map_etf_basic_to_stock_basic_df,
)


def _sample_etf_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": ["510300.SH", "159819.SZ", "513050.SH", "delisted.SH"],
            "name": ["沪深300 ETF", "人工智能 ETF", "纳指 ETF", "已退市 ETF"],
            "fund_type": ["股票型", "股票型", "股票型", "债券型"],
            "list_date": ["20120528", "20200708", "20210608", "20200101"],
            "delist_date": [None, None, None, "20230101"],
        }
    )


def test_map_etf_basic_assigns_etf_market():
    out = map_etf_basic_to_stock_basic_df(_sample_etf_df())
    assert (out["market"] == "ETF").all()


def test_map_etf_basic_extracts_symbol():
    out = map_etf_basic_to_stock_basic_df(_sample_etf_df())
    assert out.loc[out["ts_code"] == "510300.SH", "symbol"].iloc[0] == "510300"
    assert out.loc[out["ts_code"] == "159819.SZ", "symbol"].iloc[0] == "159819"


def test_map_etf_basic_normalizes_dates():
    out = map_etf_basic_to_stock_basic_df(_sample_etf_df())
    row = out.loc[out["ts_code"] == "510300.SH"].iloc[0]
    assert row["list_date"] == "2012-05-28"
    assert pd.isna(row["delist_date"])


def test_map_etf_basic_keeps_delist_date_when_present():
    out = map_etf_basic_to_stock_basic_df(_sample_etf_df())
    row = out.loc[out["ts_code"] == "delisted.SH"].iloc[0]
    assert row["delist_date"] == "2023-01-01"


def test_map_etf_basic_sets_industry_from_fund_type():
    out = map_etf_basic_to_stock_basic_df(_sample_etf_df())
    assert (out["industry"] == "股票型").sum() == 3
    assert (out["industry"] == "债券型").sum() == 1


def test_map_etf_basic_empty_input_returns_empty_schema():
    out = map_etf_basic_to_stock_basic_df(pd.DataFrame())
    assert list(out.columns) == [
        "ts_code",
        "symbol",
        "name",
        "industry",
        "market",
        "list_date",
        "delist_date",
    ]
    assert len(out) == 0


def test_map_etf_basic_none_input_returns_empty_schema():
    out = map_etf_basic_to_stock_basic_df(None)  # type: ignore[arg-type]
    assert len(out) == 0