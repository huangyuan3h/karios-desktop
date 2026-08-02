"""Tests for HK daily K-line sync via yfinance (fallback)."""

import pandas as pd

from data_sync_service.service.hk_daily_yf import (
    _date_to_yyyymmdd,
    _df_to_daily_rows,
    _ts_code_to_yf,
)


def test_ts_code_to_yf_pads_to_four_digits():
    # yfinance expects 4-digit padding. Our 5-digit tushare ts_codes
    # map to last 4 digits, zero-padded if necessary.
    assert _ts_code_to_yf("00700.HK") == "0700.HK"
    assert _ts_code_to_yf("01810.HK") == "1810.HK"
    assert _ts_code_to_yf("00005.HK") == "0005.HK"
    assert _ts_code_to_yf("09988.HK") == "9988.HK"


def test_ts_code_to_yf_never_strips_all_zeros():
    """Regression: the old `lstrip("0")` returned `1.HK` for `00001.HK`,
    which yfinance 404'd. We must always return a 4-digit symbol."""
    assert _ts_code_to_yf("00001.HK") == "0001.HK"


def test_ts_code_to_yf_rejects_non_hk():
    assert _ts_code_to_yf("510300.SH") is None
    assert _ts_code_to_yf("") is None
    assert _ts_code_to_yf(None) is None  # type: ignore[arg-type]


def test_df_to_daily_rows_empty():
    assert _df_to_daily_rows("00700.HK", pd.DataFrame()) == []
    assert _df_to_daily_rows("00700.HK", None) == []  # type: ignore[arg-type]


def test_df_to_daily_rows_normalizes_columns():
    idx = pd.to_datetime(["2026-07-28", "2026-07-29"])
    df = pd.DataFrame(
        {
            "Open": [475.0, 476.0],
            "High": [478.0, 480.0],
            "Low": [473.0, 474.0],
            "Close": [476.0, 478.0],
            "Volume": [15000000.0, 18000000.0],
        },
        index=idx,
    )
    rows = _df_to_daily_rows("00700.HK", df)
    assert len(rows) == 2
    assert rows[0]["ts_code"] == "00700.HK"
    assert rows[0]["trade_date"] == "2026-07-28"
    assert rows[0]["close"] == 476.0
    assert rows[0]["amount"] is None


def test_df_to_daily_rows_skips_rows_without_close():
    idx = pd.to_datetime(["2026-07-28"])
    df = pd.DataFrame(
        {
            "Open": [475.0],
            "High": [478.0],
            "Low": [473.0],
            "Close": [float("nan")],
            "Volume": [15000000.0],
        },
        index=idx,
    )
    assert _df_to_daily_rows("00700.HK", df) == []


def test_date_to_yyyymmdd_format():
    import datetime as _dt

    assert _date_to_yyyymmdd(_dt.date(2026, 7, 29)) == "20260729"