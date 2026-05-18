import pytest
import pandas as pd
from datetime import date
from data_sync_service.db.index_daily import (
    _numeric,
    _scalar,
    _date_str,
)


def test_numeric_none():
    assert _numeric(None) is None


def test_numeric_nan():
    assert _numeric(float("nan")) is None
    assert _numeric(pd.NA) is None


def test_numeric_valid():
    assert _numeric(3.14) == 3.14
    assert _numeric("2.5") == 2.5


def test_scalar_none():
    assert _scalar(None) is None


def test_scalar_valid():
    assert _scalar("hello") == "hello"


def test_date_str_date_object():
    result = _date_str(date(2024, 1, 15))
    assert result == "2024-01-15"


def test_date_str_yyyymmdd():
    assert _date_str("20240115") == "2024-01-15"