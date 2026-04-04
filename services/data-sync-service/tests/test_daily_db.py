import pytest
import pandas as pd
from datetime import date
from data_sync_service.db.daily import (
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
    assert _numeric(10) == 10.0


def test_numeric_invalid():
    assert _numeric("invalid") is None


def test_scalar_none():
    assert _scalar(None) is None


def test_scalar_nan():
    assert _scalar(float("nan")) is None
    assert _scalar(pd.NA) is None


def test_scalar_valid():
    assert _scalar("hello") == "hello"
    assert _scalar("  hello  ") == "hello"


def test_scalar_empty():
    assert _scalar("") is None
    assert _scalar("   ") is None


def test_date_str_none():
    assert _date_str(None) is None


def test_date_str_nan():
    assert _date_str(float("nan")) is None
    assert _date_str(pd.NA) is None


def test_date_str_date_object():
    result = _date_str(date(2024, 1, 15))
    assert result == "2024-01-15"


def test_date_str_yyyymmdd():
    assert _date_str("20240115") == "2024-01-15"


def test_date_str_iso():
    assert _date_str("2024-01-15") == "2024-01-15"


def test_date_str_empty():
    assert _date_str("") is None
    assert _date_str("   ") is None