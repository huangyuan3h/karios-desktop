import pytest
import pandas as pd
from datetime import date
from data_sync_service.db.stock_basic import (
    _scalar,
    _date,
)


def test_scalar_none():
    assert _scalar(None) is None


def test_scalar_nan():
    assert _scalar(float("nan")) is None


def test_scalar_empty():
    assert _scalar("") is None
    assert _scalar("   ") is None


def test_scalar_valid():
    assert _scalar("hello") == "hello"
    assert _scalar("  hello  ") == "hello"
    assert _scalar(123) == "123"


def test_date_none():
    assert _date(None) is None


def test_date_nan():
    assert _date(float("nan")) is None


def test_date_date_object():
    result = _date(date(2024, 1, 15))
    assert result == "2024-01-15"


def test_date_string():
    assert _date("2024-01-15") == "2024-01-15"


def test_date_empty():
    assert _date("") is None
    assert _date("   ") is None