import pytest
import pandas as pd
from datetime import date
from data_sync_service.db.trade_calendar import (
    _date_str,
)


def test_date_str_none():
    assert _date_str(None) is None


def test_date_str_nan():
    assert _date_str(float("nan")) is None


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


def test_date_str_invalid():
    assert _date_str("invalid") == "invalid"