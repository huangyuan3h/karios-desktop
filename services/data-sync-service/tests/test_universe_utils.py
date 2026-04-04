import pytest
from datetime import datetime
from data_sync_service.testback.universe import _parse_date


def test_parse_date_valid():
    result = _parse_date("2024-01-15")
    assert result is not None
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_parse_date_invalid():
    assert _parse_date("invalid") is None
    assert _parse_date("2024/01/15") is None


def test_parse_date_empty():
    assert _parse_date("") is None
    assert _parse_date(None) is None