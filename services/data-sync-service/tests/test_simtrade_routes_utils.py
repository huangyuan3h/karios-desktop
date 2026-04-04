import pytest
from datetime import date
from data_sync_service.api.simtrade_routes import (
    _parse_date,
    _numeric,
)


def test_parse_date_valid():
    assert _parse_date("2024-01-15") == date(2024, 1, 15)
    assert _parse_date("2023-12-31") == date(2023, 12, 31)


def test_parse_date_empty():
    assert _parse_date("") is None
    assert _parse_date(None) is None


def test_parse_date_short():
    assert _parse_date("2024") is None
    assert _parse_date("2024-01") is None


def test_parse_date_invalid_format():
    assert _parse_date("2024/01/15") is None
    assert _parse_date("01-15-2024") is None


def test_parse_date_invalid_values():
    assert _parse_date("2024-13-01") is None
    assert _parse_date("2024-01-32") is None


def test_numeric_none():
    assert _numeric(None) is None


def test_numeric_valid():
    assert _numeric(3.14) == 3.14
    assert _numeric(10) == 10.0
    assert _numeric("2.5") == 2.5


def test_numeric_invalid():
    assert _numeric("invalid") is None
    assert _numeric([]) is None


def test_numeric_zero():
    assert _numeric(0) == 0.0
    assert _numeric(0.0) == 0.0