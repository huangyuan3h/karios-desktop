import pytest
from datetime import date
from data_sync_service.service.close_sync import (
    _parse_yyyymmdd,
    _to_yyyymmdd,
)


def test_parse_yyyymmdd_valid():
    assert _parse_yyyymmdd("20240115") == date(2024, 1, 15)
    assert _parse_yyyymmdd("20231231") == date(2023, 12, 31)


def test_to_yyyymmdd():
    assert _to_yyyymmdd(date(2024, 1, 15)) == "20240115"
    assert _to_yyyymmdd(date(2023, 12, 31)) == "20231231"


def test_roundtrip():
    original = "20240115"
    parsed = _parse_yyyymmdd(original)
    result = _to_yyyymmdd(parsed)
    assert result == original