import pytest
from datetime import date
from data_sync_service.service.daily import (
    _today_yyyymmdd,
    _date_to_yyyymmdd,
)


def test_today_yyyymmdd_format():
    result = _today_yyyymmdd()
    assert len(result) == 8
    assert result.isdigit()


def test_date_to_yyyymmdd():
    assert _date_to_yyyymmdd(date(2024, 1, 15)) == "20240115"
    assert _date_to_yyyymmdd(date(2023, 12, 31)) == "20231231"


def test_date_to_yyyymmdd_padding():
    assert _date_to_yyyymmdd(date(2024, 1, 5)) == "20240105"