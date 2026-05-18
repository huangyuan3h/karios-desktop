import pytest
from datetime import datetime, UTC
from data_sync_service.service.tv import (
    _now_iso,
    _parse_iso_datetime,
    _tv_local_date_and_slot,
)


def test_now_iso_format():
    result = _now_iso()
    assert "T" in result
    assert result.endswith("+00:00") or "+" in result or "Z" in result


def test_parse_iso_datetime_valid():
    dt = _parse_iso_datetime("2024-01-15T10:30:00+00:00")
    assert dt is not None
    assert dt.year == 2024
    assert dt.month == 1
    assert dt.day == 15


def test_parse_iso_datetime_with_z():
    dt = _parse_iso_datetime("2024-01-15T10:30:00Z")
    assert dt is not None
    assert dt.year == 2024


def test_parse_iso_datetime_empty():
    assert _parse_iso_datetime("") is None
    assert _parse_iso_datetime(None) is None


def test_parse_iso_datetime_invalid():
    assert _parse_iso_datetime("invalid") is None
    assert _parse_iso_datetime("not-a-date") is None


def test_tv_local_date_and_slot_valid():
    date_str, slot = _tv_local_date_and_slot("2024-01-15T08:30:00+00:00")
    assert date_str == "2024-01-15"
    assert slot == "pm"  # 08:30 UTC = 16:30 Shanghai (afternoon)


def test_tv_local_date_and_slot_am():
    date_str, slot = _tv_local_date_and_slot("2024-01-15T02:30:00+00:00")
    assert date_str == "2024-01-15"
    assert slot == "am"  # 02:30 UTC = 10:30 Shanghai (morning)


def test_tv_local_date_and_slot_invalid():
    date_str, slot = _tv_local_date_and_slot("")
    assert slot == "unknown"


def test_tv_local_date_and_slot_invalid_datetime():
    date_str, slot = _tv_local_date_and_slot("invalid")
    assert slot == "unknown"