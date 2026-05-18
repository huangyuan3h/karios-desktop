import pytest
from data_sync_service.service.dashboard import (
    _now_iso,
    _today_iso_date,
)


def test_now_iso_format():
    result = _now_iso()
    assert "T" in result


def test_today_iso_date_format():
    result = _today_iso_date()
    assert len(result) == 10
    assert result[4] == "-"
    assert result[7] == "-"