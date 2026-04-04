import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
from data_sync_service.service.trade_calendar import _today_yyyymmdd


def test_today_yyyymmdd_format():
    result = _today_yyyymmdd()
    assert len(result) == 8
    assert result.isdigit()