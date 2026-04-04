import pytest
from datetime import datetime, timezone
from data_sync_service.testback.db import _now_utc


def test_now_utc():
    result = _now_utc()
    assert isinstance(result, datetime)
    assert result.tzinfo is not None