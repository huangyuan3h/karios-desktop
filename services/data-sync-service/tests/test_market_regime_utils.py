import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
from data_sync_service.service.market_regime import (
    _ema,
    _macd_histogram,
    _today_iso_date,
    _is_shanghai_trading_time_at,
    _is_shanghai_sync_window_at,
    _trade_date_from_trade_time,
)


def test_ema_basic():
    values = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    result = _ema(values, 3)
    assert len(result) > 0


def test_ema_insufficient_data():
    assert _ema([1.0, 2.0], 5) == []


def test_ema_empty():
    assert _ema([], 5) == []


def test_macd_histogram_basic():
    closes = [100.0] * 20 + [110.0] * 20
    result = _macd_histogram(closes)
    assert isinstance(result, list)


def test_macd_histogram_insufficient_data():
    assert _macd_histogram([1.0, 2.0, 3.0]) == []


def test_today_iso_date_format():
    result = _today_iso_date()
    assert len(result) == 10
    assert "-" in result


def test_is_shanghai_trading_time_morning():
    dt = datetime(2024, 1, 15, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is True


def test_is_shanghai_trading_time_afternoon():
    dt = datetime(2024, 1, 15, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is True


def test_is_shanghai_trading_time_lunch():
    dt = datetime(2024, 1, 15, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_trading_time_weekend():
    dt = datetime(2024, 1, 13, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_trading_time_before_open():
    dt = datetime(2024, 1, 15, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_trading_time_after_close():
    dt = datetime(2024, 1, 15, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_sync_window_lunch():
    dt = datetime(2024, 1, 15, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_sync_window_at(dt) is True


def test_is_shanghai_sync_window_trading():
    dt = datetime(2024, 1, 15, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_sync_window_at(dt) is True


def test_is_shanghai_sync_window_weekend():
    dt = datetime(2024, 1, 13, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_sync_window_at(dt) is False


def test_trade_date_from_trade_time_iso():
    assert _trade_date_from_trade_time("2024-01-15 10:30:00") == "2024-01-15"
    assert _trade_date_from_trade_time("2024-01-15") == "2024-01-15"


def test_trade_date_from_trade_time_yyyymmdd():
    assert _trade_date_from_trade_time("20240115") == "2024-01-15"


def test_trade_date_from_trade_time_none():
    assert _trade_date_from_trade_time(None) is None
    assert _trade_date_from_trade_time("") is None
    assert _trade_date_from_trade_time("   ") is None


def test_trade_date_from_trade_time_invalid():
    assert _trade_date_from_trade_time("invalid") is None