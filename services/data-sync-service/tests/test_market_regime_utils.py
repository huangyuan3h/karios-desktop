import pytest
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from data_sync_service.service.market_regime import (
    _ema,
    _macd_histogram,
    _is_shanghai_trading_time_at,
    _is_shanghai_sync_window_at,
    _trade_date_from_trade_time,
    _safe_float,
    _realtime_pct_or_price,
)


def test_ema_empty():
    assert _ema([], 5) == []


def test_ema_short():
    assert _ema([1, 2, 3], 5) == []


def test_ema_basic():
    values = [10.0, 11.0, 12.0, 13.0, 14.0]
    result = _ema(values, 3)
    assert len(result) > 0


def test_macd_histogram_empty():
    assert _macd_histogram([]) == []


def test_macd_histogram_short():
    assert _macd_histogram([1, 2, 3]) == []


def test_macd_histogram_basic():
    closes = [10.0] * 40
    result = _macd_histogram(closes)
    assert len(result) >= 0


def test_is_shanghai_trading_time_weekday():
    dt = datetime(2024, 1, 15, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert dt.weekday() < 5
    assert _is_shanghai_trading_time_at(dt) is True


def test_is_shanghai_trading_time_weekend():
    dt = datetime(2024, 1, 20, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert dt.weekday() >= 5
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_trading_time_night():
    dt = datetime(2024, 1, 15, 20, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False


def test_is_shanghai_trading_time_morning():
    dt = datetime(2024, 1, 15, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is True


def test_is_shanghai_trading_time_afternoon():
    dt = datetime(2024, 1, 15, 14, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is True


def test_is_shanghai_sync_window_lunch():
    dt = datetime(2024, 1, 15, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert _is_shanghai_trading_time_at(dt) is False
    assert _is_shanghai_sync_window_at(dt) is True


def test_trade_date_from_iso():
    assert _trade_date_from_trade_time("2024-01-15T10:30:00") == "2024-01-15"


def test_trade_date_from_yyyymmdd():
    assert _trade_date_from_trade_time("20240115") == "2024-01-15"


def test_trade_date_from_none():
    assert _trade_date_from_trade_time(None) is None
    assert _trade_date_from_trade_time("") is None


def test_trade_date_invalid():
    assert _trade_date_from_trade_time("invalid") is None


def test_safe_float_valid():
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5


def test_safe_float_invalid():
    assert _safe_float("invalid") is None
    assert _safe_float(None) is None


def test_safe_float_nan():
    assert _safe_float(math.nan) is None


def test_safe_float_inf():
    assert _safe_float(math.inf) is None


def test_realtime_pct_or_price_direct():
    item = {"pct_chg": 5.0, "price": 10.0}
    pct, price = _realtime_pct_or_price(item)
    assert pct == 5.0
    assert price == 10.0


def test_realtime_pct_or_price_derived():
    item = {"price": 11.0, "pre_close": 10.0}
    pct, price = _realtime_pct_or_price(item)
    assert pct == pytest.approx(10.0)
    assert price == 11.0


def test_realtime_pct_or_price_missing():
    item = {"price": 10.0}
    pct, price = _realtime_pct_or_price(item)
    assert pct is None
    assert price == 10.0