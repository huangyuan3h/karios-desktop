import pytest
import math
from data_sync_service.service.trendok import (
    _ema,
    _rsi,
    _macd,
    _atr14,
    _parse_float_safe,
)


def test_ema_empty():
    assert _ema([], 10) == []


def test_ema_invalid_period():
    assert _ema([1, 2, 3], 0) == []


def test_ema_single():
    assert _ema([10.0], 5) == [10.0]


def test_ema_basic():
    values = [10.0, 11.0, 12.0]
    result = _ema(values, 3)
    assert len(result) == 3
    assert result[0] == 10.0


def test_rsi_empty():
    assert _rsi([], 14) == []


def test_rsi_invalid_period():
    assert _rsi([1, 2], 0) == []


def test_rsi_single():
    assert _rsi([10.0], 14) == []


def test_rsi_basic():
    values = [100.0, 101.0, 102.0, 101.0, 103.0]
    result = _rsi(values, 3)
    assert len(result) == len(values)


def test_rsi_all_up():
    values = [10.0, 11.0, 12.0, 13.0, 14.0]
    result = _rsi(values, 3)
    assert result[-1] > 50


def test_rsi_all_down():
    values = [14.0, 13.0, 12.0, 11.0, 10.0]
    result = _rsi(values, 3)
    assert result[-1] < 50


def test_macd_empty():
    assert _macd([]) == ([], [], [])


def test_macd_single():
    macd, signal, hist = _macd([10.0])
    assert len(macd) == 1
    assert len(signal) == 1
    assert len(hist) == 1


def test_macd_basic():
    values = [10.0, 11.0, 12.0, 13.0, 14.0]
    macd, signal, hist = _macd(values)
    assert len(macd) == len(values)
    assert len(signal) == len(values)
    assert len(hist) == len(values)


def test_atr14_empty():
    assert _atr14([], [], [], 14) is None


def test_atr14_invalid_period():
    assert _atr14([10, 11], [9, 10], [10, 10], 0) is None


def test_atr14_short_data():
    assert _atr14([10], [9], [10], 14) is None


def test_atr14_basic():
    highs = [11.0, 12.0, 13.0, 14.0, 15.0]
    lows = [9.0, 10.0, 11.0, 12.0, 13.0]
    closes = [10.0, 11.0, 12.0, 13.0, 14.0]
    result = _atr14(highs, lows, closes, 3)
    assert result is not None
    assert result > 0


def test_parse_float_safe_none():
    assert _parse_float_safe(None) is None


def test_parse_float_safe_valid():
    assert _parse_float_safe(3.14) == 3.14
    assert _parse_float_safe("2.5") == 2.5


def test_parse_float_safe_nan():
    assert _parse_float_safe(math.nan) is None


def test_parse_float_safe_inf():
    assert _parse_float_safe(math.inf) is None
    assert _parse_float_safe(-math.inf) is None


def test_parse_float_safe_invalid():
    assert _parse_float_safe("invalid") is None