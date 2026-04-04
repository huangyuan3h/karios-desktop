import pytest
from data_sync_service.service.trendok import (
    _ema,
    _rsi,
    _macd,
    _atr14,
    _parse_float_safe,
)


def test_ema_basic():
    values = [10.0, 11.0, 12.0, 11.5, 11.0]
    result = _ema(values, 3)
    assert len(result) == len(values)
    assert result[0] == 10.0


def test_ema_empty():
    assert _ema([], 5) == []
    assert _ema([1.0, 2.0], 0) == []


def test_rsi_basic():
    values = [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 109.0]
    result = _rsi(values, 5)
    assert len(result) == len(values)
    assert all(0 <= r <= 100 for r in result)


def test_rsi_empty():
    assert _rsi([], 14) == []
    assert _rsi([1.0], 14) == []


def test_rsi_all_gains():
    values = [100.0, 101.0, 102.0, 103.0, 104.0]
    result = _rsi(values, 3)
    assert result[-1] > 50


def test_rsi_all_losses():
    values = [100.0, 99.0, 98.0, 97.0, 96.0]
    result = _rsi(values, 3)
    assert result[-1] < 50


def test_macd_basic():
    values = [100.0] * 30 + [110.0] * 10
    macd_line, signal_line, hist = _macd(values)
    assert len(macd_line) == len(values)
    assert len(signal_line) == len(values)
    assert len(hist) == len(values)


def test_macd_empty():
    macd_line, signal_line, hist = _macd([])
    assert macd_line == []
    assert signal_line == []
    assert hist == []


def test_atr14_basic():
    highs = [105.0, 106.0, 107.0, 106.0, 108.0, 109.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0, 117.0, 118.0]
    lows = [100.0, 101.0, 102.0, 101.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0, 113.0]
    closes = [103.0, 104.0, 105.0, 104.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 116.0]
    result = _atr14(highs, lows, closes, 14)
    assert result is not None
    assert result > 0


def test_atr14_insufficient_data():
    highs = [105.0, 106.0]
    lows = [100.0, 101.0]
    closes = [103.0, 104.0]
    assert _atr14(highs, lows, closes, 14) is None


def test_atr14_zero_period():
    assert _atr14([1.0], [1.0], [1.0], 0) is None


def test_parse_float_safe_valid():
    assert _parse_float_safe(3.14) == 3.14
    assert _parse_float_safe("2.5") == 2.5
    assert _parse_float_safe(10) == 10.0


def test_parse_float_safe_invalid():
    assert _parse_float_safe(None) is None
    assert _parse_float_safe("invalid") is None
    assert _parse_float_safe(float("nan")) is None
    assert _parse_float_safe(float("inf")) is None