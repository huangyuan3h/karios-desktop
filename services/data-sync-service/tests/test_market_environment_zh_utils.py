import pytest
from data_sync_service.service.market_environment_zh import (
    _finite_float,
    _fmt_pct_zh,
    _signal_to_zh,
    _format_macro_close,
)


def test_finite_float_valid():
    assert _finite_float(3.14) == 3.14
    assert _finite_float("2.5") == 2.5


def test_finite_float_invalid():
    assert _finite_float(None) is None
    assert _finite_float("invalid") is None
    import math
    assert _finite_float(float("nan")) is None
    assert _finite_float(float("inf")) is None


def test_fmt_pct_zh_positive():
    assert _fmt_pct_zh(1.5) == "涨1.50%"
    assert _fmt_pct_zh(0.0) == "涨0.00%"


def test_fmt_pct_zh_negative():
    assert _fmt_pct_zh(-1.5) == "跌1.50%"
    assert _fmt_pct_zh(-0.01) == "跌0.01%"


def test_signal_to_zh_red():
    assert _signal_to_zh("red") == "红灯"


def test_signal_to_zh_yellow():
    assert _signal_to_zh("yellow") == "黄灯"


def test_signal_to_zh_green():
    assert _signal_to_zh("green") == "绿灯"
    assert _signal_to_zh("light_green") == "绿灯"


def test_signal_to_zh_deep_green():
    assert _signal_to_zh("deep_green") == "深绿"


def test_signal_to_zh_unknown():
    assert _signal_to_zh("") == "未知"
    assert _signal_to_zh(None) == "未知"
    assert _signal_to_zh("unknown") == "未知"


def test_signal_to_zh_other():
    assert _signal_to_zh("custom") == "custom"


def test_format_macro_close_usdcnh():
    assert _format_macro_close("USDCNH.FXCM", 7.2500) == "7.25"
    assert _format_macro_close("USDCNH.FXCM", 7.1234) == "7.1234"


def test_format_macro_close_copper():
    assert _format_macro_close("COMM_COPPER", 75000.5) == "75000"


def test_format_macro_close_other():
    assert _format_macro_close("IXIC", 15000.25) == "15000.25"
    assert _format_macro_close("A50", 12000.0) == "12000.00"