import pytest
from data_sync_service.service.watchlist_momentum_alerts import (
    _safe_float,
    _regime_target,
    _next_tranche,
    _quote_trade_date,
)


def test_safe_float_none():
    assert _safe_float(None) is None


def test_safe_float_valid():
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5
    assert _safe_float(10) == 10.0


def test_safe_float_nan():
    import math
    assert _safe_float(math.nan) is None


def test_safe_float_invalid():
    assert _safe_float("invalid") is None


def test_regime_target_strong():
    assert _regime_target("Strong") == 0.25


def test_regime_target_diverging():
    assert _regime_target("Diverging") == 0.15


def test_regime_target_weak():
    assert _regime_target("Weak") == 0.05
    assert _regime_target("Unknown") == 0.05
    assert _regime_target("") == 0.05


def test_next_tranche_zero_target():
    assert _next_tranche(0.0, 0.0) == 0.0
    assert _next_tranche(0.1, 0.0) == 0.0


def test_next_tranche_first_step():
    result = _next_tranche(0.0, 0.15)
    assert result == pytest.approx(0.05)


def test_next_tranche_second_step():
    result = _next_tranche(0.04, 0.15)
    assert result == pytest.approx(0.05)
    result = _next_tranche(0.06, 0.15)
    assert result == pytest.approx(0.10)


def test_next_tranche_full():
    result = _next_tranche(0.10, 0.15)
    assert result == pytest.approx(0.15)


def test_quote_trade_date_iso():
    q = {"trade_time": "2024-01-15T10:30:00"}
    assert _quote_trade_date(q) == "2024-01-15"


def test_quote_trade_date_yyyymmdd():
    q = {"trade_time": "20240115103000"}
    assert _quote_trade_date(q) == "2024-01-15"


def test_quote_trade_date_empty():
    q = {"trade_time": ""}
    assert _quote_trade_date(q) is None
    q = {}
    assert _quote_trade_date(q) is None


def test_quote_trade_date_invalid():
    q = {"trade_time": "invalid"}
    assert _quote_trade_date(q) is None