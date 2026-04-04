import pytest
from data_sync_service.service.watchlist_v5_alerts import (
    _safe_float,
    _clamp,
    _next_tranche,
    _regime_target,
)


def test_safe_float_valid():
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5
    assert _safe_float(10) == 10.0


def test_safe_float_invalid():
    assert _safe_float(None) is None
    assert _safe_float("invalid") is None


def test_safe_float_nan():
    import math
    assert _safe_float(float("nan")) is None


def test_clamp_within_range():
    assert _clamp(5.0, 0.0, 10.0) == 5.0


def test_clamp_below():
    assert _clamp(-1.0, 0.0, 10.0) == 0.0


def test_clamp_above():
    assert _clamp(15.0, 0.0, 10.0) == 10.0


def test_clamp_edge():
    assert _clamp(0.0, 0.0, 10.0) == 0.0
    assert _clamp(10.0, 0.0, 10.0) == 10.0


def test_next_tranche_first():
    assert _next_tranche(0.0, 0.9) == 0.3


def test_next_tranche_second():
    assert _next_tranche(0.3, 0.9) == 0.6


def test_next_tranche_third():
    assert _next_tranche(0.6, 0.9) == 0.9


def test_next_tranche_full():
    assert _next_tranche(0.9, 0.9) == 0.9


def test_next_tranche_zero_target():
    assert _next_tranche(0.0, 0.0) == 0.0


def test_regime_target_strong():
    assert _regime_target("Strong") == 1.0


def test_regime_target_diverging():
    assert _regime_target("Diverging") == 0.66


def test_regime_target_weak():
    assert _regime_target("Weak") == 0.3


def test_regime_target_unknown():
    assert _regime_target("Unknown") == 0.0
    assert _regime_target("") == 0.0