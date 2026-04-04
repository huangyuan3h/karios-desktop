import pytest
from data_sync_service.testback.engine import (
    BacktestParams,
    UniverseFilter,
    DailyRuleFilter,
    _safe_float,
    _normalize_cash,
    _parse_date,
    _format_date,
    _warmup_start_date,
    _adjust_factor_ratio,
)


def test_safe_float_none():
    assert _safe_float(None) == 0.0


def test_safe_float_valid():
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5


def test_safe_float_invalid():
    try:
        result = _safe_float("invalid")
        assert result == 0.0
    except (ValueError, TypeError):
        pass


def test_safe_float_custom_default():
    assert _safe_float(None, default=-1.0) == -1.0


def test_normalize_cash_zero():
    assert _normalize_cash(0.0) == 0.0
    assert _normalize_cash(1e-9) == 0.0


def test_normalize_cash_nonzero():
    assert _normalize_cash(100.0) == 100.0
    assert _normalize_cash(-1e-7) == -1e-7
    assert _normalize_cash(-1e-9) == 0.0


def test_parse_date_valid():
    result = _parse_date("2024-01-15")
    assert result is not None
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15


def test_parse_date_invalid():
    assert _parse_date("invalid") is None
    assert _parse_date("2024/01/15") is None


def test_format_date():
    from datetime import datetime
    dt = datetime(2024, 1, 15)
    assert _format_date(dt) == "2024-01-15"


def test_warmup_start_date():
    result = _warmup_start_date("2024-01-15", 10)
    assert result < "2024-01-15"


def test_warmup_start_date_invalid():
    assert _warmup_start_date("invalid", 10) == "invalid"


def test_warmup_start_date_zero():
    assert _warmup_start_date("2024-01-15", 0) == "2024-01-15"


def test_adjust_factor_ratio_hfq():
    assert _adjust_factor_ratio([], "hfq") == 1.0


def test_adjust_factor_ratio_empty():
    assert _adjust_factor_ratio([], "qfq") == 1.0


def test_adjust_factor_ratio_with_data():
    rows = [{"adj_factor": 1.5}, {"adj_factor": 1.0}]
    result = _adjust_factor_ratio(rows, "qfq")
    assert result > 0


def test_backtest_params_defaults():
    params = BacktestParams(start_date="2024-01-01", end_date="2024-12-31")
    assert params.initial_cash == 1.0
    assert params.fee_rate == 0.0
    assert params.slippage_rate == 0.0
    assert params.adj_mode == "qfq"
    assert params.warmup_days == 20


def test_universe_filter_defaults():
    f = UniverseFilter()
    assert f.market == "CN"
    assert f.exclude_keywords is None
    assert f.min_list_days == 0


def test_daily_rule_filter_defaults():
    f = DailyRuleFilter()
    assert f.min_price is None
    assert f.max_price is None
    assert f.min_volume is None