import pytest
import math
from data_sync_service.service.market_sentiment import (
    now_iso,
    _parse_money_to_cny,
    _finite_float,
    _try_float,
    _realtime_pct_chg,
    _safe_trade_date,
    _limit_pct_for,
)
from datetime import date


def test_now_iso_format():
    result = now_iso()
    assert "T" in result


def test_parse_money_to_cny_none():
    assert _parse_money_to_cny(None) == 0.0


def test_parse_money_to_cny_number():
    assert _parse_money_to_cny(1000) == 1000.0
    assert _parse_money_to_cny(3.14) == 3.14


def test_parse_money_to_cny_nan():
    assert _parse_money_to_cny(math.nan) == 0.0


def test_parse_money_to_cny_inf():
    assert _parse_money_to_cny(math.inf) == 0.0


def test_parse_money_to_cny_string():
    assert _parse_money_to_cny("1000") == 1000.0
    assert _parse_money_to_cny("1,000") == 1000.0


def test_parse_money_to_cny_empty():
    assert _parse_money_to_cny("") == 0.0
    assert _parse_money_to_cny("-") == 0.0
    assert _parse_money_to_cny("N/A") == 0.0


def test_parse_money_to_cny_yi():
    assert _parse_money_to_cny("1亿") == pytest.approx(1e8)
    assert _parse_money_to_cny("2.5亿") == pytest.approx(2.5e8)


def test_parse_money_to_cny_wan():
    assert _parse_money_to_cny("1万") == pytest.approx(1e4)
    assert _parse_money_to_cny("2.5万") == pytest.approx(2.5e4)
    assert _parse_money_to_cny("100万元") == pytest.approx(1e6)


def test_parse_money_to_cny_complex():
    assert _parse_money_to_cny("1,000万") == pytest.approx(1e7)
    assert _parse_money_to_cny("  1.5亿  ") == pytest.approx(1.5e8)


def test_finite_float_valid():
    assert _finite_float(3.14) == 3.14
    assert _finite_float("2.5") == 2.5


def test_finite_float_nan():
    assert _finite_float(math.nan) == 0.0


def test_finite_float_inf():
    assert _finite_float(math.inf) == 0.0


def test_finite_float_invalid():
    assert _finite_float("invalid") == 0.0
    assert _finite_float(None) == 0.0


def test_finite_float_custom_default():
    assert _finite_float(None, default=-1.0) == -1.0


def test_try_float_valid():
    assert _try_float(3.14) == 3.14
    assert _try_float("2.5") == 2.5


def test_try_float_nan():
    assert _try_float(math.nan) is None


def test_try_float_inf():
    assert _try_float(math.inf) is None


def test_try_float_invalid():
    assert _try_float("invalid") is None
    assert _try_float(None) is None


def test_realtime_pct_chg_direct():
    item = {"pct_chg": 5.0}
    assert _realtime_pct_chg(item) == 5.0


def test_realtime_pct_chg_derived():
    item = {"price": 11.0, "pre_close": 10.0}
    assert _realtime_pct_chg(item) == pytest.approx(10.0)


def test_realtime_pct_chg_missing():
    item = {"price": 10.0}
    assert _realtime_pct_chg(item) is None


def test_realtime_pct_chg_zero_pre_close():
    item = {"price": 10.0, "pre_close": 0.0}
    assert _realtime_pct_chg(item) is None


def test_safe_trade_date():
    assert _safe_trade_date(date(2024, 1, 15)) == "20240115"
    assert _safe_trade_date(date(2023, 12, 31)) == "20231231"


def test_limit_pct_for_st():
    assert _limit_pct_for("000001.SZ", "ST某某") == 5.0
    assert _limit_pct_for("000001.SZ", "*ST某某") == 5.0


def test_limit_pct_for_bj():
    assert _limit_pct_for("430001.BJ", "某某股") == 30.0


def test_limit_pct_for_gem():
    assert _limit_pct_for("300001.SZ", "某某股") == 20.0
    assert _limit_pct_for("301001.SZ", "某某股") == 20.0


def test_limit_pct_for_star():
    assert _limit_pct_for("688001.SH", "某某股") == 20.0


def test_limit_pct_for_main():
    assert _limit_pct_for("000001.SZ", "某某股") == 10.0
    assert _limit_pct_for("600001.SH", "某某股") == 10.0