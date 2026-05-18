import pytest
from data_sync_service.service.mainline import (
    _safe_float,
    _limit_pct_for,
    _is_limit_up,
)


def test_safe_float_valid():
    assert _safe_float(3.14) == 3.14
    assert _safe_float("2.5") == 2.5
    assert _safe_float(10) == 10.0


def test_safe_float_invalid():
    assert _safe_float(None) == 0.0
    assert _safe_float("invalid") == 0.0
    assert _safe_float(float("nan")) == 0.0
    assert _safe_float(float("inf")) == 0.0


def test_safe_float_custom_default():
    assert _safe_float(None, default=-1.0) == -1.0
    assert _safe_float("invalid", default=99.0) == 99.0


def test_limit_pct_st_stock():
    assert _limit_pct_for("000001.SZ", "ST某某") == 5.0
    assert _limit_pct_for("000002.SZ", "*ST某某") == 5.0


def test_limit_pct_bj_stock():
    assert _limit_pct_for("430001.BJ", "北交所股票") == 30.0


def test_limit_pct_gem_stock():
    assert _limit_pct_for("300001.SZ", "创业板") == 20.0
    assert _limit_pct_for("301001.SZ", "创业板") == 20.0


def test_limit_pct_star_stock():
    assert _limit_pct_for("688001.SH", "科创板") == 20.0


def test_limit_pct_normal_stock():
    assert _limit_pct_for("000001.SZ", "普通股") == 10.0
    assert _limit_pct_for("600001.SH", "普通股") == 10.0


def test_is_limit_up_false_no_data():
    assert not _is_limit_up(ts_code="000001.SZ", pre_close=None, close=10.0, pct_chg=10.0, name="测试")
    assert not _is_limit_up(ts_code="000001.SZ", pre_close=10.0, close=None, pct_chg=10.0, name="测试")


def test_is_limit_up_normal_stock():
    pre_close = 10.0
    limit_price = pre_close * 1.10
    assert _is_limit_up(ts_code="000001.SZ", pre_close=pre_close, close=limit_price, pct_chg=9.9, name="测试")


def test_is_limit_up_gem_stock():
    pre_close = 50.0
    limit_price = pre_close * 1.20
    assert _is_limit_up(ts_code="300001.SZ", pre_close=pre_close, close=limit_price, pct_chg=19.9, name="创业板")


def test_is_limit_up_st_stock():
    pre_close = 5.0
    limit_price = pre_close * 1.05
    assert _is_limit_up(ts_code="000001.SZ", pre_close=pre_close, close=limit_price, pct_chg=4.9, name="ST测试")


def test_is_limit_up_not_limit():
    assert not _is_limit_up(ts_code="000001.SZ", pre_close=10.0, close=10.5, pct_chg=5.0, name="测试")