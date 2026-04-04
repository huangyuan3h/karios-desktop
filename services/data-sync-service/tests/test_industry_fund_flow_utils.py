import pytest
from data_sync_service.service.industry_fund_flow import (
    _parse_money_to_cny,
    _stable_industry_code,
)


def test_parse_money_to_cny_none():
    assert _parse_money_to_cny(None) == 0.0


def test_parse_money_to_cny_number():
    assert _parse_money_to_cny(100) == 100.0
    assert _parse_money_to_cny(3.14) == 3.14


def test_parse_money_to_cny_nan():
    import math
    assert _parse_money_to_cny(float("nan")) == 0.0


def test_parse_money_to_cny_empty_string():
    assert _parse_money_to_cny("") == 0.0
    assert _parse_money_to_cny("   ") == 0.0


def test_parse_money_to_cny_special_values():
    assert _parse_money_to_cny("-") == 0.0
    assert _parse_money_to_cny("—") == 0.0
    assert _parse_money_to_cny("N/A") == 0.0
    assert _parse_money_to_cny("None") == 0.0


def test_parse_money_to_cny_yi():
    assert _parse_money_to_cny("1.5亿") == 1.5e8
    assert _parse_money_to_cny("2亿") == 2e8


def test_parse_money_to_cny_wan():
    assert _parse_money_to_cny("1.5万") == 1.5e4
    assert _parse_money_to_cny("2万元") == 2e4


def test_parse_money_to_cny_with_comma():
    assert _parse_money_to_cny("1,000") == 1000.0


def test_parse_money_to_cny_negative():
    assert _parse_money_to_cny("-100") == -100.0


def test_stable_industry_code_basic():
    result = _stable_industry_code("电子")
    assert len(result) == 12
    assert result.isalnum()


def test_stable_industry_code_consistent():
    assert _stable_industry_code("计算机") == _stable_industry_code("计算机")


def test_stable_industry_code_different():
    assert _stable_industry_code("电子") != _stable_industry_code("计算机")


def test_stable_industry_code_empty():
    assert _stable_industry_code("") == ""
    assert _stable_industry_code(None) == ""
    assert _stable_industry_code("  ") == ""


def test_stable_industry_code_whitespace():
    assert _stable_industry_code(" 电子 ") == _stable_industry_code("电子")