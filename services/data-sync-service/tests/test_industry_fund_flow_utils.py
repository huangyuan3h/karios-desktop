from data_sync_service.db.industry_fund_flow import _sw_l1_industry_names_array
from data_sync_service.service.industry_fund_flow import (
    _parse_money_to_cny,
    _stable_industry_code,
)
from data_sync_service.service.industry_taxonomy import (
    SW_L1_INDUSTRIES,
    SW_L1_INDUSTRY_NAMES,
    classify_sw_l1_industry,
    is_sw_l1_industry_name,
)


def test_parse_money_to_cny_none():
    assert _parse_money_to_cny(None) == 0.0


def test_parse_money_to_cny_number():
    assert _parse_money_to_cny(100) == 100.0
    assert _parse_money_to_cny(3.14) == 3.14


def test_parse_money_to_cny_nan():
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


def test_sw_l1_taxonomy_contains_expected_universe_size():
    assert len(SW_L1_INDUSTRIES) == 31
    assert is_sw_l1_industry_name("非银金融") is True
    assert is_sw_l1_industry_name("有色金属") is True


def test_sw_l1_sql_array_param_is_list():
    names = _sw_l1_industry_names_array()
    assert isinstance(SW_L1_INDUSTRY_NAMES, tuple)
    assert isinstance(names, list)
    assert names == list(SW_L1_INDUSTRY_NAMES)


def test_sw_l1_taxonomy_rejects_child_industries():
    assert is_sw_l1_industry_name("证券Ⅱ") is False
    assert is_sw_l1_industry_name("证券Ⅲ") is False


def test_classify_sw_l1_industry_metadata():
    allowed = classify_sw_l1_industry(" 非银金融 ")
    assert allowed["is_allowed"] is True
    assert allowed["taxonomy"] == "SW"
    assert allowed["industry_level"] == 1
    assert allowed["industry_name"] == "非银金融"

    rejected = classify_sw_l1_industry("证券Ⅱ")
    assert rejected["is_allowed"] is False
    assert rejected["taxonomy"] == "UNKNOWN"
    assert rejected["industry_level"] is None