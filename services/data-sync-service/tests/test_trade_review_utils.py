import pytest
from datetime import date
from data_sync_service.db.trade_review import (
    _to_float,
    _to_int,
    _to_bool,
    _to_date,
    _to_json_obj,
)


def test_to_float_none():
    assert _to_float(None) is None


def test_to_float_valid():
    assert _to_float(3.14) == 3.14
    assert _to_float("2.5") == 2.5
    assert _to_float(10) == 10.0


def test_to_float_invalid():
    assert _to_float("invalid") is None
    assert _to_float([]) is None


def test_to_int_none():
    assert _to_int(None) is None


def test_to_int_valid():
    assert _to_int(42) == 42
    assert _to_int("100") == 100
    assert _to_int(3.9) == 3


def test_to_int_invalid():
    assert _to_int("invalid") is None
    assert _to_int([]) is None


def test_to_bool_basic():
    assert _to_bool(True) is True
    assert _to_bool(False) is False
    assert _to_bool(1) is True
    assert _to_bool(0) is False
    assert _to_bool("yes") is True
    assert _to_bool("") is False


def test_to_date_none():
    assert _to_date(None) is None


def test_to_date_from_date_object():
    d = date(2024, 1, 15)
    assert _to_date(d) == "2024-01-15"


def test_to_date_from_string():
    assert _to_date("2024-01-15") == "2024-01-15"
    assert _to_date("  2024-01-15  ") == "2024-01-15"


def test_to_date_empty_string():
    assert _to_date("") is None
    assert _to_date("   ") is None


def test_to_json_obj_dict():
    assert _to_json_obj({"key": "value"}) == {"key": "value"}


def test_to_json_obj_none():
    assert _to_json_obj(None) == {}


def test_to_json_obj_json_string():
    assert _to_json_obj('{"a": 1}') == {"a": 1}


def test_to_json_obj_invalid_json():
    assert _to_json_obj("not json") == {}
    assert _to_json_obj(123) == {}


def test_to_json_obj_non_dict_json():
    assert _to_json_obj('[1, 2, 3]') == {}