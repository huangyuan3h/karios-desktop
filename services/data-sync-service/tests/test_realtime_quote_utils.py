import pytest
from data_sync_service.service.realtime_quote import (
    _as_str,
    _get,
)


def test_as_str_none():
    assert _as_str(None) is None


def test_as_str_empty_string():
    assert _as_str("") is None
    assert _as_str("   ") is None


def test_as_str_valid_string():
    assert _as_str("hello") == "hello"
    assert _as_str("  hello  ") == "hello"


def test_as_str_number():
    assert _as_str(123) == "123"
    assert _as_str(3.14) == "3.14"


def test_get_first_key():
    obj = {"a": 1, "b": 2, "c": 3}
    assert _get(obj, "a") == 1


def test_get_second_key():
    obj = {"a": None, "b": 2}
    assert _get(obj, "x", "b") == 2


def test_get_missing_key():
    obj = {"a": 1}
    assert _get(obj, "x", "y", "z") is None


def test_get_none_value():
    obj = {"a": None, "b": None}
    assert _get(obj, "a") is None
    assert _get(obj, "a", "b") is None


def test_get_empty_object():
    assert _get({}, "a") is None