import pytest
import pandas as pd
from data_sync_service.service.realtime_quote import (
    _as_str,
    _get,
)


def test_as_str_none():
    assert _as_str(None) is None


def test_as_str_nan():
    assert _as_str(pd.NA) is None
    assert _as_str(float("nan")) is None


def test_as_str_valid():
    assert _as_str("hello") == "hello"
    assert _as_str(123) == "123"
    assert _as_str("  test  ") == "test"


def test_as_str_empty():
    assert _as_str("") is None
    assert _as_str("   ") is None


def test_get_first_key():
    obj = {"a": 1, "b": 2}
    assert _get(obj, "a", "b") == 1


def test_get_second_key():
    obj = {"b": 2}
    assert _get(obj, "a", "b") == 2


def test_get_missing():
    obj = {"c": 3}
    assert _get(obj, "a", "b") is None


def test_get_with_none_value():
    obj = {"a": None, "b": 2}
    assert _get(obj, "a", "b") == 2