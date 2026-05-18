import pytest
from data_sync_service.service.broker import (
    _norm_str,
    _sha256_bytes,
    _decode_data_url,
    _dedupe,
    _pick_first_str,
)


def test_norm_str_empty():
    assert _norm_str(None) == ""
    assert _norm_str("") == ""


def test_norm_str_whitespace():
    assert _norm_str("  hello  world  ") == "hello world"
    assert _norm_str("\t\nhello\n\t") == "hello"


def test_norm_str_number():
    assert _norm_str(123) == "123"
    assert _norm_str(3.14) == "3.14"


def test_sha256_bytes():
    result = _sha256_bytes(b"hello")
    assert len(result) == 64
    assert result == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"


def test_decode_data_url_empty():
    raw, media = _decode_data_url("")
    assert raw == b""
    assert media == "application/octet-stream"


def test_decode_data_url_no_base64():
    raw, media = _decode_data_url("data:text/plain,hello")
    assert raw == b""
    assert media == "application/octet-stream"


def test_decode_data_url_valid():
    data_url = "data:image/png;base64,aGVsbG8="
    raw, media = _decode_data_url(data_url)
    assert raw == b"hello"
    assert media == "image/png"


def test_decode_data_url_jpeg():
    data_url = "data:image/jpeg;base64,/9j/4AAQSkZJRg=="
    raw, media = _decode_data_url(data_url)
    assert media == "image/jpeg"


def test_dedupe_empty():
    assert _dedupe([], keys=["a"]) == []


def test_dedupe_single():
    rows = [{"a": "1", "b": "x"}]
    result = _dedupe(rows, keys=["a"])
    assert result == rows


def test_dedupe_removes_duplicates():
    rows = [
        {"a": "1", "b": "x"},
        {"a": "1", "b": "y"},
        {"a": "2", "b": "z"},
    ]
    result = _dedupe(rows, keys=["a"])
    assert len(result) == 2
    assert result[0]["a"] == "1"
    assert result[1]["a"] == "2"


def test_dedupe_with_none_values():
    rows = [
        {"a": None, "b": "x"},
        {"a": None, "b": "y"},
    ]
    result = _dedupe(rows, keys=["a"])
    assert len(result) == 2


def test_pick_first_str_found():
    obj = {"a": "first", "b": "second", "c": "third"}
    assert _pick_first_str(obj, ["x", "b", "a"]) == "second"


def test_pick_first_str_not_found():
    obj = {"a": "first"}
    assert _pick_first_str(obj, ["x", "y", "z"]) == ""


def test_pick_first_str_empty_values():
    obj = {"a": "", "b": "  ", "c": "valid"}
    assert _pick_first_str(obj, ["a", "b", "c"]) == "valid"