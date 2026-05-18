import pytest
import hashlib
import base64
from data_sync_service.service.broker import (
    now_iso,
    _norm_str,
    _sha256_bytes,
    _decode_data_url,
)


def test_now_iso_format():
    result = now_iso()
    assert "T" in result


def test_norm_str_none():
    assert _norm_str(None) == ""


def test_norm_str_whitespace():
    assert _norm_str("  hello  world  ") == "hello world"
    assert _norm_str("\n\nhello\n\n") == "hello"


def test_norm_str_normal():
    assert _norm_str("hello") == "hello"


def test_sha256_bytes():
    result = _sha256_bytes(b"hello")
    expected = hashlib.sha256(b"hello").hexdigest()
    assert result == expected


def test_decode_data_url_valid():
    data = base64.b64encode(b"test data").decode()
    data_url = f"data:image/png;base64,{data}"
    raw, media_type = _decode_data_url(data_url)
    assert raw == b"test data"
    assert media_type == "image/png"


def test_decode_data_url_invalid():
    raw, media_type = _decode_data_url("")
    assert raw == b""
    assert media_type == "application/octet-stream"


def test_decode_data_url_no_base64():
    raw, media_type = _decode_data_url("data:image/png")
    assert raw == b""