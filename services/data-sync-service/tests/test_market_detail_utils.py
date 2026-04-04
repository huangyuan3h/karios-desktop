import pytest
from data_sync_service.service.market_detail import (
    _parse_symbol_cn_only,
)


def test_parse_symbol_cn_only_cn_prefix():
    result = _parse_symbol_cn_only("CN:000001")
    assert result == ("CN", "000001", "000001.SZ")


def test_parse_symbol_cn_only_sh():
    result = _parse_symbol_cn_only("CN:600000")
    assert result == ("CN", "600000", "600000.SH")


def test_parse_symbol_cn_only_ts_code():
    result = _parse_symbol_cn_only("000001.SZ")
    assert result == ("CN", "000001", "000001.SZ")


def test_parse_symbol_cn_only_ts_code_upper():
    result = _parse_symbol_cn_only("600000.sh")
    assert result == ("CN", "600000", "600000.SH")


def test_parse_symbol_cn_only_invalid_format():
    assert _parse_symbol_cn_only("INVALID") is None


def test_parse_symbol_cn_only_empty():
    assert _parse_symbol_cn_only("") is None
    assert _parse_symbol_cn_only(None) is None


def test_parse_symbol_cn_only_wrong_market():
    assert _parse_symbol_cn_only("HK:00700") is None


def test_parse_symbol_cn_only_invalid_ticker():
    assert _parse_symbol_cn_only("CN:ABC") is None
    assert _parse_symbol_cn_only("CN:123") is None