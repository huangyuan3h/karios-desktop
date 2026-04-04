import pytest
from data_sync_service.service.market_bars import (
    _parse_symbol,
)


def test_parse_symbol_cn_prefix():
    result = _parse_symbol("CN:000001")
    assert result == ("CN", "000001", "000001.SZ")


def test_parse_symbol_sh():
    result = _parse_symbol("CN:600000")
    assert result == ("CN", "600000", "600000.SH")


def test_parse_symbol_ts_code():
    result = _parse_symbol("000001.SZ")
    assert result == ("CN", "000001", "000001.SZ")


def test_parse_symbol_ts_code_upper():
    result = _parse_symbol("600000.sh")
    assert result == ("CN", "600000", "600000.SH")


def test_parse_symbol_invalid_format():
    assert _parse_symbol("INVALID") is None


def test_parse_symbol_empty():
    assert _parse_symbol("") is None
    assert _parse_symbol(None) is None


def test_parse_symbol_wrong_market():
    assert _parse_symbol("HK:00700") is None


def test_parse_symbol_invalid_ticker():
    assert _parse_symbol("CN:ABC") is None