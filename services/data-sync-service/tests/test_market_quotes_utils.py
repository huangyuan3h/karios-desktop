import pytest
from data_sync_service.service.market_quotes import (
    symbol_to_ts_code,
    ts_code_to_symbol,
)


def test_symbol_to_ts_code_sh():
    assert symbol_to_ts_code("CN:600000") == "600000.SH"
    assert symbol_to_ts_code("CN:601318") == "601318.SH"


def test_symbol_to_ts_code_sz():
    assert symbol_to_ts_code("CN:000001") == "000001.SZ"
    assert symbol_to_ts_code("CN:300750") == "300750.SZ"


def test_symbol_to_ts_code_invalid_format():
    assert symbol_to_ts_code("600000") is None
    assert symbol_to_ts_code("INVALID:600000") is None


def test_symbol_to_ts_code_empty():
    assert symbol_to_ts_code("") is None
    assert symbol_to_ts_code(None) is None


def test_symbol_to_ts_code_non_digit_ticker():
    assert symbol_to_ts_code("CN:ABC123") is None


def test_ts_code_to_symbol():
    assert ts_code_to_symbol("000001.SZ") == "CN:000001"
    assert ts_code_to_symbol("600000.SH") == "CN:600000"


def test_ts_code_to_symbol_no_dot():
    assert ts_code_to_symbol("000001") == "CN:000001"


def test_ts_code_to_symbol_custom_market():
    assert ts_code_to_symbol("00700.HK", "HK") == "HK:00700"