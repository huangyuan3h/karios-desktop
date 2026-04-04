import pytest
from data_sync_service.service.market_quotes import (
    symbol_to_ts_code,
    ts_code_to_symbol,
)


def test_symbol_to_ts_code_cn_sh():
    assert symbol_to_ts_code("CN:600000") == "600000.SH"
    assert symbol_to_ts_code("CN:601318") == "601318.SH"


def test_symbol_to_ts_code_cn_sz():
    assert symbol_to_ts_code("CN:000001") == "000001.SZ"
    assert symbol_to_ts_code("CN:000002") == "000002.SZ"


def test_symbol_to_ts_code_invalid_format():
    assert symbol_to_ts_code("") is None
    assert symbol_to_ts_code("invalid") is None
    assert symbol_to_ts_code("CN") is None


def test_symbol_to_ts_code_invalid_ticker():
    assert symbol_to_ts_code("CN:12345") is None
    assert symbol_to_ts_code("CN:abcdef") is None


def test_ts_code_to_symbol():
    assert ts_code_to_symbol("000001.SZ") == "CN:000001"
    assert ts_code_to_symbol("600000.SH") == "CN:600000"


def test_ts_code_to_symbol_no_suffix():
    assert ts_code_to_symbol("000001") == "CN:000001"


def test_ts_code_to_symbol_custom_market():
    assert ts_code_to_symbol("000001.HK", "HK") == "HK:000001"