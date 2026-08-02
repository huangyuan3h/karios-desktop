from data_sync_service.service.market_detail import (
    _parse_symbol,
    _parse_symbol_cn_only,
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


def test_parse_symbol_hk_prefix():
    result = _parse_symbol("HK:00700")
    assert result == ("HK", "00700", "00700.HK")


def test_parse_symbol_hk_ts_code():
    result = _parse_symbol("00700.HK")
    assert result == ("HK", "00700", "00700.HK")


def test_parse_symbol_invalid_ticker():
    assert _parse_symbol("CN:ABC") is None
    assert _parse_symbol("CN:123") is None


# _parse_symbol_cn_only remains as a back-compat alias; chips/fund-flow still reject
# HK at the route layer with HTTP 400, but the parser itself now accepts HK.
def test_parse_symbol_cn_only_alias_supports_hk():
    result = _parse_symbol_cn_only("HK:00700")
    assert result == ("HK", "00700", "00700.HK")


def test_parse_symbol_cn_only_invalid_ticker():
    assert _parse_symbol_cn_only("CN:ABC") is None
    assert _parse_symbol_cn_only("CN:123") is None