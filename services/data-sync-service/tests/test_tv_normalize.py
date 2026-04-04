import pytest
from data_sync_service.tv.normalize import (
    normalize_header,
    normalize_headers,
    split_symbol_cell,
    drop_empty_columns,
    enrich_symbol_columns,
)


def test_normalize_header_basic():
    assert normalize_header("  hello  world  ") == "hello world"


def test_normalize_header_nbsp():
    assert normalize_header("hello\u00A0world") == "hello world"


def test_normalize_header_narrow_nbsp():
    assert normalize_header("hello\u202Fworld") == "hello world"


def test_normalize_header_newlines():
    assert normalize_header("hello\nworld") == "hello world"


def test_normalize_header_multiple_spaces():
    assert normalize_header("hello    world") == "hello world"


def test_normalize_headers():
    result = normalize_headers(["  A  ", "B\nC", ""])
    assert result == ["A", "B C", "Column"]


def test_split_symbol_cell_simple():
    result = split_symbol_cell("AAPL")
    assert result["Ticker"] == "AAPL"
    assert result["Name"] == ""
    assert result["Flags"] == ""


def test_split_symbol_cell_with_name():
    result = split_symbol_cell("AAPL\nApple Inc")
    assert result["Ticker"] == "AAPL"
    assert result["Name"] == "Apple Inc"
    assert result["Flags"] == ""


def test_split_symbol_cell_with_flags():
    result = split_symbol_cell("AAPL\nApple Inc\nD\nE")
    assert result["Ticker"] == "AAPL"
    assert result["Name"] == "Apple Inc"
    assert result["Flags"] == "D E"


def test_split_symbol_cell_empty():
    result = split_symbol_cell("")
    assert result["Ticker"] == ""
    assert result["Name"] == ""
    assert result["Flags"] == ""


def test_drop_empty_columns_all_empty():
    headers = ["A", "B", "C"]
    rows = [{"A": "", "B": None, "C": "  "}]
    kept_headers, kept_rows = drop_empty_columns(headers, rows)
    assert kept_headers == []
    assert kept_rows == [{}]


def test_drop_empty_columns_some_empty():
    headers = ["A", "B", "C"]
    rows = [{"A": "val", "B": "", "C": ""}]
    kept_headers, kept_rows = drop_empty_columns(headers, rows)
    assert "A" in kept_headers
    assert "B" not in kept_headers


def test_drop_empty_columns_empty_rows():
    headers = ["A", "B"]
    kept_headers, kept_rows = drop_empty_columns(headers, [])
    assert kept_headers == headers
    assert kept_rows == []


def test_enrich_symbol_columns_no_symbol():
    headers = ["A", "B"]
    rows = [{"A": "1", "B": "2"}]
    out_headers, out_rows = enrich_symbol_columns(headers, rows)
    assert out_headers == headers
    assert out_rows == rows


def test_enrich_symbol_columns_with_symbol():
    headers = ["Symbol", "Price"]
    rows = [{"Symbol": "AAPL\nApple Inc", "Price": "150"}]
    out_headers, out_rows = enrich_symbol_columns(headers, rows)
    assert "Ticker" in out_headers
    assert "Name" in out_headers
    assert out_rows[0]["Ticker"] == "AAPL"
    assert out_rows[0]["Name"] == "Apple Inc"


def test_enrich_symbol_columns_with_chinese():
    headers = ["代码", "Price"]
    rows = [{"代码": "000001\n平安银行", "Price": "10"}]
    out_headers, out_rows = enrich_symbol_columns(headers, rows)
    assert "Ticker" in out_headers
    assert out_rows[0]["Ticker"] == "000001"