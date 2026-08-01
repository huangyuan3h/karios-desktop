"""Unit tests for tv/scanner_api.py (OPT-057)."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from data_sync_service.tv import scanner_api
from data_sync_service.tv.scanner_api import (
    ScannerApiResult,
    build_request_payload,
    fetch_screener_via_api,
    friendly_to_internal_columns,
    internal_to_friendly_rows,
)


# --- helpers ---------------------------------------------------------------


def _ok_response(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def _ok_data(rows: list[list]) -> dict:
    """Build a synthetic Scanner API response body."""
    return {"data": [[None, row, None] for row in rows]}


# --- build_request_payload -------------------------------------------------


def test_build_request_payload_basic():
    payload = build_request_payload(
        filter_payload={"and": [{"left": "close", "operation": "greater", "right": 0}]},
        columns=["name", "close"],
        range_=(0, 50),
    )
    assert payload["filter"] == {"and": [{"left": "close", "operation": "greater", "right": 0}]}
    assert payload["columns"] == ["name", "close"]
    assert payload["range"] == [0, 50]
    assert payload["sort"]["sortBy"] == "market_cap_basic"
    assert payload["sort"]["sortOrder"] == "desc"


def test_build_request_payload_default_range():
    payload = build_request_payload(
        filter_payload={"and": []},
        columns=["name"],
    )
    assert payload["range"] == [0, scanner_api.DEFAULT_MAX_ROWS]


# --- fetch_screener_via_api: input validation ------------------------------


def test_fetch_rejects_empty_filter():
    with pytest.raises(scanner_api.PermanentApiError, match="filter_payload"):
        fetch_screener_via_api(filter_payload={}, columns=["name"])


def test_fetch_rejects_empty_columns():
    with pytest.raises(scanner_api.PermanentApiError, match="columns"):
        fetch_screener_via_api(filter_payload={"and": []}, columns=[])


# --- fetch_screener_via_api: success path ---------------------------------


def test_fetch_success_parses_response():
    payload = _ok_data([
        ["NVDA", "NVIDIA Corp", 123.45, 1.5, 1000, 2_000_000_000_000, "Tech", "Semis", "US", 30.0, 60.0, 1.0, 150.0],
    ])
    with patch.object(scanner_api, "_post_json", return_value=payload):
        result = fetch_screener_via_api(
            filter_payload={"and": [{"left": "close", "operation": "greater", "right": 0}]},
            columns=scanner_api.default_columns(),
        )
    assert isinstance(result, ScannerApiResult)
    assert result.headers == scanner_api.default_columns()
    assert len(result.rows) == 1
    row = result.rows[0]
    # Friendly mapping happens inside dispatcher; here we just check
    # the raw index-to-header alignment.
    assert row["name"] == "NVDA"
    assert row["close"] == "123.45"


def test_fetch_handles_short_rows():
    """If TV returns fewer values per row than requested columns, missing
    values should default to empty string (not raise)."""
    payload = _ok_data([["NVDA", "NVIDIA Corp"]])  # only 2 of N columns
    with patch.object(scanner_api, "_post_json", return_value=payload):
        result = fetch_screener_via_api(
            filter_payload={"and": []},
            columns=["name", "description", "close"],
        )
    assert result.rows[0]["name"] == "NVDA"
    assert result.rows[0]["description"] == "NVIDIA Corp"
    assert result.rows[0]["close"] == ""  # missing → ""


def test_fetch_skips_malformed_entries():
    payload = {"data": [[None, ["NVDA", "NVIDIA"], None], "bogus", [None, ["AAPL"], None]]}
    with patch.object(scanner_api, "_post_json", return_value=payload):
        result = fetch_screener_via_api(
            filter_payload={"and": []},
            columns=["name", "description"],
        )
    assert len(result.rows) == 2


# --- fetch_screener_via_api: error paths -----------------------------------


def test_fetch_treats_5xx_as_transient():
    err = scanner_api.TransientApiError("http_503:server overloaded")
    with patch.object(scanner_api, "_post_json", side_effect=err):
        with pytest.raises(scanner_api.TransientApiError):
            fetch_screener_via_api(
                filter_payload={"and": []},
                columns=["name"],
                max_retries=0,  # disable retry to keep test fast
            )


def test_fetch_treats_4xx_as_permanent():
    err = scanner_api.PermanentApiError("http_400:bad filter")
    with patch.object(scanner_api, "_post_json", side_effect=err):
        with pytest.raises(scanner_api.PermanentApiError):
            fetch_screener_via_api(
                filter_payload={"and": []},
                columns=["name"],
                max_retries=0,
            )


def test_fetch_retries_on_transient_then_succeeds():
    payload = _ok_data([["NVDA", "NVIDIA"]])
    call_count = {"n": 0}

    def flaky(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise scanner_api.TransientApiError("network:URLError")
        return payload

    with patch.object(scanner_api, "_post_json", side_effect=flaky):
        result = fetch_screener_via_api(
            filter_payload={"and": []},
            columns=["name", "description"],
            max_retries=2,
            backoff_s=0,
        )
    assert call_count["n"] == 2
    assert len(result.rows) == 1


def test_fetch_json_decode_raises_permanent():
    with patch.object(scanner_api, "_post_json", return_value=b"{not json"):
        with pytest.raises(scanner_api.PermanentApiError, match="json_decode"):
            fetch_screener_via_api(
                filter_payload={"and": []},
                columns=["name"],
                max_retries=0,
            )


def test_fetch_missing_data_raises_permanent():
    payload = {"no_data_field": []}
    with patch.object(scanner_api, "_post_json", return_value=_ok_response(payload)):
        with pytest.raises(scanner_api.PermanentApiError, match="response_missing_data_list"):
            fetch_screener_via_api(
                filter_payload={"and": []},
                columns=["name"],
                max_retries=0,
            )


# --- friendly_to_internal_columns / internal_to_friendly_rows -------------


def test_friendly_to_internal_columns_known():
    out = friendly_to_internal_columns(["Symbol", "Price", "High 52W"])
    assert out == ["name", "close", "High.Interval52Week"]


def test_friendly_to_internal_columns_unknown_passthrough():
    """Unknown friendly names are passed through unchanged so power
    users can extend the whitelist without code changes."""
    out = friendly_to_internal_columns(["Symbol", "CustomMetric"])
    assert out == ["name", "CustomMetric"]


def test_internal_to_friendly_rows_roundtrip():
    headers_internal = ["name", "close", "High.Interval52Week"]
    raw_values = [["NVDA", 123.45, 150.0]]
    headers_friendly, rows = internal_to_friendly_rows(headers_internal, raw_values)
    assert headers_friendly == ["Symbol", "Price", "High 52W"]
    assert rows[0]["Symbol"] == "NVDA"
    assert rows[0]["Price"] == "123.45"
    # 150.0 is integer-valued, formatter strips ".0"
    assert rows[0]["High 52W"] == "150"


def test_internal_to_friendly_rows_unknown_passthrough():
    """Internal names not in COLUMN_MAP should pass through as-is."""
    headers_internal = ["name", "CustomInternal"]
    raw_values = [["NVDA", "x"]]
    headers_friendly, rows = internal_to_friendly_rows(headers_internal, raw_values)
    assert headers_friendly == ["Symbol", "CustomInternal"]
    assert rows[0]["CustomInternal"] == "x"


def test_internal_to_friendly_rows_value_formatting():
    headers_internal = ["name", "close", "market_cap_basic"]
    raw_values = [["NVDA", 123.0, 1_500_000_000_000.0]]
    headers_friendly, rows = internal_to_friendly_rows(headers_internal, raw_values)
    assert headers_friendly == ["Symbol", "Price", "Market Cap"]
    # Integer-valued floats should render as int strings (TV sometimes returns 123.0)
    assert rows[0]["Price"] == "123"
    assert rows[0]["Market Cap"] == "1500000000000"