"""TradingView Scanner API client (OPT-057).

Undocumented internal API at https://scanner.tradingview.com/global/scan.
POST JSON: { filter, symbols, columns, sort, range } → returns 30+ fields per row.

This module is the PRIMARY data source for mode='api' screeners (replacing
Chrome CDP for new screener registrations). It does NOT require TV login,
cookies, or Chrome — just an HTTP POST.

Spike proven 2026-08-01: see docs/designs/ego-lite-spike-2026-08.md §2.

NOTE: This API has no SLA / contract. Failures are treated as transient
and trigger fallback to ego_lite → chrome in the dispatcher.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

SCANNER_API_URL = "https://scanner.tradingview.com/global/scan"
DEFAULT_TIMEOUT_S = 10.0
DEFAULT_MAX_ROWS = 100

# Friendly-name → TV Scanner API internal name (the column key inside the
# `columns` array of the request payload). Lock this mapping so we don't
# silently break if TV renames columns.
COLUMN_MAP: dict[str, str] = {
    "Symbol": "name",
    "Name": "description",
    "Price": "close",
    "Change %": "change",
    "Volume": "volume",
    "Market Cap": "market_cap_basic",
    "Sector": "sector",
    "Industry": "industry",
    "Country": "country",
    "P/E": "price_earnings_ttm",
    "RSI": "RSI",
    "MACD": "MACD.macd",
    # High 52W is required by the pullback window (-15% to -5%).
    # TV encodes it as `High.Interval52Week` (dotted for nested indicators).
    "High 52W": "High.Interval52Week",
}


class ScannerApiError(RuntimeError):
    """Base error for TV Scanner API calls."""


class TransientApiError(ScannerApiError):
    """Retryable error (5xx, network, timeout). Triggers fallback to ego_lite."""


class PermanentApiError(ScannerApiError):
    """Non-retryable error (4xx, malformed filter). Bubbles up to user."""


@dataclass(frozen=True)
class ScannerApiResult:
    """Normalised output of one Scanner API call.

    `rows` is a list of dicts keyed by friendly column name (e.g. "Price"),
    matching the contract expected by the rest of the screener pipeline
    (capture.py / normalize.py). `raw_rows` is the original payload for
    debug / audit (`payload.capturedVia` already records the channel).
    """

    headers: list[str]
    rows: list[dict[str, str]]
    raw_rows: list[list[Any]] = field(default_factory=list)
    captured_at: str = ""


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(tz=UTC).isoformat()


def _scanner_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Content-Type": "application/json",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "Connection": "keep-alive",
    }


def build_request_payload(
    *,
    filter_payload: dict[str, Any] | list[Any],
    columns: list[str],
    sort_by: str = "market_cap_basic",
    sort_order: str = "desc",
    range_: tuple[int, int] = (0, DEFAULT_MAX_ROWS),
) -> dict[str, Any]:
    """Build the Scanner API request body.

    `filter_payload` is the raw TV filter JSON (see spike §2 for shape).
    `columns` is a list of internal column names (e.g. ["name", "close"]).

    NOTE: TV Scanner API expects `filter` to be an array of conditions.
    A single condition is represented as a dict inside the array.
    Multiple conditions in the array are ANDed together.
    """
    return {
        "filter": filter_payload,
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": list(columns),
        "sort": {"sortBy": sort_by, "sortOrder": sort_order},
        "range": [int(range_[0]), int(range_[1])],
    }


def _post_json(url: str, payload: dict[str, Any], *, timeout: float) -> bytes:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=_scanner_headers(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            raw = resp.read()
    except urllib.error.HTTPError as e:
        preview = e.read()[:160].decode("utf-8", errors="replace") if e.fp else ""
        if 500 <= e.code < 600:
            raise TransientApiError(f"http_{e.code}:{preview}") from e
        raise PermanentApiError(f"http_{e.code}:{preview}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise TransientApiError(f"network:{type(e).__name__}:{e}") from e
    if status >= 400:
        preview = raw[:160].decode("utf-8", errors="replace")
        if 500 <= status < 600:
            raise TransientApiError(f"http_{status}:{preview}")
        raise PermanentApiError(f"http_{status}:{preview}")
    return raw


def fetch_screener_via_api(
    *,
    filter_payload: dict[str, Any] | list[Any],
    columns: list[str],
    sort_by: str = "market_cap_basic",
    sort_order: str = "desc",
    range_: tuple[int, int] = (0, DEFAULT_MAX_ROWS),
    timeout_s: float = DEFAULT_TIMEOUT_S,
    max_retries: int = 1,
    backoff_s: float = 1.0,
) -> ScannerApiResult:
    """POST scanner.tradingview.com/global/scan and return normalised rows.

    Raises TransientApiError on retryable failures (network / 5xx / timeout)
    so the dispatcher can fall back to ego_lite. Raises PermanentApiError
    on 4xx (caller bug, e.g. malformed filter).
    """
    if not isinstance(filter_payload, (dict, list)):
        raise PermanentApiError("filter_payload must be a non-empty dict or list")
    if isinstance(filter_payload, (dict, list)) and not filter_payload:
        raise PermanentApiError("filter_payload must be a non-empty dict or list")
    if not columns:
        raise PermanentApiError("columns must be a non-empty list")
    payload = build_request_payload(
        filter_payload=filter_payload,
        columns=columns,
        sort_by=sort_by,
        sort_order=sort_order,
        range_=range_,
    )
    last_err: TransientApiError | None = None
    for attempt in range(max_retries + 1):
        try:
            raw = _post_json(SCANNER_API_URL, payload, timeout=timeout_s)
            return _parse_response(raw=raw, requested_columns=columns)
        except TransientApiError as e:
            last_err = e
            if attempt >= max_retries:
                raise
            time.sleep(backoff_s * (2**attempt))
    if last_err is not None:
        raise last_err
    raise TransientApiError("exhausted retries without error (impossible)")


def _parse_response(*, raw: bytes | dict, requested_columns: list[str]) -> ScannerApiResult:
    if isinstance(raw, (bytes, bytearray)):
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise PermanentApiError(f"json_decode:{e}") from e
    elif isinstance(raw, dict):
        body = raw
    else:
        raise PermanentApiError("response_not_decodable")
    if not isinstance(body, dict):
        raise PermanentApiError("response_not_object")
    data = body.get("data")
    if not isinstance(data, list):
        raise PermanentApiError("response_missing_data_list")
    headers = list(requested_columns)
    rows: list[dict[str, str]] = []
    raw_rows: list[list[Any]] = []
    for entry in data:
        # TV Scanner API returns objects {"s": "SYMBOL", "d": [values]} OR
        # arrays [None, [values], None] depending on the endpoint variant.
        raw_values: list[Any] = []
        if isinstance(entry, dict):
            raw_values = entry.get("d", [])
        elif isinstance(entry, list) and len(entry) >= 2:
            raw_values = entry[1] if isinstance(entry[1], list) else []
        else:
            continue
        raw_rows.append(raw_values)
        d: dict[str, str] = {}
        for i, col in enumerate(requested_columns):
            v = raw_values[i] if i < len(raw_values) else None
            d[col] = _format_value(v)
        rows.append(d)
    return ScannerApiResult(
        headers=headers,
        rows=rows,
        raw_rows=raw_rows,
        captured_at=_now_iso(),
    )


def _format_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)


def default_columns() -> list[str]:
    """Default column list for new screener registrations."""
    return list(COLUMN_MAP.values())


def friendly_to_internal_columns(friendly_names: list[str]) -> list[str]:
    """Translate friendly column names (UI) → TV Scanner API internal names.

    Unknown friendly names are passed through as-is so power users can
    extend the whitelist without code changes.
    """
    out: list[str] = []
    for n in friendly_names:
        out.append(COLUMN_MAP.get(n, n))
    return out


def internal_to_friendly_rows(
    headers_internal: list[str],
    raw_values_lists: list[list[Any]],
) -> tuple[list[str], list[dict[str, str]]]:
    """Translate raw (header, value) pairs from the API into friendly rows.

    This is the inverse of `friendly_to_internal_columns`: it remaps internal
    column names (e.g. `market_cap_basic`) back to friendly names
    (e.g. `Market Cap`) for display, matching `pickColumns` in ScreenerPage.
    """
    inv_map = {v: k for k, v in COLUMN_MAP.items()}
    friendly_headers = [inv_map.get(h, h) for h in headers_internal]
    rows: list[dict[str, str]] = []
    for raw_values in raw_values_lists:
        d: dict[str, str] = {}
        for i, h in enumerate(friendly_headers):
            v = raw_values[i] if i < len(raw_values) else None
            d[h] = _format_value(v)
        rows.append(d)
    return friendly_headers, rows