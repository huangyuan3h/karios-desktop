"""East Money ETF realtime fund-flow helpers."""

from __future__ import annotations

import math
import time
from datetime import UTC, datetime
from typing import Any

from data_sync_service.service.em_push2_http import em_get_json

EM_ETF_SPOT_URLS = (
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
)
EM_ETF_SPOT_URL = EM_ETF_SPOT_URLS[1]
EM_ETF_PAGE_SIZE = 500
EM_ETF_MAX_PAGES = 30
EM_ETF_FLOW_SOURCE = "eastmoney.realtime_flow"
EM_ETF_FLOW_FIELDS = ",".join(
    [
        "f12",
        "f14",
        "f2",
        "f3",
        "f38",
        "f62",
        "f66",
        "f69",
        "f72",
        "f75",
        "f78",
        "f81",
        "f84",
        "f87",
        "f124",
        "f184",
        "f297",
    ]
)
EM_ETF_REFERER = "https://quote.eastmoney.com/center/gridlist.html#fund_etf"
_LAST_FETCH_ERROR: str | None = None

# Cache the full-market ETF snapshot per Shanghai date. The full list is
# ~1500 ETFs across ~16 pages (~38s under eastmoney throttling) yet the
# dashboard sync only needs the handful on the watchlist — caching makes
# repeated syncs (user clicks Sync & Copy) near-instant after the first
# pull of the day. Invalidated at midnight Asia/Shanghai.
_ETF_ROWS_CACHE: list[dict[str, Any]] | None = None
_ETF_ROWS_CACHE_DATE: str | None = None


def _cache_date() -> str:
    return datetime.now(tz=UTC).strftime("%Y-%m-%d")


def get_last_em_etf_fetch_error() -> str | None:
    return _LAST_FETCH_ERROR


def _set_last_fetch_error(error: str | None) -> None:
    global _LAST_FETCH_ERROR
    _LAST_FETCH_ERROR = error


def _market_id_for_symbol(symbol: str) -> int:
    s = str(symbol or "").strip()
    return 1 if s.startswith(("5", "6")) else 0


def _em_etf_spot_request(params: dict[str, str]) -> dict[str, Any]:
    errors: list[str] = []
    for url in EM_ETF_SPOT_URLS:
        try:
            return em_get_json(url, params=params, referer=EM_ETF_REFERER)
        except Exception as e:  # noqa: BLE001
            host = url.split("//", 1)[-1].split("/", 1)[0]
            errors.append(f"{host}:{e}")
    raise RuntimeError("; ".join(errors))


def _em_etf_spot_params(page_number: int) -> dict[str, str]:
    return {
        "pn": str(page_number),
        "pz": str(EM_ETF_PAGE_SIZE),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f12",
        "fs": "b:MK0021,b:MK0022,b:MK0023,b:MK0024,b:MK0827",
        "fields": EM_ETF_FLOW_FIELDS,
        "_": str(int(time.time() * 1000)),
    }


def _fetch_all_etf_spot_rows() -> list[dict[str, Any]]:
    global _ETF_ROWS_CACHE, _ETF_ROWS_CACHE_DATE
    today = _cache_date()
    if _ETF_ROWS_CACHE is not None and _ETF_ROWS_CACHE_DATE == today:
        return list(_ETF_ROWS_CACHE)
    rows: list[dict[str, Any]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages and page_number <= EM_ETF_MAX_PAGES:
        j = _em_etf_spot_request(_em_etf_spot_params(page_number))
        data = j.get("data") if isinstance(j, dict) else None
        if not isinstance(data, dict):
            if page_number == 1:
                raise RuntimeError(f"eastmoney_etf_missing_data:page={page_number}")
            break
        diff = data.get("diff")
        if not isinstance(diff, list) or not diff:
            if page_number == 1:
                raise RuntimeError("eastmoney_etf_empty_diff:page=1")
            break
        for row in diff:
            if isinstance(row, dict):
                rows.append(row)
        try:
            total = int(data.get("total") or 0)
            actual_page_size = max(1, len(diff))
            total_pages = max(1, math.ceil(total / actual_page_size))
        except (TypeError, ValueError):
            total_pages = page_number
        page_number += 1
    if not rows:
        raise RuntimeError("eastmoney_etf_no_rows")
    _ETF_ROWS_CACHE = list(rows)
    _ETF_ROWS_CACHE_DATE = today
    return rows


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _date_from_yyyymmdd(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _trade_time_from_timestamp(v: Any) -> str | None:
    try:
        ts = int(v)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=UTC).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _normalize_em_flow_row(row: dict[str, Any]) -> dict[str, Any]:
    fd_share = _safe_float(row.get("f38"))
    fd_share_wan = fd_share / 10_000.0 if fd_share is not None else None
    quote_ts = row.get("f124")
    return {
        "name": str(row.get("f14") or "").strip() or None,
        "latestPrice": _safe_float(row.get("f2")),
        "pctChange": _safe_float(row.get("f3")),
        "fdShareWan": fd_share_wan,
        "mainNetInflow": _safe_float(row.get("f62")),
        "superLargeNetInflow": _safe_float(row.get("f66")),
        "superLargeNetInflowRatio": _safe_float(row.get("f69")),
        "largeNetInflow": _safe_float(row.get("f72")),
        "largeNetInflowRatio": _safe_float(row.get("f75")),
        "mediumNetInflow": _safe_float(row.get("f78")),
        "mediumNetInflowRatio": _safe_float(row.get("f81")),
        "smallNetInflow": _safe_float(row.get("f84")),
        "smallNetInflowRatio": _safe_float(row.get("f87")),
        "mainNetInflowRatio": _safe_float(row.get("f184")),
        "tradeTime": _trade_time_from_timestamp(quote_ts),
        "dataDate": _date_from_yyyymmdd(row.get("f297")),
        "marketId": _market_id_for_symbol(str(row.get("f12") or "")),
        "source": EM_ETF_FLOW_SOURCE,
    }


def fetch_em_etf_realtime_flow_for_symbols(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """Return realtime East Money ETF fund-flow rows keyed by plain symbol."""
    wanted = {str(s).strip() for s in symbols if str(s).strip()}
    if not wanted:
        _set_last_fetch_error(None)
        return {}
    try:
        rows = _fetch_all_etf_spot_rows()
    except Exception as e:  # noqa: BLE001
        _set_last_fetch_error(str(e) or e.__class__.__name__)
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("f12") or "").strip()
        if code not in wanted:
            continue
        out[code] = _normalize_em_flow_row(row)
    missing = sorted(wanted - set(out))
    if missing:
        _set_last_fetch_error(f"eastmoney_etf_missing_symbols:{','.join(missing)}")
    else:
        _set_last_fetch_error(None)
    return out


def fetch_em_etf_spot_for_symbols(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """
    Return map symbol -> {fdShareWan, mainNetInflow, dataDate}.
    fdShareWan is 万份 (same unit as Tushare fund_share).
    mainNetInflow is CNY yuan (East Money 主力净流入-净额).
    """
    return fetch_em_etf_realtime_flow_for_symbols(symbols)
