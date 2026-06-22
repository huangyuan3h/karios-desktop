"""East Money ETF spot fallback for share / main-net-inflow when Tushare lags."""

from __future__ import annotations

import math
from typing import Any

from data_sync_service.service.em_push2_http import em_get_json

EM_ETF_SPOT_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
EM_ETF_PAGE_SIZE = 500
EM_ETF_MAX_PAGES = 30


def _market_id_for_symbol(symbol: str) -> int:
    s = str(symbol or "").strip()
    return 1 if s.startswith(("5", "6")) else 0


def _em_etf_spot_request(params: dict[str, str]) -> dict[str, Any]:
    return em_get_json(
        EM_ETF_SPOT_URL,
        params=params,
        referer="https://quote.eastmoney.com/center/gridlist.html#fund_etf",
    )


def _fetch_all_etf_spot_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    page_number = 1
    total_pages = 1
    while page_number <= total_pages and page_number <= EM_ETF_MAX_PAGES:
        params = {
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
            "fields": "f12,f14,f2,f38,f62,f297",
        }
        j = _em_etf_spot_request(params)
        data = j.get("data") if isinstance(j, dict) else None
        diff = data.get("diff") if isinstance(data, dict) else None
        if not isinstance(diff, list) or not diff:
            break
        for row in diff:
            if isinstance(row, dict):
                rows.append(row)
        try:
            total = int((data or {}).get("total") or 0)
            total_pages = max(1, math.ceil(total / EM_ETF_PAGE_SIZE))
        except (TypeError, ValueError):
            total_pages = page_number
        page_number += 1
    return rows


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def fetch_em_etf_spot_for_symbols(symbols: list[str]) -> dict[str, dict[str, Any]]:
    """
    Return map symbol -> {fdShareWan, mainNetInflow, dataDate}.
    fdShareWan is 万份 (same unit as Tushare fund_share).
    mainNetInflow is CNY yuan (East Money 主力净流入-净额).
    """
    wanted = {str(s).strip() for s in symbols if str(s).strip()}
    if not wanted:
        return {}
    try:
        rows = _fetch_all_etf_spot_rows()
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("f12") or "").strip()
        if code not in wanted:
            continue
        fd_share = _safe_float(row.get("f38"))
        # f38 is 最新份额 in 份; convert to 万份 for Tushare parity
        fd_share_wan = fd_share / 10_000.0 if fd_share is not None else None
        main_net = _safe_float(row.get("f62"))
        data_date_raw = row.get("f297")
        data_date: str | None = None
        if data_date_raw is not None:
            s = str(data_date_raw).strip()
            if len(s) == 8 and s.isdigit():
                data_date = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
        out[code] = {
            "fdShareWan": fd_share_wan,
            "mainNetInflow": main_net,
            "dataDate": data_date,
            "marketId": _market_id_for_symbol(code),
        }
    return out
