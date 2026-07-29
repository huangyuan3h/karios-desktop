"""Realtime quote via tushare `realtime_quote` (query-only; no DB writes).

HK tickers fall back to East Money push2 because tushare `realtime_quote` does
not reliably return HK rows on every key/region.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.service.em_push2_http import em_get_json


def _as_str(val: Any) -> str | None:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    return s or None


def _get(obj: Any, *keys: str) -> Any:
    for k in keys:
        if k in obj and obj[k] is not None:
            return obj[k]
    return None


def _is_hk(ts_code: str) -> bool:
    return ts_code.strip().upper().endswith(".HK")


def _em_hk_fields() -> str:
    # f43 price, f44 high, f45 low, f46 open, f47 volume, f48 amount,
    # f57 code, f58 name, f60 pre_close, f169 change, f170 pct_chg.
    return "f43,f44,f45,f46,f47,f48,f57,f58,f60,f169,f170"


def _fetch_em_hk_quote(ts_code: str) -> dict[str, Any] | None:
    """Try East Money push2 for a single HK ticker. Returns normalized quote or None."""
    code = ts_code.strip().upper()
    if not code.endswith(".HK"):
        return None
    ticker = code[:-3]
    secid = f"116.{ticker}"
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {"secid": secid, "fields": _em_hk_fields(), "invt": "2", "fltt": "2"}
    try:
        j = em_get_json(url, params=params, referer="https://quote.eastmoney.com/")
    except Exception:
        return None
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, dict):
        return None

    def _num(key: str) -> float | None:
        v = data.get(key)
        if v in (None, "", "-"):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    price = _num("f43")
    if price is None or price <= 0:
        return None
    high = _num("f44")
    low = _num("f45")
    open_ = _num("f46")
    pre_close = _num("f60")
    change = _num("f169")
    pct_chg = _num("f170")
    volume = _num("f47")
    amount = _num("f48")

    return {
        "ts_code": code,
        "price": _as_str(price),
        "open": _as_str(open_),
        "high": _as_str(high),
        "low": _as_str(low),
        "pre_close": _as_str(pre_close),
        "change": _as_str(change),
        "pct_chg": _as_str(pct_chg),
        "volume": _as_str(volume),
        "amount": _as_str(amount),
        "trade_time": None,
    }


def _split_hk(codes: list[str]) -> tuple[list[str], list[str]]:
    """Split codes into (hk_codes, other_codes), preserving order, dedup."""
    hk: list[str] = []
    other: list[str] = []
    seen_hk: set[str] = set()
    seen_other: set[str] = set()
    for c in codes:
        up = c.strip().upper()
        if not up:
            continue
        if up.endswith(".HK"):
            if up not in seen_hk:
                seen_hk.add(up)
                hk.append(up)
        else:
            if up not in seen_other:
                seen_other.add(up)
                other.append(up)
    return hk, other


def _tushare_quotes(codes: list[str], *, api_key: str) -> list[dict[str, Any]]:
    """Call tushare realtime_quote for non-HK codes; returns normalized items list."""
    if not codes:
        return []
    ts.set_token(api_key)
    try:
        if hasattr(ts, "realtime_quote"):
            df = ts.realtime_quote(ts_code=",".join(codes))
        else:
            pro = ts.pro_api(api_key)
            df = pro.realtime_quote(ts_code=",".join(codes))
    except Exception:
        return []

    if not isinstance(df, pd.DataFrame) or df.empty:
        return []

    rows = df.to_dict(orient="records")
    out: list[dict[str, Any]] = []
    for r in rows:
        r2 = {str(k).lower(): v for k, v in r.items()}
        ts_code = _as_str(_get(r2, "ts_code", "code"))
        trade_time = _as_str(_get(r2, "trade_time", "time", "datetime"))
        if not trade_time:
            date_raw = _as_str(_get(r2, "date"))
            time_raw = _as_str(_get(r2, "time"))
            if date_raw and len(date_raw) == 8 and date_raw.isdigit():
                date_raw = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if date_raw and time_raw:
                trade_time = f"{date_raw} {time_raw}"
        else:
            date_raw = _as_str(_get(r2, "date"))
            if date_raw and len(date_raw) == 8 and date_raw.isdigit():
                date_raw = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            if date_raw and len(trade_time) <= 8 and ":" in trade_time:
                trade_time = f"{date_raw} {trade_time}"
        out.append(
            {
                "ts_code": ts_code,
                "price": _as_str(_get(r2, "price", "current", "last")),
                "open": _as_str(_get(r2, "open")),
                "high": _as_str(_get(r2, "high")),
                "low": _as_str(_get(r2, "low")),
                "pre_close": _as_str(_get(r2, "pre_close", "prev_close")),
                "change": _as_str(_get(r2, "change")),
                "pct_chg": _as_str(_get(r2, "pct_change", "pct_chg", "change_pct")),
                "volume": _as_str(_get(r2, "vol", "volume")),
                "amount": _as_str(_get(r2, "amount", "turnover")),
                "trade_time": trade_time,
            }
        )

    return [x for x in out if x.get("ts_code")]


def fetch_realtime_quotes(ts_codes: list[str]) -> dict[str, Any]:
    """
    Fetch realtime quotes for one or more ts_code.

    Returns:
      {"ok": True, "items": [...]} or {"ok": False, "error": "..."}.

    Notes:
    - Values are normalized to strings to avoid float precision issues in JSON.
    - Field names are normalized to: ts_code, price, open, high, low, pre_close, change, pct_chg, volume, amount, trade_time.
    - HK tickers (".HK" suffix) fall back to East Money push2 when tushare cannot return them.
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return {"ok": False, "error": "ts_code is required"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    hk_codes, other_codes = _split_hk(codes)

    items: list[dict[str, Any]] = []
    if other_codes:
        items.extend(_tushare_quotes(other_codes, api_key=settings.tu_share_api_key))
    if hk_codes:
        for code in hk_codes:
            em_quote = _fetch_em_hk_quote(code)
            if em_quote is not None:
                items.append(em_quote)
            else:
                # Last-resort: try tushare in case the .HK tushare bug is fixed.
                tq = _tushare_quotes([code], api_key=settings.tu_share_api_key)
                if tq:
                    items.extend(tq)

    return {"ok": True, "items": items}


def fetch_realtime_quotes_batched(
    ts_codes: list[str],
    *,
    batch_size: int = 50,
    max_workers: int = 6,
) -> list[dict[str, Any]]:
    """
    Fetch realtime quotes in parallel batches; returns merged items list.

    Stateless tushare calls are safe to run concurrently; max_workers caps
    inflight requests (similar rate limit intent as serial sleep between batches).
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return []
    parts = [codes[i : i + batch_size] for i in range(0, len(codes), batch_size)]
    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fetch_realtime_quotes, part) for part in parts if part]
        for future in as_completed(futures):
            resp = future.result()
            if isinstance(resp, dict) and resp.get("ok"):
                items.extend(resp.get("items") or [])
    return items

