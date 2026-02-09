"""Realtime quote via tushare `realtime_quote` (query-only; no DB writes)."""

from __future__ import annotations

from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings


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


def fetch_realtime_quotes(ts_codes: list[str]) -> dict[str, Any]:
    """
    Fetch realtime quotes for one or more ts_code.

    Returns:
      {"ok": True, "items": [...]} or {"ok": False, "error": "..."}.

    Notes:
    - Values are normalized to strings to avoid float precision issues in JSON.
    - Field names are normalized to: ts_code, price, open, high, low, pre_close, change, pct_chg, volume, amount, trade_time.
    """
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return {"ok": False, "error": "ts_code is required"}

    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    # Some tushare versions require set_token before calling module-level APIs.
    ts.set_token(settings.tu_share_api_key)

    try:
        if hasattr(ts, "realtime_quote"):
            df = ts.realtime_quote(ts_code=",".join(codes))
        else:
            # Fallback: try through pro client if available.
            pro = ts.pro_api(settings.tu_share_api_key)
            df = getattr(pro, "realtime_quote")(ts_code=",".join(codes))
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}

    if df is None or len(df) == 0:
        return {"ok": True, "items": []}

    # Normalize to list[dict] with lower-cased keys (tushare returns upper-case columns).
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
            # If trade_time is time-only (e.g. "11:15:51"), enrich with date.
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

    # Filter out rows without ts_code
    out2 = [x for x in out if x.get("ts_code")]
    return {"ok": True, "items": out2}

