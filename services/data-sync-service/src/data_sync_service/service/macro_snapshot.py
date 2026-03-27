"""Macro + CN index snapshot for Index page (EOD from DB + optional realtime overlay)."""

from __future__ import annotations

import math
from typing import Any

from data_sync_service.db.macro_daily import ensure_table, fetch_last_closes, get_latest_row
from data_sync_service.service.macro_daily import (
    SID_A50,
    SID_COMM_COPPER,
    SID_COMM_ENERGY,
    SID_COMM_GOLD,
    SID_IXIC,
    SID_NVDA,
    SID_USDCNH,
)
from data_sync_service.service.macro_snapshot_on_demand import (
    enrich_macro_items_on_demand,
    macro_snapshot_warning,
)
from data_sync_service.service.market_regime import get_index_signals
from data_sync_service.service.realtime_quote import fetch_realtime_quotes

MACRO_CARDS: list[dict[str, Any]] = [
    {
        "seriesId": SID_IXIC,
        "name": "Nasdaq Composite",
        "category": "us_tech",
        "why": "Overnight US tech drives CN tech sentiment; watch before the A-share open.",
        "realtimeTsCode": "IXIC",
    },
    {
        "seriesId": SID_NVDA,
        "name": "NVIDIA",
        "category": "us_tech",
        "why": "Leading US AI / GPU name; often correlates with CN compute / optics names.",
        "realtimeTsCode": "NVDA",
    },
    {
        "seriesId": SID_USDCNH,
        "name": "USD/CNH",
        "category": "fx",
        "why": "Offshore CNY is a key valve for northbound / foreign flows into A-shares.",
        "realtimeTsCode": "USDCNH.FXCM",
    },
    {
        "seriesId": SID_A50,
        "name": "FTSE China A50",
        "category": "a50",
        "why": "Overnight A50 futures are a rough 'night session' for A-share positioning.",
        "realtimeTsCode": None,
    },
    {
        "seriesId": SID_COMM_ENERGY,
        "name": "Energy (INE SC main)",
        "category": "commodity",
        "why": "Oil moves often lead cyclical / energy sector rotation in A-shares.",
        "realtimeTsCode": None,
    },
    {
        "seriesId": SID_COMM_GOLD,
        "name": "Gold (SHFE AU main)",
        "category": "commodity",
        "why": "Gold / metals strength often precedes mining and materials themes.",
        "realtimeTsCode": None,
    },
    {
        "seriesId": SID_COMM_COPPER,
        "name": "Copper (SHFE CU main)",
        "category": "commodity",
        "why": "Copper is a cyclical macro bellwether; relevant for basic materials.",
        "realtimeTsCode": None,
    },
]


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def _ma(closes: list[float], n: int) -> float | None:
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / float(n)


def _macro_item_from_db(meta: dict[str, Any], closes: list[tuple[str, float]]) -> dict[str, Any]:
    series_id = str(meta["seriesId"])
    close_vals = [c for _, c in closes if c is not None and math.isfinite(c)]
    as_of = closes[-1][0] if closes else None
    last_close = close_vals[-1] if close_vals else None
    ma5 = _ma(close_vals, 5) if len(close_vals) >= 5 else None
    ma20 = _ma(close_vals, 20) if len(close_vals) >= 20 else None
    latest = get_latest_row(series_id)
    pct = None
    if latest and latest.get("pct_chg") is not None:
        pct = _safe_float(latest.get("pct_chg"))
    return {
        "seriesId": series_id,
        "name": meta["name"],
        "category": meta["category"],
        "why": meta.get("why"),
        "asOfDate": as_of,
        "close": last_close,
        "pctChg": pct,
        "ma5": ma5,
        "ma20": ma20,
        "source": str(latest.get("source") or "") if latest else None,
        "underlyingTsCode": str(latest.get("underlying_ts_code") or "") if latest else None,
        "realtime": False,
        "tradeTime": None,
        "quotePrice": None,
        "quotePctChg": None,
    }


def build_macro_snapshot() -> dict[str, Any]:
    ensure_table()
    # Skip full-market breadth (very slow); Index page needs a fast response.
    cn_index_signals = get_index_signals(include_breadth=False)
    macro_items: list[dict[str, Any]] = []
    for meta in MACRO_CARDS:
        sid = str(meta["seriesId"])
        closes = fetch_last_closes(sid, days=80)
        if not closes:
            macro_items.append(
                {
                    "seriesId": sid,
                    "name": meta["name"],
                    "category": meta["category"],
                    "why": meta.get("why"),
                    "asOfDate": None,
                    "close": None,
                    "pctChg": None,
                    "ma5": None,
                    "ma20": None,
                    "source": None,
                    "underlyingTsCode": None,
                    "realtime": False,
                    "tradeTime": None,
                    "quotePrice": None,
                    "quotePctChg": None,
                }
            )
            continue
        item = _macro_item_from_db(meta, closes)
        macro_items.append(item)

    # Realtime overlay (best-effort; unsupported codes return empty)
    rt_codes: list[str] = []
    code_to_series: dict[str, str] = {}
    for meta in MACRO_CARDS:
        sid = str(meta["seriesId"])
        explicit = meta.get("realtimeTsCode")
        if explicit:
            c = str(explicit).strip()
            rt_codes.append(c)
            code_to_series[c] = sid
            continue
        latest = get_latest_row(sid)
        und = latest.get("underlying_ts_code") if latest else None
        if und and str(und).strip():
            c = str(und).strip()
            rt_codes.append(c)
            code_to_series[c] = sid

    rt_codes = sorted(set(rt_codes))
    if rt_codes:
        res = fetch_realtime_quotes(rt_codes)
        if isinstance(res, dict) and res.get("ok") and isinstance(res.get("items"), list):
            for it in res["items"]:
                if not isinstance(it, dict):
                    continue
                ts_c = str(it.get("ts_code") or "").strip()
                if not ts_c or ts_c not in code_to_series:
                    continue
                sid = code_to_series[ts_c]
                price = _safe_float(it.get("price"))
                pct = _safe_float(it.get("pct_chg"))
                if pct is None and price is not None:
                    pre = _safe_float(it.get("pre_close"))
                    if pre is not None and pre > 0:
                        pct = (price - pre) / pre * 100.0
                tt = it.get("trade_time")
                for m in macro_items:
                    if m.get("seriesId") == sid:
                        m["realtime"] = True
                        m["tradeTime"] = str(tt) if tt else None
                        m["quotePrice"] = price
                        m["quotePctChg"] = pct
                        if price is not None:
                            m["close"] = price
                        if pct is not None:
                            m["pctChg"] = pct
                        break

    # DB empty + realtime_quote often has no US/FX ticks: query Tushare daily on demand (no DB write).
    macro_items = enrich_macro_items_on_demand(macro_items)

    out: dict[str, Any] = {"cnIndexSignals": cn_index_signals, "macro": macro_items}
    warn = macro_snapshot_warning()
    if warn:
        out["warning"] = warn
    return out
