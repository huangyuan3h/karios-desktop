"""Sync East Money industry board membership (ts_code -> industry board name)."""

from __future__ import annotations

import json
import random
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db.stock_eastmoney_industry import count_rows, lookup_by_ts_codes, upsert_rows
from data_sync_service.db.stock_basic import ensure_table as ensure_stock_basic


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _ts_code_to_secid(ts_code: str) -> str | None:
    parts = (ts_code or "").strip().split(".")
    if len(parts) != 2:
        return None
    ticker, suffix = parts[0].strip(), parts[1].strip().upper()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    market = "1" if suffix == "SH" else "0"
    return f"{market}.{ticker}"


def _symbol_to_ts_code(symbol: str) -> str | None:
    s = (symbol or "").strip().upper()
    if not s.startswith("CN:"):
        return None
    ticker = s.split(":", 1)[1].strip()
    if len(ticker) != 6 or not ticker.isdigit():
        return None
    suffix = "SH" if ticker.startswith("6") else "SZ"
    return f"{ticker}.{suffix}"


def _fetch_em_industry_for_ts_code(ts_code: str) -> str | None:
    """
    Fetch East Money industry label for one A-share via push2 stock/get (field f127).
    """
    secid = _ts_code_to_secid(ts_code)
    if not secid:
        return None
    url = "https://push2.eastmoney.com/api/qt/stock/get"
    params = {
        "secid": secid,
        "fields": "f127",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "_": str(int(time.time() * 1000)),
    }
    req = urllib.request.Request(
        f"{url}?{urllib.parse.urlencode(params)}",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "close",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read()
    j = json.loads(raw.decode("utf-8", errors="replace"))
    data = j.get("data") if isinstance(j, dict) else None
    if not isinstance(data, dict):
        return None
    name = str(data.get("f127") or "").strip()
    return name or None


def fetch_em_industries_for_ts_codes(
    ts_codes: list[str],
    *,
    sleep_s: float = 0.05,
) -> dict[str, str]:
    """Return ts_code -> East Money industry label for codes that resolve."""
    out: dict[str, str] = {}
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    for i, code in enumerate(codes):
        try:
            name = _fetch_em_industry_for_ts_code(code)
        except Exception:
            name = None
        if name:
            out[code] = name
        if i + 1 < len(codes):
            time.sleep(max(0.0, float(sleep_s)) + random.random() * 0.02)
    return out


def _list_cn_ts_codes(*, limit: int | None = None) -> list[str]:
    ensure_stock_basic()
    from data_sync_service.db import get_connection

    lim = int(limit) if limit is not None and int(limit) > 0 else 100000
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code
                FROM stock_basic
                WHERE (market IN ('主板', '中小板', '创业板', '科创板', 'CN') OR ts_code ~ '^[0-9]{6}\\.(SH|SZ)$')
                  AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ')
                ORDER BY ts_code
                LIMIT %s
                """,
                (lim,),
            )
            rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def sync_eastmoney_industry(
    *,
    symbols: list[str] | None = None,
    limit: int | None = None,
    sleep_s: float = 0.05,
) -> dict[str, Any]:
    """
    Sync ts_code -> East Money industry labels.

    - symbols: optional CN:xxxxxx list (fast path for watchlist smoke tests)
    - limit: when symbols omitted, number of CN stocks from stock_basic to refresh
    """
    updated_at = _now_iso()
    ts_codes: list[str] = []
    if symbols:
        for sym in symbols:
            code = _symbol_to_ts_code(sym)
            if code:
                ts_codes.append(code)
    else:
        ts_codes = _list_cn_ts_codes(limit=limit if limit is not None else 200)

    if not ts_codes:
        return {"ok": False, "error": "no_ts_codes", "updated": 0}

    resolved = fetch_em_industries_for_ts_codes(ts_codes, sleep_s=sleep_s)
    rows = [
        {
            "ts_code": code,
            "industry_name": name,
            "industry_code": "",
            "updated_at": updated_at,
        }
        for code, name in resolved.items()
    ]
    updated = upsert_rows(rows)
    return {
        "ok": True,
        "requested": len(ts_codes),
        "resolved": len(resolved),
        "updated": updated,
        "totalInDb": count_rows(),
        "sample": [{"ts_code": k, "industry_name": v} for k, v in list(resolved.items())[:5]],
        "updatedAt": updated_at,
    }


def lookup_em_industries_for_ts_codes(ts_codes: list[str]) -> dict[str, str]:
    """DB lookup only; never HTTP. Use on TrendOK hot path."""
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return {}
    return lookup_by_ts_codes(codes)


def ensure_em_industries_for_ts_codes(ts_codes: list[str]) -> None:
    """Fetch and cache East Money industry labels for missing ts_codes only."""
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return
    existing = lookup_by_ts_codes(codes)
    missing = [c for c in codes if c not in existing]
    if not missing:
        return
    resolved = fetch_em_industries_for_ts_codes(missing, sleep_s=0.04)
    if not resolved:
        return
    updated_at = _now_iso()
    upsert_rows(
        [
            {
                "ts_code": code,
                "industry_name": name,
                "industry_code": "",
                "updated_at": updated_at,
            }
            for code, name in resolved.items()
        ]
    )
