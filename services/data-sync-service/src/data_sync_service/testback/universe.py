from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

from data_sync_service.db.stock_basic import fetch_all


def _parse_date(date_str: str) -> datetime | None:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None


def build_universe(
    as_of_date: str,
    market: str | None = "CN",
    exclude_keywords: Iterable[str] | None = None,
    min_list_days: int = 0,
) -> list[str]:
    """
    Build a static universe using stock_basic fields.
    The result is a list of ts_code candidates before dynamic daily filtering.
    """
    rows = fetch_all()
    exclude = [k.strip() for k in (exclude_keywords or []) if k and k.strip()]
    as_of = _parse_date(as_of_date)
    min_days = max(0, int(min_list_days))
    out: list[str] = []
    for row in rows:
        ts_code = str(row.get("ts_code") or "").strip()
        if not ts_code:
            continue
        market_val = str(row.get("market") or "").strip().upper()
        if market and market_val not in ("CN", "主板", "中小板", "创业板", "科创板") and market.upper() == "CN":
            continue
        name = str(row.get("name") or "")
        if exclude and any(k in name for k in exclude):
            continue
        if as_of and min_days > 0:
            list_date = _parse_date(str(row.get("list_date") or ""))
            if list_date is None:
                continue
            if list_date > as_of - timedelta(days=min_days):
                continue
        out.append(ts_code)
    return out
