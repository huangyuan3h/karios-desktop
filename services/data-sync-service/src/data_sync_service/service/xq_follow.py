"""Daily Snowball (雪球) follow-count snapshot service (P0-13 attention line)."""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import xq_follow

JOB_TYPE = "xq_follow_snapshot"
SH_TZ = ZoneInfo("Asia/Shanghai")


def _today() -> str:
    return datetime.now(SH_TZ).strftime("%Y-%m-%d")


def _retry(fn, tries: int = 3, base: float = 2.0):
    last: Exception | None = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            if i < tries - 1:
                time.sleep(base * (i + 1))
    raise last  # type: ignore[misc]


def _to_ts_code(sym: str) -> str | None:
    """'SH600519' -> '600519.SH'; only A-share 6xx.SH / 0xx.SZ / 3xx.SZ survive.

    Excludes SH 000xxx indices and BJ codes."""
    s = str(sym).strip().upper()
    if len(s) < 8 or s[:2] not in ("SH", "SZ"):
        return None
    code, mkt = s[2:], s[:2]
    if mkt == "SH" and not code.startswith("6"):
        return None
    if mkt == "SZ" and not code.startswith(("0", "3")):
        return None
    return f"{code}.{mkt}"


def sync_xq_follow_snapshot(*, as_of: str | None = None) -> dict[str, Any]:
    """Fetch the current 雪球 follow cross-section and upsert one snapshot row set."""
    import akshare as ak  # type: ignore[import-not-found]

    try:
        df = _retry(lambda: ak.stock_hot_follow_xq(symbol="最热门"))
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"xq follow fetch failed: {str(e)[:160]}"}
    if df is None or getattr(df, "empty", True):
        return {"ok": False, "error": "xq follow returned empty"}

    trade_date = as_of or _today()
    rows: list[dict] = []
    for r in df.itertuples(index=False):
        ts = _to_ts_code(getattr(r, "股票代码", ""))
        if ts is None:
            continue
        rows.append({
            "ts_code": ts,
            "follow": getattr(r, "关注", None),
            "price": getattr(r, "最新价", None),
        })
    if not rows:
        return {"ok": False, "error": "xq follow: no mappable A-share rows"}

    updated = xq_follow.upsert_rows(rows, trade_date)
    return {"ok": True, "updated": updated, "trade_date": trade_date}
