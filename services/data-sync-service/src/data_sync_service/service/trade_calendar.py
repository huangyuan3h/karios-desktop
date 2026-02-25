"""Manual trade calendar sync from tushare into DB."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.trade_calendar import summary as cal_summary
from data_sync_service.db.trade_calendar import upsert_from_dataframe

FIELDS = ["exchange", "cal_date", "is_open", "pretrade_date"]


def _today_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def sync_trade_calendar(
    exchange: str = "SSE",
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """
    Sync trade calendar into DB. This is intended to be called manually.
    """
    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    if not start_date:
        # default: 1 year back
        start_date = (datetime.now(timezone.utc).date() - timedelta(days=365)).strftime("%Y%m%d")
    if not end_date:
        end_date = _today_yyyymmdd()

    pro = ts.pro_api(settings.tu_share_api_key)
    # trade_cal usually fits in one page, but keep pagination for robustness
    limit = 5000
    offset = 0
    total_rows = 0
    while True:
        df: pd.DataFrame = pro.trade_cal(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
            fields=",".join(FIELDS),
        )
        if df is None or df.empty:
            break
        total_rows += upsert_from_dataframe(df)
        if len(df) < limit:
            break
        offset += limit

    s = cal_summary(
        exchange=exchange,
        start_date=date.fromisoformat(f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"),
        end_date=date.fromisoformat(f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"),
    )
    return {"ok": True, "updated": total_rows, "summary": s}

