"""Sync stock basic list from tushare to database."""

from __future__ import annotations

from typing import Any

import pandas as pd
import tushare as ts

from data_sync_service.config import get_settings
from data_sync_service.db.stock_basic import upsert_from_dataframe

FIELDS = [
    "ts_code",
    "symbol",
    "name",
    "industry",
    "market",
    "list_date",
    "delist_date",
]


def sync_stock_basic() -> dict[str, Any]:
    """
    Fetch stock_basic from tushare and upsert into database.
    Returns {"ok": True, "updated": n} or {"ok": False, "error": "..."}.
    """
    settings = get_settings()
    if not settings.tu_share_api_key:
        return {"ok": False, "error": "TU_SHARE_API_KEY is not set"}

    try:
        pro = ts.pro_api(settings.tu_share_api_key)
        df: pd.DataFrame = pro.stock_basic(fields=",".join(FIELDS))
        if df is None or df.empty:
            return {"ok": True, "updated": 0, "message": "no data from tushare"}

        n = upsert_from_dataframe(df)
        return {"ok": True, "updated": n}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)}
