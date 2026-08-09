"""Stock daily valuation (total_mv / circ_mv / turnover_rate) — §19.2 step 12.

Market-cap layering for the S-3 universe: split candidates by market cap and
enforce a liquidity floor without touching tradability. Fed from tushare
``daily_basic`` (per trade date, whole market), stored per (ts_code, date).
"""

from __future__ import annotations

import logging

import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_dailybasic"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code       TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    total_mv      DOUBLE PRECISION,
    circ_mv       DOUBLE PRECISION,
    turnover_rate DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
)
"""


def ensure_table() -> None:
    from data_sync_service.db.daily import ensure_once

    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def sync_daily_basic_for_date(trade_date: str) -> int:
    """Fetch one trade date's valuation rows for the whole market (best-effort)."""
    try:
        pro = ts.pro_api(get_settings().tu_share_api_key)
    except Exception:  # noqa: BLE001
        ts.set_token(get_settings().tu_share_api_key)
        pro = ts.pro_api()
    df = pro.daily_basic(
        trade_date=trade_date.replace("-", ""),
        fields="ts_code,total_mv,circ_mv,turnover_rate",
    )
    if df is None or df.empty:
        return 0
    ensure_table()
    rows = [
        (str(r.ts_code), trade_date, float(r.total_mv), float(r.circ_mv), float(r.turnover_rate))
        for r in df.itertuples()
        if r.ts_code
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (ts_code, trade_date, total_mv, circ_mv, turnover_rate)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date) DO NOTHING
                """,
                rows,
            )
        conn.commit()
    return len(rows)


def market_cap_by_date(trade_date: str) -> dict[str, float]:
    """{ts_code: total_mv} for one trade date (10k CNY)."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, total_mv FROM {TABLE_NAME} WHERE trade_date = %s AND total_mv IS NOT NULL",
                (trade_date,),
            )
            return {str(r[0]): float(r[1]) for r in cur.fetchall()}
