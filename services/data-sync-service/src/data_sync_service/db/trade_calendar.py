"""Trade calendar table: schema and helpers."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pandas as pd

from data_sync_service.db import get_connection

TABLE_NAME = "trade_calendar"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    exchange       TEXT NOT NULL,
    cal_date       DATE NOT NULL,
    is_open        SMALLINT NOT NULL,
    pretrade_date  DATE,
    PRIMARY KEY (exchange, cal_date)
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (exchange, cal_date, is_open, pretrade_date)
VALUES (%s, %s, %s, %s)
ON CONFLICT (exchange, cal_date) DO UPDATE SET
    is_open = EXCLUDED.is_open,
    pretrade_date = EXCLUDED.pretrade_date;
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _date_str(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def upsert_from_dataframe(df: pd.DataFrame) -> int:
    """Upsert rows from tushare trade_cal DataFrame."""
    ensure_table()
    rows = []
    for _, row in df.iterrows():
        exchange = str(row.get("exchange") or "").strip() or "SSE"
        cal_date = _date_str(row.get("cal_date"))
        pretrade_date = _date_str(row.get("pretrade_date"))
        is_open = int(row.get("is_open") or 0)
        if not cal_date:
            continue
        rows.append((exchange, cal_date, is_open, pretrade_date))

    if not rows:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def is_trading_day(exchange: str, cal_date: date) -> bool | None:
    """
    Return True/False if calendar row exists; None if not found.
    """
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT is_open FROM {TABLE_NAME} WHERE exchange = %s AND cal_date = %s",
                (exchange, cal_date),
            )
            row = cur.fetchone()
    if not row:
        return None
    return int(row[0]) == 1


def get_open_dates(exchange: str, start_date: date, end_date: date) -> list[date]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT cal_date
                FROM {TABLE_NAME}
                WHERE exchange = %s
                  AND cal_date >= %s AND cal_date <= %s
                  AND is_open = 1
                ORDER BY cal_date
                """,
                (exchange, start_date, end_date),
            )
            rows = cur.fetchall()
    return [r[0] for r in rows if r and r[0]]


def get_latest_calendar_date(exchange: str) -> date | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(cal_date) FROM {TABLE_NAME} WHERE exchange = %s",
                (exchange,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def summary(exchange: str, start_date: date, end_date: date) -> dict[str, Any]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*), SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END)
                FROM {TABLE_NAME}
                WHERE exchange = %s AND cal_date >= %s AND cal_date <= %s
                """,
                (exchange, start_date, end_date),
            )
            row = cur.fetchone()
    total = int(row[0] or 0) if row else 0
    open_days = int(row[1] or 0) if row else 0
    return {"exchange": exchange, "start_date": start_date.isoformat(), "end_date": end_date.isoformat(), "rows": total, "open_days": open_days}

