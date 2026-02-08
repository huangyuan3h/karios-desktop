"""Daily K-line table: schema, upsert from tushare, get last trade date, fetch for API."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from data_sync_service.db import get_connection

TABLE_NAME = "daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code    TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open       NUMERIC,
    high       NUMERIC,
    low        NUMERIC,
    close      NUMERIC,
    pre_close  NUMERIC,
    change     NUMERIC,
    pct_chg    NUMERIC,
    vol        NUMERIC,
    amount     NUMERIC,
    adj_factor NUMERIC,
    PRIMARY KEY (ts_code, trade_date)
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    pre_close = EXCLUDED.pre_close,
    change = EXCLUDED.change,
    pct_chg = EXCLUDED.pct_chg,
    vol = EXCLUDED.vol,
    amount = EXCLUDED.amount;
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            # Backfill schema change for existing databases.
            cur.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS adj_factor NUMERIC")
        conn.commit()


def _numeric(val: object) -> float | None:
    if pd.isna(val) or val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _scalar(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    return str(val).strip() or None


def _date_str(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def get_last_trade_date(ts_code: str) -> date | None:
    """Return the latest trade_date for ts_code in DB, or None."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {TABLE_NAME} WHERE ts_code = %s",
                (ts_code,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_last_adj_factor_date(ts_code: str) -> date | None:
    """Return the latest trade_date with non-null adj_factor for ts_code, or None."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {TABLE_NAME} WHERE ts_code = %s AND adj_factor IS NOT NULL",
                (ts_code,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def upsert_from_dataframe(df: pd.DataFrame) -> int:
    """Upsert daily bars from tushare DataFrame. Returns number of rows upserted."""
    ensure_table()
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _scalar(row.get("ts_code")),
            _date_str(row.get("trade_date")),
            _numeric(row.get("open")),
            _numeric(row.get("high")),
            _numeric(row.get("low")),
            _numeric(row.get("close")),
            _numeric(row.get("pre_close")),
            _numeric(row.get("change")),
            _numeric(row.get("pct_chg")),
            _numeric(row.get("vol")),
            _numeric(row.get("amount")),
        ))
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
        conn.commit()
    return len(rows)


def fetch_daily(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return daily bars from DB with optional filters. Dates as YYYY-MM-DD."""
    ensure_table()
    conditions = []
    params = []
    if ts_code:
        conditions.append("ts_code = %s")
        params.append(ts_code)
    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)
    where_sql = " WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount, adj_factor
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY ts_code, trade_date
                LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    out = []
    for row in rows:
        obj = {}
        for col, val in zip(columns, row):
            if val is None:
                obj[col] = None
            elif hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            elif hasattr(val, "__float__") and col != "ts_code":
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        out.append(obj)
    return out


def update_adj_factor_from_dataframe(df: pd.DataFrame) -> int:
    """
    Update daily.adj_factor from a DataFrame with columns: ts_code, trade_date, adj_factor.
    trade_date may be YYYYMMDD or YYYY-MM-DD; we normalize to YYYY-MM-DD.
    Returns number of input rows processed (best-effort; DB update count may differ).
    """
    ensure_table()
    rows = []
    for _, row in df.iterrows():
        ts_code = _scalar(row.get("ts_code"))
        trade_date = _date_str(row.get("trade_date"))
        adj_factor = _numeric(row.get("adj_factor"))
        if not ts_code or not trade_date:
            continue
        rows.append((adj_factor, ts_code, trade_date))

    if not rows:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"UPDATE {TABLE_NAME} SET adj_factor = %s WHERE ts_code = %s AND trade_date = %s",
                rows,
            )
        conn.commit()
    return len(rows)


def fetch_last_bars(ts_code: str, days: int = 60) -> list[dict[str, Any]]:
    """
    Return last N daily bars for a single ts_code, ordered by date ASC.
    Fields: date, open, high, low, close, volume, amount (all strings).
    """
    ensure_table()
    days2 = max(1, min(int(days), 400))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, open, high, low, close, vol, amount
                FROM {TABLE_NAME}
                WHERE ts_code = %s
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (ts_code, days2),
            )
            rows = cur.fetchall()
    # Reverse to ASC
    out: list[dict[str, Any]] = []
    for r in reversed(rows):
        d = r[0].strftime("%Y-%m-%d") if r and hasattr(r[0], "strftime") else str(r[0])
        out.append(
            {
                "date": d,
                "open": str(r[1] or ""),
                "high": str(r[2] or ""),
                "low": str(r[3] or ""),
                "close": str(r[4] or ""),
                "volume": str(r[5] or ""),
                "amount": str(r[6] or ""),
            }
        )
    return out
