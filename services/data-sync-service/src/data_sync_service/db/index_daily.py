"""Index daily table: schema, upsert from tushare, get last trade date, fetch for API."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.db import get_connection

TABLE_NAME = "index_daily"

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
        conn.commit()


def _numeric(val: Any) -> float | None:
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


def upsert_from_dataframe(df: pd.DataFrame) -> int:
    """Upsert index daily bars from tushare DataFrame. Returns number of rows upserted."""
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
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
        conn.commit()
    return len(rows)


def fetch_index_daily(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return index daily bars from DB with optional filters. Dates as YYYY-MM-DD."""
    ensure_table()
    conditions = []
    params: list[object] = []
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
                SELECT ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY ts_code, trade_date
                LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    out: list[dict[str, Any]] = []
    for row in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(columns, row):
            if val is None:
                obj[col] = None
            elif hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            elif hasattr(val, "__float__") and col not in ("ts_code", "trade_date"):
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        out.append(obj)
    return out


def fetch_last_closes(ts_code: str, days: int = 60) -> list[tuple[str, float]]:
    """
    Return last N (date, close) rows for a single index, ordered by date ASC.
    """
    ensure_table()
    days2 = max(1, min(int(days), 400))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, close
                FROM {TABLE_NAME}
                WHERE ts_code = %s
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (ts_code, days2),
            )
            rows = cur.fetchall()
    out: list[tuple[str, float]] = []
    for r in reversed(rows):
        d = r[0].strftime("%Y-%m-%d") if r and hasattr(r[0], "strftime") else str(r[0])
        try:
            close = float(r[1] or 0.0)
        except Exception:
            close = 0.0
        out.append((d, close))
    return out
