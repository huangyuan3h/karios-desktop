"""Stock basic list table: schema and upsert from tushare DataFrame."""

from __future__ import annotations

import pandas as pd

from data_sync_service.db import get_connection

TABLE_NAME = "stock_basic"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code     TEXT PRIMARY KEY,
    symbol      TEXT NOT NULL,
    name        TEXT,
    industry    TEXT,
    market      TEXT,
    list_date   DATE,
    delist_date DATE
);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (ts_code, symbol, name, industry, market, list_date, delist_date)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    name = EXCLUDED.name,
    industry = EXCLUDED.industry,
    market = EXCLUDED.market,
    list_date = EXCLUDED.list_date,
    delist_date = EXCLUDED.delist_date;
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _scalar(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    return str(val).strip() or None


def _date(val: object) -> str | None:
    if pd.isna(val) or val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val).strip() or None


def upsert_from_dataframe(df: pd.DataFrame) -> int:
    """Upsert rows from tushare stock_basic DataFrame. Returns number of rows upserted."""
    ensure_table()
    rows = []
    for _, row in df.iterrows():
        rows.append((
            _scalar(row.get("ts_code")),
            _scalar(row.get("symbol")),
            _scalar(row.get("name")),
            _scalar(row.get("industry")),
            _scalar(row.get("market")),
            _date(row.get("list_date")),
            _date(row.get("delist_date")),
        ))
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
        conn.commit()
    return len(rows)


def fetch_all() -> list[dict]:
    """Return all stock_basic rows from DB as list of dicts. Dates as YYYY-MM-DD strings."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, symbol, name, industry, market, list_date, delist_date FROM {TABLE_NAME} ORDER BY ts_code"
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    out = []
    for row in rows:
        obj = {}
        for col, val in zip(columns, row):
            if hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            else:
                obj[col] = val
        out.append(obj)
    return out
