"""Macro / global daily bars: schema, upsert, and queries for Index page."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd  # type: ignore[import-not-found, import-untyped]

from data_sync_service.db import get_connection

TABLE_NAME = "macro_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    series_id   TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    source      TEXT,
    underlying_ts_code TEXT,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC,
    pre_close   NUMERIC,
    change      NUMERIC,
    pct_chg     NUMERIC,
    vol         NUMERIC,
    amount      NUMERIC,
    PRIMARY KEY (series_id, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_macro_daily_series_date ON {TABLE_NAME} (series_id, trade_date DESC);
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


UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    series_id, trade_date, source, underlying_ts_code,
    open, high, low, close, pre_close, change, pct_chg, vol, amount
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (series_id, trade_date) DO UPDATE SET
    source = EXCLUDED.source,
    underlying_ts_code = EXCLUDED.underlying_ts_code,
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


def get_last_trade_date(series_id: str) -> date | None:
    """Return the latest trade_date for series_id in DB, or None."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {TABLE_NAME} WHERE series_id = %s",
                (series_id,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def upsert_from_dataframe(
    df: pd.DataFrame,
    *,
    series_id: str,
    source: str,
    underlying_ts_code: str | None = None,
) -> int:
    """
    Upsert rows from a tushare DataFrame. Expects normalized columns:
    trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
    (pct_chg may come from pct_change for us_daily).
    """
    ensure_table()
    rows: list[tuple[Any, ...]] = []
    for _, row in df.iterrows():
        td = _date_str(row.get("trade_date"))
        if not td:
            continue
        pct = _numeric(row.get("pct_chg"))
        if pct is None:
            pct = _numeric(row.get("pct_change"))
        close_v = _numeric(row.get("close"))
        if close_v is None:
            close_v = _numeric(row.get("settle"))
        rows.append(
            (
                series_id,
                td,
                source,
                underlying_ts_code,
                _numeric(row.get("open")),
                _numeric(row.get("high")),
                _numeric(row.get("low")),
                close_v,
                _numeric(row.get("pre_close")),
                _numeric(row.get("change")),
                pct,
                _numeric(row.get("vol")),
                _numeric(row.get("amount")),
            )
        )
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(UPSERT_SQL, r)
        conn.commit()
    return len(rows)


def fetch_macro_daily(
    series_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Return macro daily rows with optional filters. Dates as YYYY-MM-DD."""
    ensure_table()
    conditions: list[str] = []
    params: list[object] = []
    if series_id:
        conditions.append("series_id = %s")
        params.append(series_id)
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
                SELECT series_id, trade_date, source, underlying_ts_code,
                       open, high, low, close, pre_close, change, pct_chg, vol, amount
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY series_id, trade_date DESC
                LIMIT %s
                """,
                params,
            )
            raw = cur.fetchall()
            columns = [d.name for d in cur.description]
    out: list[dict[str, Any]] = []
    for row in reversed(raw):
        obj: dict[str, Any] = {}
        for col, val in zip(columns, row, strict=True):
            if val is None:
                obj[col] = None
            elif hasattr(val, "strftime") and col == "trade_date":
                obj[col] = val.strftime("%Y-%m-%d")
            elif hasattr(val, "__float__") and col not in ("series_id", "trade_date", "source", "underlying_ts_code"):
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        out.append(obj)
    return out


def fetch_last_closes(series_id: str, days: int = 80) -> list[tuple[str, float]]:
    """Return last N (date, close) ordered ASC for MA helpers."""
    ensure_table()
    days2 = max(1, min(int(days), 500))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, close
                FROM {TABLE_NAME}
                WHERE series_id = %s AND close IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (series_id, days2),
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


def get_latest_row(series_id: str) -> dict[str, Any] | None:
    """Latest non-null close row for snapshot and realtime mapping."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT series_id, trade_date, source, underlying_ts_code,
                       open, high, low, close, pre_close, change, pct_chg, vol, amount
                FROM {TABLE_NAME}
                WHERE series_id = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (series_id,),
            )
            row = cur.fetchone()
            columns = [d.name for d in cur.description] if cur.description else []
    if not row:
        return None
    obj: dict[str, Any] = {}
    for col, val in zip(columns, row, strict=True):
        if val is None:
            obj[col] = None
        elif hasattr(val, "strftime") and col == "trade_date":
            obj[col] = val.strftime("%Y-%m-%d")
        elif hasattr(val, "__float__") and col not in (
            "series_id",
            "trade_date",
            "source",
            "underlying_ts_code",
        ):
            try:
                obj[col] = float(val)
            except (TypeError, ValueError):
                obj[col] = val
        else:
            obj[col] = val
    return obj


def list_distinct_series_ids() -> list[str]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT series_id FROM {TABLE_NAME} ORDER BY series_id")
            rows = cur.fetchall()
    return [str(r[0]) for r in rows if r and r[0]]
