"""Index daily basic (market breadth indicators): schema, upsert, queries."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from data_sync_service.db import get_connection

TABLE_NAME = "index_dailybasic"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code       TEXT NOT NULL,
    trade_date    DATE NOT NULL,
    total_mv      NUMERIC,
    float_mv      NUMERIC,
    total_share   NUMERIC,
    float_share   NUMERIC,
    free_share    NUMERIC,
    turnover_rate NUMERIC,
    turnover_rate_f NUMERIC,
    pe            NUMERIC,
    pe_ttm        NUMERIC,
    pb            NUMERIC,
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS ix_index_basic_date ON {TABLE_NAME} (trade_date DESC);
"""

UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME} (
    ts_code, trade_date, total_mv, float_mv, total_share, float_share, free_share,
    turnover_rate, turnover_rate_f, pe, pe_ttm, pb
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    total_mv = EXCLUDED.total_mv,
    float_mv = EXCLUDED.float_mv,
    total_share = EXCLUDED.total_share,
    float_share = EXCLUDED.float_share,
    free_share = EXCLUDED.free_share,
    turnover_rate = EXCLUDED.turnover_rate,
    turnover_rate_f = EXCLUDED.turnover_rate_f,
    pe = EXCLUDED.pe,
    pe_ttm = EXCLUDED.pe_ttm,
    pb = EXCLUDED.pb;
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
    ensure_table()
    rows: list[tuple] = []
    for _, r in df.iterrows():
        td = _date_str(r.get("trade_date"))
        if not td:
            continue
        rows.append((
            str(r.get("ts_code") or "").strip(),
            td,
            _numeric(r.get("total_mv")),
            _numeric(r.get("float_mv")),
            _numeric(r.get("total_share")),
            _numeric(r.get("float_share")),
            _numeric(r.get("free_share")),
            _numeric(r.get("turnover_rate")),
            _numeric(r.get("turnover_rate_f")),
            _numeric(r.get("pe")),
            _numeric(r.get("pe_ttm")),
            _numeric(r.get("pb")),
        ))
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(UPSERT_SQL, row)
        conn.commit()
    return len(rows)


def get_last_trade_date(ts_code: str) -> date | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT MAX(trade_date) FROM {TABLE_NAME} WHERE ts_code = %s",
                (ts_code,),
            )
            row = cur.fetchone()
    return row[0] if row and row[0] else None


def fetch_index_basic(
    ts_code: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    ensure_table()
    conditions: list[str] = []
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
                SELECT ts_code, trade_date, total_mv, float_mv, total_share, float_share,
                       free_share, turnover_rate, turnover_rate_f, pe, pe_ttm, pb
                FROM {TABLE_NAME}
                {where_sql}
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                params,
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    out: list[dict[str, Any]] = []
    for row in reversed(rows):
        obj: dict[str, Any] = {}
        for col, val in zip(cols, row):
            if val is None:
                obj[col] = None
            elif hasattr(val, "strftime") and col == "trade_date":
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


def fetch_last_float_mv_turnover(ts_code: str, days: int = 80) -> list[tuple[str, float, float]]:
    """Return last N (date, float_mv, turnover_rate) ordered by date ASC."""
    ensure_table()
    days2 = max(1, min(int(days), 400))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, float_mv, turnover_rate
                FROM {TABLE_NAME}
                WHERE ts_code = %s AND float_mv IS NOT NULL
                ORDER BY trade_date DESC
                LIMIT %s
                """,
                (ts_code, days2),
            )
            rows = cur.fetchall()
    out: list[tuple[str, float, float]] = []
    for r in reversed(rows):
        d = r[0].strftime("%Y-%m-%d") if r and hasattr(r[0], "strftime") else str(r[0])
        try:
            float_mv = float(r[1] or 0.0)
        except Exception:
            float_mv = 0.0
        try:
            turnover = float(r[2] or 0.0)
        except Exception:
            turnover = 0.0
        out.append((d, float_mv, turnover))
    return out