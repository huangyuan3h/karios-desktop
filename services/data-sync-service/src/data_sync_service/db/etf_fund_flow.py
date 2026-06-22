"""ETF fund-flow daily table: share, price, and realtime flow snapshots."""

from __future__ import annotations

from datetime import date
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "market_etf_fund_flow_daily"
FLOW_COLUMNS = [
    "source",
    "trade_time",
    "main_net_inflow",
    "super_large_net_inflow",
    "large_net_inflow",
    "medium_net_inflow",
    "small_net_inflow",
]
NUMERIC_COLUMNS = {
    "fd_share",
    "close",
    "avg_price",
    "net_inflow",
    "main_net_inflow",
    "super_large_net_inflow",
    "large_net_inflow",
    "medium_net_inflow",
    "small_net_inflow",
}
TEXT_COLUMNS = {"ts_code", "updated_at", "source", "trade_time"}
SELECT_COLUMNS = [
    "ts_code",
    "trade_date",
    "fd_share",
    "close",
    "avg_price",
    "net_inflow",
    "updated_at",
    *FLOW_COLUMNS,
]

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code                  TEXT NOT NULL,
    trade_date               DATE NOT NULL,
    fd_share                 DOUBLE PRECISION,
    close                    DOUBLE PRECISION,
    avg_price                DOUBLE PRECISION,
    net_inflow               DOUBLE PRECISION,
    updated_at               TEXT NOT NULL,
    source                   TEXT,
    trade_time               TEXT,
    main_net_inflow          DOUBLE PRECISION,
    super_large_net_inflow   DOUBLE PRECISION,
    large_net_inflow         DOUBLE PRECISION,
    medium_net_inflow        DOUBLE PRECISION,
    small_net_inflow         DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_etf_fund_flow_date ON {TABLE_NAME}(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_etf_fund_flow_source ON {TABLE_NAME}(source);
"""


def _ensure_table_impl() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def ensure_table() -> None:
    ensure_once("market_etf_fund_flow_daily", _ensure_table_impl)


def _date_str(val: object) -> str | None:
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or None


def _value_for_column(row: dict[str, Any], column: str) -> Any:
    if column == "trade_date":
        return _date_str(row.get(column))
    if column == "source":
        return row.get("source")
    if column == "trade_time":
        return row.get("trade_time") or row.get("tradeTime")
    if column == "main_net_inflow":
        return row.get("main_net_inflow", row.get("mainNetInflow"))
    if column == "super_large_net_inflow":
        return row.get("super_large_net_inflow", row.get("superLargeNetInflow"))
    if column == "large_net_inflow":
        return row.get("large_net_inflow", row.get("largeNetInflow"))
    if column == "medium_net_inflow":
        return row.get("medium_net_inflow", row.get("mediumNetInflow"))
    if column == "small_net_inflow":
        return row.get("small_net_inflow", row.get("smallNetInflow"))
    return row.get(column)


def _row_to_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    obj: dict[str, Any] = {}
    for col, val in zip(columns, row):
        if val is None:
            obj[col] = None
        elif col == "trade_date" and hasattr(val, "strftime"):
            obj[col] = val.strftime("%Y-%m-%d")
        elif col in NUMERIC_COLUMNS:
            try:
                obj[col] = float(val)
            except (TypeError, ValueError):
                obj[col] = val
        elif col in TEXT_COLUMNS:
            obj[col] = val
        else:
            obj[col] = val
    return obj


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


def get_latest_date() -> str | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(trade_date) FROM {TABLE_NAME}")
            row = cur.fetchone()
    if not row or not row[0]:
        return None
    d = row[0]
    return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)


def upsert_daily_rows(rows: list[dict[str, Any]]) -> int:
    ensure_table()
    if not rows:
        return 0
    columns = SELECT_COLUMNS
    values = []
    for r in rows:
        td = _date_str(r.get("trade_date"))
        if not td:
            continue
        values.append(tuple(_value_for_column(r, col) for col in columns))
    if not values:
        return 0
    col_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_sql = ",\n                    ".join(
        f"{col} = excluded.{col}" for col in columns if col not in ("ts_code", "trade_date")
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}({col_sql})
                VALUES ({placeholders})
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    {update_sql}
                """,
                values,
            )
        conn.commit()
    return len(values)


def fetch_rows_for_codes(
    ts_codes: list[str],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    ensure_table()
    codes = [c.strip() for c in ts_codes if c and c.strip()]
    if not codes:
        return []
    conditions = ["ts_code = ANY(%s)"]
    params: list[object] = [codes]
    if start_date:
        conditions.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= %s")
        params.append(end_date)
    where_sql = " AND ".join(conditions)
    select_sql = ", ".join(SELECT_COLUMNS)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {select_sql}
                FROM {TABLE_NAME}
                WHERE {where_sql}
                ORDER BY ts_code, trade_date ASC
                """,
                tuple(params),
            )
            rows = cur.fetchall()
            columns = [d.name for d in cur.description]
    return [_row_to_dict(columns, row) for row in rows]


def fetch_row(ts_code: str, trade_date: str) -> dict[str, Any] | None:
    ensure_table()
    select_sql = ", ".join(SELECT_COLUMNS)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {select_sql}
                FROM {TABLE_NAME}
                WHERE ts_code = %s AND trade_date = %s
                """,
                (ts_code, trade_date),
            )
            row = cur.fetchone()
            if not row:
                return None
            columns = [d.name for d in cur.description]
    return _row_to_dict(columns, row)
