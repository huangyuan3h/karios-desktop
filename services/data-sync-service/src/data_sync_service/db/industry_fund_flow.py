from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

TABLE_NAME = "market_cn_industry_fund_flow_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date          TEXT NOT NULL,
    industry_code TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    net_inflow    DOUBLE PRECISION NOT NULL,
    updated_at    TEXT NOT NULL,
    raw_json      JSONB NOT NULL,
    PRIMARY KEY(date, industry_code)
);

CREATE INDEX IF NOT EXISTS idx_cn_industry_fund_flow_date ON {TABLE_NAME}(date DESC);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def upsert_daily_rows(rows: Iterable[dict[str, Any]]) -> int:
    ensure_table()
    rows_list = [r for r in rows if r]
    if not rows_list:
        return 0
    values = []
    for r in rows_list:
        values.append(
            (
                str(r.get("date") or ""),
                str(r.get("industry_code") or ""),
                str(r.get("industry_name") or ""),
                float(r.get("net_inflow") or 0.0),
                str(r.get("updated_at") or ""),
                Json(r.get("raw") if isinstance(r.get("raw"), dict) else {"raw": r.get("raw")}),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(
                    date, industry_code, industry_name, net_inflow, updated_at, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(date, industry_code) DO UPDATE SET
                    industry_name = excluded.industry_name,
                    net_inflow = excluded.net_inflow,
                    updated_at = excluded.updated_at,
                    raw_json = excluded.raw_json
                """,
                values,
            )
        conn.commit()
    return len(values)


def get_latest_date() -> str | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
            row = cur.fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])


def get_dates_upto(as_of_date: str, days: int) -> list[str]:
    ensure_table()
    lim = max(1, min(int(days), 60))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT date
                FROM {TABLE_NAME}
                WHERE date <= %s
                ORDER BY date DESC
                LIMIT %s
                """,
                (as_of_date, lim),
            )
            rows = cur.fetchall()
    dates = [str(r[0]) for r in rows if r and r[0]]
    return list(reversed(dates))


def get_top_rows(as_of_date: str, top_n: int) -> list[dict[str, Any]]:
    ensure_table()
    lim = max(1, min(int(top_n), 300))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT industry_code, industry_name, net_inflow
                FROM {TABLE_NAME}
                WHERE date = %s
                ORDER BY net_inflow DESC
                LIMIT %s
                """,
                (as_of_date, lim),
            )
            rows = cur.fetchall()
    return [
        {
            "industry_code": str(r[0]),
            "industry_name": str(r[1]),
            "net_inflow": float(r[2] or 0.0),
        }
        for r in rows
    ]


def get_rows_by_date(as_of_date: str) -> list[dict[str, Any]]:
    """Return all industry flow rows for a given date."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT industry_code, industry_name, net_inflow
                FROM {TABLE_NAME}
                WHERE date = %s
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
    return [
        {
            "industry_code": str(r[0]),
            "industry_name": str(r[1]),
            "net_inflow": float(r[2] or 0.0),
        }
        for r in rows
    ]


def get_sum_by_industry_for_dates(dates: list[str]) -> list[dict[str, Any]]:
    """Return per-industry sum of net_inflow for given dates."""
    ensure_table()
    if not dates:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT industry_name, SUM(net_inflow) AS sum_inflow
                FROM {TABLE_NAME}
                WHERE date = ANY(%s)
                GROUP BY industry_name
                ORDER BY sum_inflow DESC
                """,
                (dates,),
            )
            rows = cur.fetchall()
    return [
        {
            "industry_name": str(r[0]),
            "sum_inflow": float(r[1] or 0.0),
        }
        for r in rows
    ]


def get_series_for_industry(*, industry_name: str, dates: list[str]) -> list[dict[str, Any]]:
    ensure_table()
    if not dates:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, net_inflow
                FROM {TABLE_NAME}
                WHERE industry_name = %s AND date = ANY(%s)
                ORDER BY date ASC
                """,
                (industry_name, dates),
            )
            rows = cur.fetchall()
    return [{"date": str(r[0]), "net_inflow": float(r[1] or 0.0)} for r in rows]


def export_all_rows() -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, industry_code, industry_name, net_inflow, updated_at, raw_json
                FROM {TABLE_NAME}
                """
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        raw = r[5] if isinstance(r[5], dict) else json.loads(str(r[5]) or "{}")
        out.append(
            {
                "date": str(r[0]),
                "industry_code": str(r[1]),
                "industry_name": str(r[2]),
                "net_inflow": float(r[3] or 0.0),
                "updated_at": str(r[4]),
                "raw": raw,
            }
        )
    return out
