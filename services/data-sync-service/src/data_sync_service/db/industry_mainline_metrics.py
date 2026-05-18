from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

TABLE_NAME = "market_cn_industry_mainline_metrics_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date            TEXT NOT NULL,
    industry_name   TEXT NOT NULL,
    total_count     INTEGER NOT NULL,
    limit_up_count  INTEGER NOT NULL,
    limit_up_2d_count INTEGER NOT NULL,
    surge_count     INTEGER NOT NULL,
    surge_ratio     DOUBLE PRECISION NOT NULL,
    avg_close       DOUBLE PRECISION NOT NULL,
    avg_pct         DOUBLE PRECISION NOT NULL,
    updated_at      TEXT NOT NULL,
    raw_json        JSONB NOT NULL,
    PRIMARY KEY(date, industry_name)
);

CREATE INDEX IF NOT EXISTS idx_mainline_metrics_date ON {TABLE_NAME}(date DESC);
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
                str(r.get("industry_name") or ""),
                int(r.get("total_count") or 0),
                int(r.get("limit_up_count") or 0),
                int(r.get("limit_up_2d_count") or 0),
                int(r.get("surge_count") or 0),
                float(r.get("surge_ratio") or 0.0),
                float(r.get("avg_close") or 0.0),
                float(r.get("avg_pct") or 0.0),
                str(r.get("updated_at") or ""),
                Json(r.get("raw") if isinstance(r.get("raw"), dict) else {"raw": r.get("raw")}),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (
                    date, industry_name, total_count, limit_up_count, limit_up_2d_count,
                    surge_count, surge_ratio, avg_close, avg_pct, updated_at, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(date, industry_name) DO UPDATE SET
                    total_count = excluded.total_count,
                    limit_up_count = excluded.limit_up_count,
                    limit_up_2d_count = excluded.limit_up_2d_count,
                    surge_count = excluded.surge_count,
                    surge_ratio = excluded.surge_ratio,
                    avg_close = excluded.avg_close,
                    avg_pct = excluded.avg_pct,
                    updated_at = excluded.updated_at,
                    raw_json = excluded.raw_json
                """,
                values,
            )
        conn.commit()
    return len(values)


def list_rows_by_date(as_of_date: str) -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    industry_name, total_count, limit_up_count, limit_up_2d_count,
                    surge_count, surge_ratio, avg_close, avg_pct, updated_at, raw_json
                FROM {TABLE_NAME}
                WHERE date = %s
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        raw = r[9] if isinstance(r[9], dict) else json.loads(str(r[9]) or "{}")
        out.append(
            {
                "industry_name": str(r[0]),
                "total_count": int(r[1] or 0),
                "limit_up_count": int(r[2] or 0),
                "limit_up_2d_count": int(r[3] or 0),
                "surge_count": int(r[4] or 0),
                "surge_ratio": float(r[5] or 0.0),
                "avg_close": float(r[6] or 0.0),
                "avg_pct": float(r[7] or 0.0),
                "updated_at": str(r[8] or ""),
                "raw": raw,
            }
        )
    return out


def list_rows_for_dates(dates: list[str]) -> list[dict[str, Any]]:
    ensure_table()
    if not dates:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    date, industry_name, total_count, limit_up_count, limit_up_2d_count,
                    surge_count, surge_ratio, avg_close, avg_pct, updated_at, raw_json
                FROM {TABLE_NAME}
                WHERE date = ANY(%s)
                """,
                (dates,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        raw = r[10] if isinstance(r[10], dict) else json.loads(str(r[10]) or "{}")
        out.append(
            {
                "date": str(r[0]),
                "industry_name": str(r[1]),
                "total_count": int(r[2] or 0),
                "limit_up_count": int(r[3] or 0),
                "limit_up_2d_count": int(r[4] or 0),
                "surge_count": int(r[5] or 0),
                "surge_ratio": float(r[6] or 0.0),
                "avg_close": float(r[7] or 0.0),
                "avg_pct": float(r[8] or 0.0),
                "updated_at": str(r[9] or ""),
                "raw": raw,
            }
        )
    return out


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
