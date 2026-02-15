from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

TABLE_NAME = "market_cn_industry_mainline_scores_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date          TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    flow_score    DOUBLE PRECISION NOT NULL,
    breadth_score DOUBLE PRECISION NOT NULL,
    trend_score   DOUBLE PRECISION NOT NULL,
    total_score   DOUBLE PRECISION NOT NULL,
    updated_at    TEXT NOT NULL,
    flags_json    JSONB NOT NULL,
    PRIMARY KEY(date, industry_name)
);

CREATE INDEX IF NOT EXISTS idx_mainline_scores_date ON {TABLE_NAME}(date DESC);
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
                float(r.get("flow_score") or 0.0),
                float(r.get("breadth_score") or 0.0),
                float(r.get("trend_score") or 0.0),
                float(r.get("total_score") or 0.0),
                str(r.get("updated_at") or ""),
                Json(r.get("flags") if isinstance(r.get("flags"), dict) else {"flags": r.get("flags")}),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (
                    date, industry_name, flow_score, breadth_score, trend_score,
                    total_score, updated_at, flags_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(date, industry_name) DO UPDATE SET
                    flow_score = excluded.flow_score,
                    breadth_score = excluded.breadth_score,
                    trend_score = excluded.trend_score,
                    total_score = excluded.total_score,
                    updated_at = excluded.updated_at,
                    flags_json = excluded.flags_json
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
                SELECT industry_name, flow_score, breadth_score, trend_score,
                       total_score, updated_at, flags_json
                FROM {TABLE_NAME}
                WHERE date = %s
                """,
                (as_of_date,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        flags = r[6] if isinstance(r[6], dict) else json.loads(str(r[6]) or "{}")
        out.append(
            {
                "industry_name": str(r[0]),
                "flow_score": float(r[1] or 0.0),
                "breadth_score": float(r[2] or 0.0),
                "trend_score": float(r[3] or 0.0),
                "total_score": float(r[4] or 0.0),
                "updated_at": str(r[5] or ""),
                "flags": flags,
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
                SELECT date, industry_name, flow_score, breadth_score, trend_score,
                       total_score, updated_at, flags_json
                FROM {TABLE_NAME}
                WHERE date = ANY(%s)
                """,
                (dates,),
            )
            rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        flags = r[7] if isinstance(r[7], dict) else json.loads(str(r[7]) or "{}")
        out.append(
            {
                "date": str(r[0]),
                "industry_name": str(r[1]),
                "flow_score": float(r[2] or 0.0),
                "breadth_score": float(r[3] or 0.0),
                "trend_score": float(r[4] or 0.0),
                "total_score": float(r[5] or 0.0),
                "updated_at": str(r[6] or ""),
                "flags": flags,
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
