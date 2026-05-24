"""East Money industry board mapping: ts_code -> industry board name."""

from __future__ import annotations

from typing import Any, Iterable

from data_sync_service.db import get_connection

TABLE_NAME = "stock_eastmoney_industry"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code        TEXT PRIMARY KEY,
    industry_name  TEXT NOT NULL,
    industry_code  TEXT NOT NULL,
    updated_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stock_em_industry_name ON {TABLE_NAME}(industry_name);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def upsert_rows(rows: Iterable[dict[str, Any]]) -> int:
    ensure_table()
    values = []
    for r in rows:
        ts_code = str(r.get("ts_code") or "").strip()
        industry_name = str(r.get("industry_name") or "").strip()
        industry_code = str(r.get("industry_code") or "").strip()
        updated_at = str(r.get("updated_at") or "").strip()
        if not ts_code or not industry_name:
            continue
        values.append((ts_code, industry_name, industry_code, updated_at))
    if not values:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (ts_code, industry_name, industry_code, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (ts_code) DO UPDATE SET
                    industry_name = excluded.industry_name,
                    industry_code = excluded.industry_code,
                    updated_at = excluded.updated_at
                """,
                values,
            )
        conn.commit()
    return len(values)


def lookup_by_ts_codes(ts_codes: list[str]) -> dict[str, str]:
    """Return ts_code -> East Money industry board name."""
    ensure_table()
    codes = [str(c or "").strip() for c in ts_codes if c and str(c).strip()]
    if not codes:
        return {}
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT ts_code, industry_name FROM {TABLE_NAME} WHERE ts_code = ANY(%s)",
                    (codes,),
                )
                rows = cur.fetchall()
        return {str(r[0]): str(r[1]) for r in rows if r and r[0] and r[1]}
    except Exception:
        return {}


def count_rows() -> int:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            row = cur.fetchone()
    return int(row[0] or 0) if row else 0
