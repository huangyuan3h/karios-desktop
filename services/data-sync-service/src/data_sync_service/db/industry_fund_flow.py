from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection
from data_sync_service.service.industry_taxonomy import (
    DEFAULT_INDUSTRY_FLOW_SOURCE,
    SW_L1_INDUSTRY_NAMES,
    SW_L1_LEVEL,
    SW_TAXONOMY,
    row_is_sw_l1,
    with_sw_l1_metadata,
)

TABLE_NAME = "market_cn_industry_fund_flow_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date          TEXT NOT NULL,
    industry_code TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    net_inflow    DOUBLE PRECISION NOT NULL,
    updated_at    TEXT NOT NULL,
    raw_json      JSONB NOT NULL,
    taxonomy      TEXT NOT NULL DEFAULT 'UNKNOWN',
    industry_level INTEGER,
    source        TEXT NOT NULL DEFAULT 'eastmoney_bkzj',
    PRIMARY KEY(date, industry_code)
);

CREATE INDEX IF NOT EXISTS idx_cn_industry_fund_flow_date ON {TABLE_NAME}(date DESC);
CREATE INDEX IF NOT EXISTS idx_cn_industry_fund_flow_taxonomy_level_date
    ON {TABLE_NAME}(taxonomy, industry_level, date DESC);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def _int_or_none(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _metadata_filter_sql() -> str:
    return f"""
    AND (
        (taxonomy = '{SW_TAXONOMY}' AND industry_level = {SW_L1_LEVEL})
        OR (taxonomy = 'UNKNOWN' AND industry_name = ANY(%s))
    )
    """


def _row_with_metadata(
    *,
    date_value: Any | None = None,
    industry_code: Any,
    industry_name: Any,
    net_inflow: Any,
    taxonomy: Any | None = None,
    industry_level: Any | None = None,
    source: Any | None = None,
) -> dict[str, Any]:
    row = with_sw_l1_metadata(
        {
            "industry_code": str(industry_code),
            "industry_name": str(industry_name),
            "net_inflow": float(net_inflow or 0.0),
            "taxonomy": str(taxonomy or ""),
            "industry_level": _int_or_none(industry_level),
            "source": str(source or ""),
        }
    )
    if date_value is not None:
        row["date"] = str(date_value)
    return row


def upsert_daily_rows(rows: Iterable[dict[str, Any]]) -> int:
    ensure_table()
    rows_list = [r for r in rows if r]
    if not rows_list:
        return 0
    values = []
    for r in rows_list:
        meta = with_sw_l1_metadata(r)
        values.append(
            (
                str(meta.get("date") or ""),
                str(meta.get("industry_code") or ""),
                str(meta.get("industry_name") or ""),
                float(meta.get("net_inflow") or 0.0),
                str(meta.get("updated_at") or ""),
                Json(meta.get("raw") if isinstance(meta.get("raw"), dict) else {"raw": meta.get("raw")}),
                str(meta.get("taxonomy") or "UNKNOWN"),
                _int_or_none(meta.get("industry_level")),
                str(meta.get("source") or DEFAULT_INDUSTRY_FLOW_SOURCE),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(
                    date, industry_code, industry_name, net_inflow, updated_at, raw_json,
                    taxonomy, industry_level, source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(date, industry_code) DO UPDATE SET
                    industry_name = excluded.industry_name,
                    net_inflow = excluded.net_inflow,
                    updated_at = excluded.updated_at,
                    raw_json = excluded.raw_json,
                    taxonomy = excluded.taxonomy,
                    industry_level = excluded.industry_level,
                    source = excluded.source
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
                SELECT industry_code, industry_name, net_inflow, taxonomy, industry_level, source
                FROM {TABLE_NAME}
                WHERE date = %s
                {_metadata_filter_sql()}
                ORDER BY net_inflow DESC
                LIMIT %s
                """,
                (as_of_date, SW_L1_INDUSTRY_NAMES, lim),
            )
            rows = cur.fetchall()
    out = [
        _row_with_metadata(
            industry_code=r[0],
            industry_name=r[1],
            net_inflow=r[2],
            taxonomy=r[3],
            industry_level=r[4],
            source=r[5],
        )
        for r in rows
    ]
    return [r for r in out if row_is_sw_l1(r)]


def get_rows_by_date(as_of_date: str) -> list[dict[str, Any]]:
    """Return SW Level 1 industry flow rows for a given date."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT industry_code, industry_name, net_inflow, taxonomy, industry_level, source
                FROM {TABLE_NAME}
                WHERE date = %s
                {_metadata_filter_sql()}
                """,
                (as_of_date, SW_L1_INDUSTRY_NAMES),
            )
            rows = cur.fetchall()
    out = [
        _row_with_metadata(
            industry_code=r[0],
            industry_name=r[1],
            net_inflow=r[2],
            taxonomy=r[3],
            industry_level=r[4],
            source=r[5],
        )
        for r in rows
    ]
    return [r for r in out if row_is_sw_l1(r)]


def get_rows_for_dates(dates: list[str]) -> list[dict[str, Any]]:
    """Return SW Level 1 industry flow rows for the given dates."""
    ensure_table()
    if not dates:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, industry_code, industry_name, net_inflow, taxonomy, industry_level, source
                FROM {TABLE_NAME}
                WHERE date = ANY(%s)
                {_metadata_filter_sql()}
                ORDER BY date ASC, industry_name ASC
                """,
                (dates, SW_L1_INDUSTRY_NAMES),
            )
            rows = cur.fetchall()
    out = [
        _row_with_metadata(
            date_value=r[0],
            industry_code=r[1],
            industry_name=r[2],
            net_inflow=r[3],
            taxonomy=r[4],
            industry_level=r[5],
            source=r[6],
        )
        for r in rows
    ]
    return [r for r in out if row_is_sw_l1(r)]


def get_sum_by_industry_for_dates(dates: list[str]) -> list[dict[str, Any]]:
    """Return per-industry SW Level 1 sum of net_inflow for given dates."""
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
                {_metadata_filter_sql()}
                GROUP BY industry_name
                ORDER BY sum_inflow DESC
                """,
                (dates, SW_L1_INDUSTRY_NAMES),
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
                {_metadata_filter_sql()}
                ORDER BY date ASC
                """,
                (industry_name, dates, SW_L1_INDUSTRY_NAMES),
            )
            rows = cur.fetchall()
    return [{"date": str(r[0]), "net_inflow": float(r[1] or 0.0)} for r in rows]


def export_all_rows() -> list[dict[str, Any]]:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, industry_code, industry_name, net_inflow, updated_at, raw_json,
                       taxonomy, industry_level, source
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
                "taxonomy": str(r[6] or "UNKNOWN"),
                "industry_level": _int_or_none(r[7]),
                "source": str(r[8] or DEFAULT_INDUSTRY_FLOW_SOURCE),
            }
        )
    return out
