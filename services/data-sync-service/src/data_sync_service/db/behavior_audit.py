"""behavior_audit table: user's real holdings vs backtest "should hold" (OPT-106)."""

from __future__ import annotations

import json
from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "behavior_audit"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id          SERIAL PRIMARY KEY,
    audit_date  TEXT NOT NULL,
    market      TEXT NOT NULL,
    expected    INTEGER NOT NULL,
    actual      INTEGER NOT NULL,
    extra       INTEGER NOT NULL,
    missing     INTEGER NOT NULL,
    extra_list  TEXT,
    missing_list TEXT,
    sat_expected INTEGER NOT NULL DEFAULT 0,
    sat_actual  INTEGER NOT NULL DEFAULT 0,
    sat_extra   INTEGER NOT NULL DEFAULT 0,
    sat_missing INTEGER NOT NULL DEFAULT 0,
    sat_extra_list TEXT,
    sat_missing_list TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (audit_date, market)
);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)


def insert_audit(
    *,
    audit_date: str,
    market: str,
    expected: int,
    actual: int,
    extra: int,
    missing: int,
    extra_list: list[dict[str, Any]] | None = None,
    missing_list: list[dict[str, Any]] | None = None,
    sat_expected: int = 0,
    sat_actual: int = 0,
    sat_extra: int = 0,
    sat_missing: int = 0,
    sat_extra_list: list[dict[str, Any]] | None = None,
    sat_missing_list: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Upsert one day+market audit (idempotent on re-run)."""
    ensure_table()
    sat_payload = (
        sat_expected,
        sat_actual,
        sat_extra,
        sat_missing,
        json.dumps(sat_extra_list or [], ensure_ascii=False),
        json.dumps(sat_missing_list or [], ensure_ascii=False),
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME}
                    (audit_date, market, expected, actual, extra, missing, extra_list, missing_list,
                     sat_expected, sat_actual, sat_extra, sat_missing, sat_extra_list, sat_missing_list)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (audit_date, market) DO UPDATE SET
                        expected = EXCLUDED.expected,
                        actual = EXCLUDED.actual,
                        extra = EXCLUDED.extra,
                        missing = EXCLUDED.missing,
                        extra_list = EXCLUDED.extra_list,
                        missing_list = EXCLUDED.missing_list,
                        sat_expected = EXCLUDED.sat_expected,
                        sat_actual = EXCLUDED.sat_actual,
                        sat_extra = EXCLUDED.sat_extra,
                        sat_missing = EXCLUDED.sat_missing,
                        sat_extra_list = EXCLUDED.sat_extra_list,
                        sat_missing_list = EXCLUDED.sat_missing_list,
                        created_at = now()
                    RETURNING id
                    """,
                    (
                        audit_date,
                        market,
                        expected,
                        actual,
                        extra,
                        missing,
                        json.dumps(extra_list or [], ensure_ascii=False),
                        json.dumps(missing_list or [], ensure_ascii=False),
                        *sat_payload,
                    ),
                )
            except Exception:  # noqa: BLE001
                # Pre-0040 DBs (migration not run yet): persist core leg only.
                conn.rollback()
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME}
                    (audit_date, market, expected, actual, extra, missing, extra_list, missing_list)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (audit_date, market) DO UPDATE SET
                        expected = EXCLUDED.expected,
                        actual = EXCLUDED.actual,
                        extra = EXCLUDED.extra,
                        missing = EXCLUDED.missing,
                        extra_list = EXCLUDED.extra_list,
                        missing_list = EXCLUDED.missing_list,
                        created_at = now()
                    RETURNING id
                    """,
                    (
                        audit_date,
                        market,
                        expected,
                        actual,
                        extra,
                        missing,
                        json.dumps(extra_list or [], ensure_ascii=False),
                        json.dumps(missing_list or [], ensure_ascii=False),
                    ),
                )
            row = cur.fetchone()
    return {"id": row[0] if row else None}


def latest_audit(limit: int = 2) -> list[dict[str, Any]]:
    """Most recent audits, newest date first (both markets per date)."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT audit_date, market, expected, actual, extra, missing,
                           extra_list, missing_list,
                           sat_expected, sat_actual, sat_extra, sat_missing,
                           sat_extra_list, sat_missing_list
                    FROM {TABLE_NAME}
                    ORDER BY audit_date DESC, market
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
            except Exception:  # noqa: BLE001
                # Pre-0040 DBs (migration not run yet): legacy columns only.
                conn.rollback()
                cur.execute(
                    f"""
                    SELECT audit_date, market, expected, actual, extra, missing,
                           extra_list, missing_list
                    FROM {TABLE_NAME}
                    ORDER BY audit_date DESC, market
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = [( *r, 0, 0, 0, 0, None, None) for r in cur.fetchall()]
    out: list[dict[str, Any]] = []
    for r in rows:
        def _load(v: str | None) -> list[dict[str, Any]]:
            if not v:
                return []
            try:
                return json.loads(v)
            except (TypeError, json.JSONDecodeError):
                return []

        out.append({
            "auditDate": str(r[0]),
            "market": str(r[1]),
            "expected": r[2],
            "actual": r[3],
            "extra": r[4],
            "missing": r[5],
            "extraList": _load(r[6]),
            "missingList": _load(r[7]),
            "satExpected": r[8] or 0,
            "actualSat": r[9] or 0,
            "satExtra": r[10] or 0,
            "satMissing": r[11] or 0,
            "satExtraList": _load(r[12]),
            "satMissingList": _load(r[13]),
        })
    return out
