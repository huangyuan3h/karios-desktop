from __future__ import annotations

import json
from typing import Any, Iterable

from psycopg.types.json import Json  # type: ignore[import-not-found]

from data_sync_service.db import get_connection

TABLE_NAME = "market_cn_sentiment_daily"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    date TEXT PRIMARY KEY,
    as_of_date TEXT NOT NULL,
    up_count INTEGER NOT NULL,
    down_count INTEGER NOT NULL,
    flat_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    up_down_ratio DOUBLE PRECISION NOT NULL,
    market_turnover_cny DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    market_volume DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    yesterday_limitup_premium DOUBLE PRECISION NOT NULL,
    failed_limitup_rate DOUBLE PRECISION NOT NULL,
    risk_mode TEXT NOT NULL,
    rules_json JSONB NOT NULL,
    updated_at TEXT NOT NULL,
    raw_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cn_sentiment_date ON {TABLE_NAME}(date DESC);
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
                str(r.get("as_of_date") or ""),
                int(r.get("up_count") or 0),
                int(r.get("down_count") or 0),
                int(r.get("flat_count") or 0),
                int(r.get("total_count") or 0),
                float(r.get("up_down_ratio") or 0.0),
                float(r.get("market_turnover_cny") or 0.0),
                float(r.get("market_volume") or 0.0),
                float(r.get("yesterday_limitup_premium") or 0.0),
                float(r.get("failed_limitup_rate") or 0.0),
                str(r.get("risk_mode") or "normal"),
                Json(r.get("rules") if isinstance(r.get("rules"), list) else []),
                str(r.get("updated_at") or ""),
                Json(r.get("raw") if isinstance(r.get("raw"), dict) else {"raw": r.get("raw")}),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME}(
                    date, as_of_date,
                    up_count, down_count, flat_count, total_count, up_down_ratio,
                    market_turnover_cny, market_volume,
                    yesterday_limitup_premium, failed_limitup_rate,
                    risk_mode, rules_json, updated_at, raw_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(date) DO UPDATE SET
                    as_of_date = excluded.as_of_date,
                    up_count = excluded.up_count,
                    down_count = excluded.down_count,
                    flat_count = excluded.flat_count,
                    total_count = excluded.total_count,
                    up_down_ratio = excluded.up_down_ratio,
                    market_turnover_cny = excluded.market_turnover_cny,
                    market_volume = excluded.market_volume,
                    yesterday_limitup_premium = excluded.yesterday_limitup_premium,
                    failed_limitup_rate = excluded.failed_limitup_rate,
                    risk_mode = excluded.risk_mode,
                    rules_json = excluded.rules_json,
                    updated_at = excluded.updated_at,
                    raw_json = excluded.raw_json
                """,
                values,
            )
        conn.commit()
    return len(values)


def list_days(*, as_of_date: str, days: int) -> list[dict[str, Any]]:
    ensure_table()
    days2 = max(1, min(int(days), 30))
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT date, up_count, down_count, flat_count, total_count,
                       up_down_ratio, market_turnover_cny, market_volume,
                       yesterday_limitup_premium, failed_limitup_rate,
                       risk_mode, rules_json, updated_at
                FROM {TABLE_NAME}
                WHERE date <= %s
                ORDER BY date DESC
                LIMIT %s
                """,
                (as_of_date, days2),
            )
            rows = cur.fetchall()
    items: list[dict[str, Any]] = []
    for r in rows:
        rules = r[11] if isinstance(r[11], list) else json.loads(str(r[11]) or "[]")
        items.append(
            {
                "date": str(r[0]),
                "upCount": int(r[1] or 0),
                "downCount": int(r[2] or 0),
                "flatCount": int(r[3] or 0),
                "totalCount": int(r[4] or 0),
                "upDownRatio": float(r[5] or 0.0),
                "marketTurnoverCny": float(r[6] or 0.0),
                "marketVolume": float(r[7] or 0.0),
                "yesterdayLimitUpPremium": float(r[8] or 0.0),
                "failedLimitUpRate": float(r[9] or 0.0),
                "riskMode": str(r[10] or "normal"),
                "rules": [str(x) for x in rules] if isinstance(rules, list) else [],
                "updatedAt": str(r[12] or ""),
            }
        )
    return list(reversed(items))


def get_latest_date() -> str | None:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
            row = cur.fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])
