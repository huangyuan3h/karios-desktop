"""allocation_weights table — T4 weekly R5c capital decisions (2026-08-11).

The Monday decision (Asia/Shanghai) is persisted here so the paper intake
scales sleeves by the SAME week weights the backtest replays. First decision
of the week wins (idempotent); a missing row falls back to a same-day
decision recorded on the spot.
"""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "allocation_weights"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    week_start  TEXT PRIMARY KEY,
    cn_regime   TEXT NOT NULL,
    hk_regime   TEXT NOT NULL,
    w_cn        DOUBLE PRECISION NOT NULL,
    w_hk        DOUBLE PRECISION NOT NULL,
    decided_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)


def insert_week_decision(
    *,
    week_start: str,
    cn_regime: str,
    hk_regime: str,
    w_cn: float,
    w_hk: float,
) -> dict[str, Any]:
    """Record (or keep the first) decision for the week. Returns the row."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME} (week_start, cn_regime, hk_regime, w_cn, w_hk)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (week_start) DO NOTHING
                RETURNING week_start, cn_regime, hk_regime, w_cn, w_hk
                """,
                (week_start, cn_regime, hk_regime, w_cn, w_hk),
            )
            row = cur.fetchone()
            conn.commit()
            if row is not None:
                return {
                    "week_start": row[0],
                    "cn_regime": row[1],
                    "hk_regime": row[2],
                    "w_cn": float(row[3]),
                    "w_hk": float(row[4]),
                }
    return get_week_decision(week_start)


def get_week_decision(week_start: str) -> dict[str, Any] | None:
    """The week's persisted decision, or None when not decided yet."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT week_start, cn_regime, hk_regime, w_cn, w_hk FROM {TABLE_NAME} WHERE week_start = %s",
                (week_start,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return {
        "week_start": row[0],
        "cn_regime": row[1],
        "hk_regime": row[2],
        "w_cn": float(row[3]),
        "w_hk": float(row[4]),
    }
