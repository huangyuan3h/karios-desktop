from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from data_sync_service.db import get_connection
from psycopg.types.json import Json

RUN_TABLE = "backtest_run"
TRADE_TABLE = "backtest_trade"

CREATE_RUN_SQL = f"""
CREATE TABLE IF NOT EXISTS {RUN_TABLE} (
    id            TEXT PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    start_date    DATE NOT NULL,
    end_date      DATE NOT NULL,
    status        TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    params        JSONB,
    summary       JSONB,
    equity_curve  JSONB,
    drawdown_curve JSONB,
    positions_curve JSONB,
    daily_log     JSONB,
    error_message TEXT
);
"""

CREATE_TRADE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TRADE_TABLE} (
    id           BIGSERIAL PRIMARY KEY,
    run_id       TEXT NOT NULL,
    ts_code      TEXT NOT NULL,
    trade_date   DATE NOT NULL,
    action       TEXT NOT NULL,
    qty          NUMERIC,
    price        NUMERIC,
    fee          NUMERIC,
    cash_after   NUMERIC,
    reason       TEXT
);
"""


def ensure_tables() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_RUN_SQL)
            cur.execute(CREATE_TRADE_SQL)
            cur.execute(f"ALTER TABLE {RUN_TABLE} ADD COLUMN IF NOT EXISTS daily_log JSONB")
        conn.commit()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def insert_run(
    run_id: str,
    strategy_name: str,
    start_date: str,
    end_date: str,
    params: dict[str, Any],
) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {RUN_TABLE}
                    (id, strategy_name, start_date, end_date, status, created_at, params)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, strategy_name, start_date, end_date, "running", _now_utc(), Json(params)),
            )
        conn.commit()


def update_run_success(
    run_id: str,
    summary: dict[str, Any],
    equity_curve: list[dict[str, Any]],
    drawdown_curve: list[dict[str, Any]],
    positions_curve: list[dict[str, Any]],
    daily_log: list[dict[str, Any]],
) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {RUN_TABLE}
                SET status = %s,
                    summary = %s,
                    equity_curve = %s,
                    drawdown_curve = %s,
                    positions_curve = %s,
                    daily_log = %s,
                    error_message = NULL
                WHERE id = %s
                """,
                (
                    "success",
                    Json(summary),
                    Json(equity_curve),
                    Json(drawdown_curve),
                    Json(positions_curve),
                    Json(daily_log),
                    run_id,
                ),
            )
        conn.commit()


def update_run_failed(run_id: str, error_message: str) -> None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {RUN_TABLE}
                SET status = %s,
                    error_message = %s
                WHERE id = %s
                """,
                ("failed", error_message, run_id),
            )
        conn.commit()


def insert_trades(run_id: str, trades: list[dict[str, Any]]) -> None:
    if not trades:
        return
    ensure_tables()
    rows = []
    for t in trades:
        rows.append(
            (
                run_id,
                t.get("ts_code"),
                t.get("trade_date"),
                t.get("action"),
                t.get("qty"),
                t.get("price"),
                t.get("fee"),
                t.get("cash_after"),
                t.get("reason"),
            )
        )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TRADE_TABLE}
                    (run_id, ts_code, trade_date, action, qty, price, fee, cash_after, reason)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()


def fetch_run(run_id: str) -> dict[str, Any] | None:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, strategy_name, start_date, end_date, status, created_at,
                       params, summary, equity_curve, drawdown_curve, positions_curve, daily_log, error_message
                FROM {RUN_TABLE}
                WHERE id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d.name for d in cur.description]
    out: dict[str, Any] = {}
    for col, val in zip(cols, row):
        if hasattr(val, "strftime"):
            out[col] = val.strftime("%Y-%m-%d %H:%M:%S")
        else:
            out[col] = val
    return out


def fetch_trades(run_id: str) -> list[dict[str, Any]]:
    ensure_tables()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts_code, trade_date, action, qty, price, fee, cash_after, reason
                FROM {TRADE_TABLE}
                WHERE run_id = %s
                ORDER BY trade_date, id
                """,
                (run_id,),
            )
            rows = cur.fetchall()
            cols = [d.name for d in cur.description]
    out: list[dict[str, Any]] = []
    for row in rows:
        obj: dict[str, Any] = {}
        for col, val in zip(cols, row):
            if hasattr(val, "strftime"):
                obj[col] = val.strftime("%Y-%m-%d")
            elif hasattr(val, "__float__") and col not in ("ts_code", "action", "reason"):
                try:
                    obj[col] = float(val)
                except (TypeError, ValueError):
                    obj[col] = val
            else:
                obj[col] = val
        out.append(obj)
    return out
