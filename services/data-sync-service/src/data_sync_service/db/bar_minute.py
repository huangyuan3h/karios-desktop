"""bar_minute — 1-minute OHLCV bars (TIP-014 Phase 3 / D7).

Stores intraday minute bars captured daily from Tencent minute endpoints
(HK: appstock/app/hkMinute/query · CN: appstock/app/minute/query). This is
the ONLY intraday source reachable from this network (Eastmoney push2his is
IP-rate-limited since 2026-08-14; yfinance rate-limited; no other source).

Design:
- 1-minute granularity stored raw; re-sample to 5m/any window in analysis.
- Data accumulates forward from deployment (no backfill — Tencent minute
  endpoints return only the current session; Eastmoney history blocked).
- A 5m bar for the last hour of the session (尾盘) is what the entry-price
  research (last_hour_low validation) needs; also useful for fan-day style
  research later.
"""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection

TABLE_NAME = "bar_minute"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    ts_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    trade_time TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION NOT NULL,
    vol DOUBLE PRECISION,
    amount DOUBLE PRECISION,
    UNIQUE (ts_code, trade_date, trade_time)
);
CREATE INDEX IF NOT EXISTS ix_bar_minute_ts_date ON {TABLE_NAME} (ts_code, trade_date);
"""


def ensure_table() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()


def upsert_minute_bars(ts_code: str, trade_date: str, rows: list[dict[str, Any]]) -> int:
    """Insert 1-minute bars for one symbol/date (idempotent on the unique key).

    Each row: {"time": "0930", "open":…, "high":…, "low":…, "close":…, "vol":…, "amount":…}
    """
    if not rows:
        return 0
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    f"""
                    INSERT INTO {TABLE_NAME}(ts_code, trade_date, trade_time, open, high, low, close, vol, amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, vol = EXCLUDED.vol, amount = EXCLUDED.amount
                    """,
                    (
                        ts_code, trade_date, r["time"],
                        r.get("open"), r.get("high"), r.get("low"),
                        r["close"], r.get("vol"), r.get("amount"),
                    ),
                )
        conn.commit()
    return len(rows)


def has_minute_data(ts_code: str, trade_date: str) -> bool:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT 1 FROM {TABLE_NAME} WHERE ts_code = %s AND trade_date = %s LIMIT 1",
                (ts_code, trade_date),
            )
            return cur.fetchone() is not None


def count_rows() -> int:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TABLE_NAME}")
            return int(cur.fetchone()[0])
