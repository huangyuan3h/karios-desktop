"""bar_5min — last-hour 5-minute OHLCV (14:30–15:00) for CN A-shares.

Used to study 14:30 vs 15:00 gap-list drift and last-5-minute
limit-lock paths. Not mixed with bar_minute (1-minute Tencent tape): a
14:30 5-minute bar is not the same OHLC as the 14:30 1-minute print.

History source: baostock (1y+ 5min in one query). Tushare stk_mins can
reach 10y but is 1 call/min (or /hour on this token). Eastmoney/akshare
5min only keep ~6 weeks.
"""

from __future__ import annotations

from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db._ensure_guard import ensure_once

TABLE_NAME = "bar_5min"

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
    source TEXT NOT NULL DEFAULT 'baostock',
    UNIQUE (ts_code, trade_date, trade_time)
);
CREATE INDEX IF NOT EXISTS ix_bar_5min_ts_date ON {TABLE_NAME} (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS ix_bar_5min_date_time ON {TABLE_NAME} (trade_date, trade_time);
"""


# Source priority (higher rank wins on conflict, OPT-224): real 5-minute bars
# ('baostock'/'tushare.stk_mins'/'ext_*') always beat 'live_1430' — a
# close-only 14:30 snapshot written by the live panel job. The snapshot exists
# so the replay's breadth gate has a market-wide 14:30 sample and held legs get
# a 14:30 exit print; it must never clobber a real bar's OHLC (amp_1430).
def _source_rank(col: str) -> str:
    return f"(CASE {col} WHEN 'live_1430' THEN -1 WHEN 'ext_15min' THEN 0 ELSE 1 END)"


UPSERT_SQL = f"""
INSERT INTO {TABLE_NAME}(
    ts_code, trade_date, trade_time, open, high, low, close, vol, amount, source
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    vol = EXCLUDED.vol,
    amount = EXCLUDED.amount,
    source = EXCLUDED.source
WHERE {_source_rank(f"{TABLE_NAME}.source")}
    <= {_source_rank("EXCLUDED.source")}
"""


def ensure_table() -> None:
    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for stmt in CREATE_SQL.split(";"):
                    part = stmt.strip()
                    if part:
                        cur.execute(part)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def upsert_5min_bars(
    ts_code: str,
    rows: list[dict[str, Any]],
    *,
    source: str = "baostock",
) -> int:
    """Idempotent upsert. Each row needs trade_date, time, close; OHLC/vol optional."""
    if not rows:
        return 0
    payload = []
    for r in rows:
        close = r.get("close")
        if close is None:
            continue
        payload.append(
            (
                ts_code,
                r["trade_date"],
                r["time"],
                r.get("open"),
                r.get("high"),
                r.get("low"),
                float(close),
                r.get("vol"),
                r.get("amount"),
                source,
            )
        )
    return upsert_5min_payload(payload)


def upsert_5min_payload(payload: list[tuple], *, on_conflict: str = "update") -> int:
    """Bulk COPY into a temp table then ranked INSERT. Safe to rerun.

    ``on_conflict="update"`` (default): same-rank rows overwrite existing
    prints (live sync corrections). ``on_conflict="nothing"``: historical
    backfills only fill MISSING (ts, date, time) keys and never rewrite an
    existing print — OPT-211 P3: a 09-15 baostock re-fetch overwrote live
    14:30 prints and repriced satellite fills (starport valid 20.7→18.0
    with no code change). Backfills must use "nothing".
    """
    if on_conflict not in ("update", "nothing"):
        raise ValueError(f"on_conflict must be 'update'|'nothing', got {on_conflict!r}")
    if not payload:
        return 0
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE _bar_5min_in (
                    ts_code TEXT,
                    trade_date TEXT,
                    trade_time TEXT,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    vol DOUBLE PRECISION,
                    amount DOUBLE PRECISION,
                    source TEXT
                ) ON COMMIT DROP
                """
            )
            with cur.copy(
                "COPY _bar_5min_in (ts_code, trade_date, trade_time, "
                "open, high, low, close, vol, amount, source) FROM STDIN"
            ) as copy:
                for row in payload:
                    copy.write_row(row)
            conflict_clause = (
                "ON CONFLICT (ts_code, trade_date, trade_time) DO NOTHING"
                if on_conflict == "nothing"
                else f"""ON CONFLICT (ts_code, trade_date, trade_time) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    vol = EXCLUDED.vol,
                    amount = EXCLUDED.amount,
                    source = EXCLUDED.source
                WHERE {_source_rank(f"{TABLE_NAME}.source")}
                    <= {_source_rank("EXCLUDED.source")}"""
            )
            cur.execute(
                f"""
                INSERT INTO {TABLE_NAME}(
                    ts_code, trade_date, trade_time, open, high, low, close, vol, amount, source
                )
                SELECT ts_code, trade_date, trade_time, open, high, low, close, vol, amount, source
                FROM _bar_5min_in
                WHERE close IS NOT NULL
                {conflict_clause}
                """
            )
        conn.commit()
    return len(payload)


def coverage_by_ts_code(start_date: str, end_date: str) -> dict[str, int]:
    """Distinct trade_dates that already have a 15:00 bar in [start, end]."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts_code, COUNT(DISTINCT trade_date)
                FROM {TABLE_NAME}
                WHERE trade_time = '1500'
                  AND trade_date >= %s AND trade_date <= %s
                GROUP BY ts_code
                """,
                (start_date, end_date),
            )
            return {str(ts): int(n) for ts, n in cur.fetchall()}


def count_rows() -> int:
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TABLE_NAME}")
            return int(cur.fetchone()[0])


def fetch_1430_closes(
    ts_codes: list[str], start_date: str, end_date: str
) -> dict[str, dict[str, float]]:
    """Raw 14:30 close per ts_code/day over [start, end] (H-CLOCK-1430).

    ``bar_5min`` stores **raw** prints; the ``source='live_1430'`` snapshot rows
    written by the live panel job are the same 14:30 print (close only) and are
    accepted. A missing row means "no 14:30 print that day" — callers must skip
    the fill, never guess a price.
    """
    codes = [c.strip().upper() for c in ts_codes if c and c.strip()]
    if not codes:
        return {}
    ensure_table()
    out: dict[str, dict[str, float]] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts_code, trade_date, close
                FROM {TABLE_NAME}
                WHERE ts_code = ANY(%s)
                  AND trade_time = '1430'
                  AND trade_date >= %s AND trade_date <= %s
                  AND close IS NOT NULL AND close > 0
                """,
                (codes, start_date, end_date),
            )
            for ts_code, trade_date, close in cur.fetchall():
                d = (
                    trade_date.strftime("%Y-%m-%d")
                    if hasattr(trade_date, "strftime")
                    else str(trade_date)
                )
                out.setdefault(str(ts_code), {})[d] = float(close)
    return out
