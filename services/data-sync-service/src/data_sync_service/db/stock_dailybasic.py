"""Stock daily valuation (total_mv / circ_mv / turnover_rate) — §19.2 step 12.

Market-cap layering for the S-3 universe: split candidates by market cap and
enforce a liquidity floor without touching tradability. Fed from tushare
``daily_basic`` (per trade date, whole market), stored per (ts_code, date).
"""

from __future__ import annotations

import logging

import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_dailybasic"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code       TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    total_mv      DOUBLE PRECISION,
    circ_mv       DOUBLE PRECISION,
    turnover_rate DOUBLE PRECISION,
    PRIMARY KEY (ts_code, trade_date)
)
"""


def ensure_table() -> None:
    from data_sync_service.db.daily import ensure_once

    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def sync_daily_basic_for_date(trade_date: str) -> int:
    """Fetch one trade date's valuation rows for the whole market (best-effort)."""
    try:
        pro = ts.pro_api(get_settings().tu_share_api_key)
    except Exception:  # noqa: BLE001
        ts.set_token(get_settings().tu_share_api_key)
        pro = ts.pro_api()
    df = pro.daily_basic(
        trade_date=trade_date.replace("-", ""),
        fields="ts_code,total_mv,circ_mv,turnover_rate",
    )
    if df is None or df.empty:
        return 0
    ensure_table()
    rows = [
        (str(r.ts_code), trade_date, float(r.total_mv), float(r.circ_mv), float(r.turnover_rate))
        for r in df.itertuples()
        if r.ts_code
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {TABLE_NAME} (ts_code, trade_date, total_mv, circ_mv, turnover_rate)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ts_code, trade_date) DO NOTHING
                """,
                rows,
            )
        conn.commit()
    return len(rows)


def sync_daily_basic_gap(end_date: str | None = None) -> dict[str, object]:
    """Incrementally sync stock_dailybasic from the table's last date to end_date.

    ``stock_dailybasic`` was orphaned after 2026-08-07 (no scheduler wrote it);
    the Twin-Star satellite now depends on daily total_mv, so this is the
    dedicated chain step (daily_basic_job, weekdays 17:20 Asia/Shanghai).
    Idempotent: per-(ts_code, trade_date) upsert, per-date tushare call.
    """
    from datetime import date, timedelta

    from data_sync_service.db.trade_calendar import is_trading_day
    from data_sync_service.db.sync_job_record import insert_record

    JOB_TYPE = "stock_daily_basic_sync"
    settings = get_settings()
    if not settings.tu_share_api_key:
        insert_record(job_type=JOB_TYPE, success=False, error_message="TU_SHARE_API_KEY not set")
        return {"ok": False, "error": "TU_SHARE_API_KEY not set"}

    end = date.fromisoformat(end_date) if end_date else date.today()
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(trade_date) FROM {TABLE_NAME}")
            row = cur.fetchone()
    last = date.fromisoformat(row[0]) if row and row[0] else None
    if last is None:
        return {"ok": False, "error": "no existing rows; run full backfill first"}

    days: list[date] = []
    d = last + timedelta(days=1)
    while d <= end:
        if is_trading_day("SSE", d) is True:
            days.append(d)
        d += timedelta(days=1)
    if not days:
        return {"ok": True, "skipped": True, "updated": 0, "message": "no gap"}

    total = 0
    for day in days:
        try:
            total += sync_daily_basic_for_date(day.isoformat())
        except Exception as exc:  # noqa: BLE001
            logger.warning("daily_basic sync failed for %s: %s", day.isoformat(), exc)
            insert_record(
                job_type=JOB_TYPE,
                success=False,
                last_ts_code=day.isoformat(),
                error_message=str(exc),
            )
            return {"ok": False, "error": str(exc), "day": day.isoformat(), "updated": total}
    insert_record(job_type=JOB_TYPE, success=True, last_ts_code=None, error_message=None)
    return {"ok": True, "updated": total, "days": len(days)}


def market_cap_by_date(trade_date: str) -> dict[str, float]:
    """{ts_code: total_mv} for one trade date (10k CNY)."""
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ts_code, total_mv FROM {TABLE_NAME} WHERE trade_date = %s AND total_mv IS NOT NULL",
                (trade_date,),
            )
            return {str(r[0]): float(r[1]) for r in cur.fetchall()}
