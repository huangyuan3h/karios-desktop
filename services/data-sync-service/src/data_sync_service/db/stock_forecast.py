"""Stock earnings forecasts (业绩预告) — P14 PEAD (post-earnings drift).

Fed from tushare ``forecast`` (per announcement date, whole market). Stores
per (ts_code, ann_date): forecast type + profit range + change pct, so the
backtest engine can gate entries to names with POSITIVE earnings surprises
announced within the last N sessions (盈余公告后漂移).

Data discipline (planned-doc §2 P14): the event date MUST be the
announcement date (ann_date), NOT the report period end (end_date) — a
report published in April is only tradeable from its announcement onward.
"""

from __future__ import annotations

import logging
import time

import tushare as ts  # type: ignore[import-not-found]

from data_sync_service.config import get_settings
from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

TABLE_NAME = "stock_forecast"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    ts_code         TEXT NOT NULL,
    ann_date        TEXT NOT NULL,
    end_date        TEXT,
    forecast_type   TEXT,
    net_profit_min  DOUBLE PRECISION,
    net_profit_max  DOUBLE PRECISION,
    change_pct      DOUBLE PRECISION,
    PRIMARY KEY (ts_code, ann_date)
)
"""

# Positive-surprise types (预增/扭亏/略增/续盈). Types like 预减/首亏/续亏/略减
# are negative — excluded by the engine gate, kept in the table for diagnostics.
POSITIVE_TYPES = frozenset({"预增", "扭亏", "略增", "续盈"})


def _iso_date(s: str) -> str:
    """'20260805' -> '2026-08-05' (tushare returns compact dates)."""
    s = s.strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def ensure_table() -> None:
    from data_sync_service.db.daily import ensure_once

    def _impl() -> None:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_SQL)
            conn.commit()

    ensure_once(TABLE_NAME, _impl)


def sync_forecast_for_dates(start_date: str, end_date: str, *, limit: int = 2000) -> int:
    """Fetch forecast rows for announcement dates in [start_date, end_date].

    tushare forecast requires ann_date (one day per call) — we loop
    day-by-day with a short sleep and a retry on rate-limit errors.
    Idempotent (ON CONFLICT DO NOTHING).
    """
    try:
        pro = ts.pro_api(get_settings().tu_share_api_key)
    except Exception:  # noqa: BLE001
        ts.set_token(get_settings().tu_share_api_key)
        pro = ts.pro_api()
    ensure_table()
    total = 0
    from datetime import date as _date
    from datetime import timedelta

    d = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    while d <= end:
        day = d.isoformat()
        rows = None
        for attempt in range(3):
            try:
                df = pro.forecast(
                    ann_date=day.replace("-", ""),
                    fields="ts_code,ann_date,end_date,type,net_profit_min,net_profit_max,p_change_min,p_change_max",
                )
                rows = [] if (df is None or df.empty) else [
                    (
                        str(r.ts_code),
                        _iso_date(str(r.ann_date)),
                        str(r.end_date or ""),
                        str(r.type or ""),
                        float(r.net_profit_min) if r.net_profit_min is not None else None,
                        float(r.net_profit_max) if r.net_profit_max is not None else None,
                        float(r.p_change_min) if getattr(r, "p_change_min", None) is not None else None,
                    )
                    for r in df.itertuples()
                    if getattr(r, "ts_code", None)
                ]
                break
            except Exception as exc:  # noqa: BLE001 — rate limit / transient
                if attempt == 2:
                    logger.warning("forecast fetch failed %s: %s", day, exc)
                time.sleep(3.0 * (attempt + 1))
        if rows is None:
            d += timedelta(days=1)
            continue
        if rows:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"""
                        INSERT INTO {TABLE_NAME}
                            (ts_code, ann_date, end_date, forecast_type, net_profit_min, net_profit_max, change_pct)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (ts_code, ann_date) DO NOTHING
                        """,
                        rows,
                    )
                conn.commit()
            total += len(rows)
        d += timedelta(days=1)
        time.sleep(0.5)
    return total


def positive_forecast_dates(start_date: str, end_date: str) -> dict[str, set[str]]:
    """{ts_code: set(ann_date)} with a POSITIVE forecast type in the window.

    Used by the backtest engine's PEAD gate: a candidate's entry day must be
    within N sessions after one of these dates.
    """
    ensure_table()
    out: dict[str, set[str]] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT ts_code, ann_date FROM {TABLE_NAME}
                WHERE ann_date >= %s AND ann_date <= %s
                  AND forecast_type = ANY(%s::text[])
                """,
                (start_date, end_date, list(POSITIVE_TYPES)),
            )
            for ts, d in cur.fetchall():
                out.setdefault(str(ts), set()).add(_iso_date(str(d)))
    return out
