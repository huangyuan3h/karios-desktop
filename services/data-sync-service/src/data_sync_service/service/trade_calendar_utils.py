"""Shared trade-calendar helpers for CN market date selection."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable
from zoneinfo import ZoneInfo

from data_sync_service.db.trade_calendar import get_open_dates, is_trading_day

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_EXCHANGE = "SSE"


def shanghai_today() -> date:
    return datetime.now(tz=SHANGHAI_TZ).date()


def shanghai_today_iso() -> str:
    return shanghai_today().isoformat()


def is_cn_trading_day(d: date, *, exchange: str = DEFAULT_EXCHANGE) -> bool | None:
    """True when d is an open SSE day; False when closed; None if calendar not seeded."""
    return is_trading_day(exchange, d)


def last_open_date_on_or_before(
    d: date,
    *,
    exchange: str = DEFAULT_EXCHANGE,
) -> date | None:
    """Last exchange open date on or before d; None if calendar has no rows in range."""
    flag = is_trading_day(exchange, d)
    if flag is True:
        return d
    start = d - timedelta(days=120)
    opens = get_open_dates(exchange=exchange, start_date=start, end_date=d)
    return opens[-1] if opens else None


def clamp_to_last_open_date(d0: str, *, exchange: str = DEFAULT_EXCHANGE) -> str:
    """Return ISO date clamped to last open day on or before d0."""
    try:
        d = date.fromisoformat(str(d0).strip()[:10])
    except ValueError:
        return str(d0)
    last = last_open_date_on_or_before(d, exchange=exchange)
    return last.isoformat() if last else d.isoformat()


def trade_dates_upto(
    d0: str,
    days: int,
    *,
    exchange: str = DEFAULT_EXCHANGE,
    fallback_dates_fn: Callable[[str, int], list[str]] | None = None,
) -> list[str]:
    """
    Return the last N open trading dates up to and including d0 (clamped to last open day).

    When trade_calendar is not seeded, optional fallback_dates_fn provides DB DISTINCT dates.
    """
    lim = max(1, min(int(days), 60))
    try:
        end = date.fromisoformat(str(d0).strip()[:10])
    except ValueError:
        end = shanghai_today()
    end = last_open_date_on_or_before(end, exchange=exchange) or end

    if is_trading_day(exchange, end) is not None:
        start = end - timedelta(days=120)
        opens = get_open_dates(exchange=exchange, start_date=start, end_date=end)
        if opens:
            xs = [x.isoformat() for x in opens][-lim:]
            return xs

    if fallback_dates_fn is not None:
        fb = fallback_dates_fn(end.isoformat(), lim)
        if fb:
            return fb[-lim:]

    return []


def resolve_effective_as_of(raw: str | None) -> str:
    """
    Dashboard as-of: prefer stored latest date but clamp to last SSE open day on or before it.
    """
    s = (raw or "").strip() or shanghai_today_iso()
    try:
        d = date.fromisoformat(s[:10])
    except ValueError:
        return shanghai_today_iso()
    last = last_open_date_on_or_before(d)
    if last is None:
        return s
    return min(s, last.isoformat())
