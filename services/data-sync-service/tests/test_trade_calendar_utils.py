"""Unit tests for trade_calendar_utils."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.service import trade_calendar_utils as tcu


def test_trade_dates_upto_uses_open_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    opens = [
        date(2026, 6, 16),
        date(2026, 6, 17),
        date(2026, 6, 18),
        date(2026, 6, 22),
    ]

    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: True)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: opens)
    monkeypatch.setattr(
        tcu,
        "last_open_date_on_or_before",
        lambda d, exchange="SSE": d,
    )

    out = tcu.trade_dates_upto("2026-06-22", 5)
    assert out == ["2026-06-16", "2026-06-17", "2026-06-18", "2026-06-22"]
    assert "2026-06-19" not in out
    assert "2026-06-20" not in out


def test_trade_dates_upto_fallback_when_calendar_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: None)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [])
    monkeypatch.setattr(
        tcu,
        "last_open_date_on_or_before",
        lambda d, exchange="SSE": d,
    )

    out = tcu.trade_dates_upto(
        "2026-06-22",
        3,
        fallback_dates_fn=lambda _d, n: ["2026-06-18", "2026-06-19", "2026-06-22"],
    )
    assert out == ["2026-06-18", "2026-06-19", "2026-06-22"]


def test_resolve_effective_as_of_clamps_holiday(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tcu,
        "last_open_date_on_or_before",
        lambda d, exchange="SSE": date(2026, 6, 18),
    )
    assert tcu.resolve_effective_as_of("2026-06-20") == "2026-06-18"


def test_resolve_effective_as_of_keeps_trading_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tcu,
        "last_open_date_on_or_before",
        lambda d, exchange="SSE": d,
    )
    assert tcu.resolve_effective_as_of("2026-06-22") == "2026-06-22"


# --- compute_market_status ---

SH = ZoneInfo("Asia/Shanghai")


def test_compute_market_status_pre_market() -> None:
    # Thursday 2026-06-25 06:59 Shanghai — pre-market
    now = datetime(2026, 6, 25, 6, 59, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "PreOpen"
    assert st["isPreMarket"] is True
    assert st["isMarketOpen"] is False
    assert st["isTradingDay"] is True


def test_compute_market_status_open_morning() -> None:
    # Thursday 10:15 — open
    now = datetime(2026, 6, 25, 10, 15, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "Open"
    assert st["isMarketOpen"] is True
    assert st["isPreMarket"] is False


def test_compute_market_status_lunch_break() -> None:
    # Thursday 12:00 — lunch break
    now = datetime(2026, 6, 25, 12, 0, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "LunchBreak"
    assert st["isMarketOpen"] is False


def test_compute_market_status_open_afternoon() -> None:
    # Thursday 14:30 — open
    now = datetime(2026, 6, 25, 14, 30, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "Open"
    assert st["isMarketOpen"] is True


def test_compute_market_status_closed_after_hours() -> None:
    # Thursday 16:00 — closed (after hours)
    now = datetime(2026, 6, 25, 16, 0, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "Closed"
    assert st["isMarketOpen"] is False
    assert st["isPreMarket"] is False
    assert st["isTradingDay"] is True


def test_compute_market_status_weekend() -> None:
    # Saturday 10:00 — weekend
    now = datetime(2026, 6, 27, 10, 0, tzinfo=SH)
    st = tcu.compute_market_status(now)
    assert st["phase"] == "Weekend"
    assert st["isTradingDay"] is False
    assert st["isPreMarket"] is False
    assert st["isMarketOpen"] is False


# --- previous_open_date ---


def test_previous_open_date_returns_strictly_earlier(monkeypatch: pytest.MonkeyPatch) -> None:
    opens = [date(2026, 6, 18), date(2026, 6, 22)]
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: opens)
    prev = tcu.previous_open_date(date(2026, 6, 22))
    assert prev == date(2026, 6, 18)


def test_previous_open_date_none_when_no_earlier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [date(2026, 6, 22)])
    prev = tcu.previous_open_date(date(2026, 6, 22))
    assert prev is None
