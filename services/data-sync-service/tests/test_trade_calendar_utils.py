"""Unit tests for trade_calendar_utils."""

from __future__ import annotations

from datetime import date

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
