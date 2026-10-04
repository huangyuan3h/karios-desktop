"""Extra coverage for trade_calendar_utils uncovered branches (M30 gap-closure).

Covers clamp/nth/fallback/session-counting paths with faked calendars.
All assertions are behavioral (dates returned), no padding.
"""

from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.service import trade_calendar_utils as tcu


def test_clamp_to_last_open_date_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        tcu, "last_open_date_on_or_before", lambda d, exchange="SSE": date(2026, 6, 18)
    )
    assert tcu.clamp_to_last_open_date("2026-06-20") == "2026-06-18"


def test_clamp_to_last_open_date_invalid_returns_input() -> None:
    assert tcu.clamp_to_last_open_date("not-a-date") == "not-a-date"


def test_clamp_to_last_open_date_no_calendar_keeps_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "last_open_date_on_or_before", lambda d, exchange="SSE": None)
    assert tcu.clamp_to_last_open_date("2026-06-22") == "2026-06-22"


def test_nth_open_date_from_happy(monkeypatch: pytest.MonkeyPatch) -> None:
    opens = [date(2026, 6, 16), date(2026, 6, 17), date(2026, 6, 18)]
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: opens)
    assert tcu.nth_open_date_from("2026-06-16", 2) == "2026-06-17"


def test_nth_open_date_from_bad_inputs() -> None:
    assert tcu.nth_open_date_from("bad-date", 2) is None
    assert tcu.nth_open_date_from("2026-06-16", 0) is None
    assert tcu.nth_open_date_from("2026-06-16", -1) is None


def test_nth_open_date_from_calendar_error_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(**_):
        raise RuntimeError("db down")

    monkeypatch.setattr(tcu, "get_open_dates", boom)
    assert tcu.nth_open_date_from("2026-06-16", 2) is None


def test_nth_open_date_from_window_too_short_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [date(2026, 6, 16)])
    assert tcu.nth_open_date_from("2026-06-16", 3) is None


def test_trade_dates_upto_bad_date_falls_back_to_today(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opens = [date(2026, 6, 18), date(2026, 6, 19)]
    monkeypatch.setattr(tcu, "shanghai_today", lambda: date(2026, 6, 19))
    monkeypatch.setattr(tcu, "last_open_date_on_or_before", lambda d, exchange="SSE": d)
    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: True)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: opens)
    out = tcu.trade_dates_upto("bad-date", 2)
    assert out == ["2026-06-18", "2026-06-19"]


def test_trade_dates_upto_empty_when_no_calendar_no_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "last_open_date_on_or_before", lambda d, exchange="SSE": d)
    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: None)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [])
    assert tcu.trade_dates_upto("2026-06-22", 3) == []


def test_resolve_effective_as_of_invalid_returns_today(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "shanghai_today_iso", lambda: "2026-06-22")
    assert tcu.resolve_effective_as_of("bad-date") == "2026-06-22"


def test_resolve_effective_as_of_no_calendar_returns_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "last_open_date_on_or_before", lambda d: None)
    assert tcu.resolve_effective_as_of("2026-06-20") == "2026-06-20"


def test_mon_fri_between_skips_weekend() -> None:
    out = tcu._mon_fri_between(date(2026, 6, 19), date(2026, 6, 22))
    # 2026-06-19 Fri, 20 Sat, 21 Sun, 22 Mon
    assert out == [date(2026, 6, 19), date(2026, 6, 22)]


def test_calendar_seeded_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    # Seeded via is_trading_day hit
    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: True)
    assert tcu._calendar_seeded("SSE", date(2026, 6, 22)) is True
    # Seeded via open dates fallback
    monkeypatch.setattr(tcu, "is_trading_day", lambda exchange, d: None)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [date(2026, 6, 22)])
    assert tcu._calendar_seeded("SSE", date(2026, 6, 22)) is True
    # Empty -> not seeded
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: [])
    assert tcu._calendar_seeded("SSE", date(2026, 6, 22)) is False

    # Exception -> not seeded
    def boom(**_):
        raise RuntimeError("db down")

    monkeypatch.setattr(tcu, "get_open_dates", boom)
    assert tcu._calendar_seeded("SSE", date(2026, 6, 22)) is False


def test_open_sessions_between_invalid_and_range_guards() -> None:
    assert tcu.open_sessions_between("bad", "2026-06-22") == []
    assert tcu.open_sessions_between("2026-06-23", "2026-06-22") == []
    # Span > 370 days rejected
    assert tcu.open_sessions_between("2024-01-01", "2026-06-22") == []


def test_open_sessions_between_seeded(monkeypatch: pytest.MonkeyPatch) -> None:
    opens = [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)]
    monkeypatch.setattr(tcu, "_calendar_seeded", lambda exchange, probe: True)
    monkeypatch.setattr(tcu, "get_open_dates", lambda **_: opens)
    out = tcu.open_sessions_between("2026-06-18", "2026-06-22")
    assert out == ["2026-06-18", "2026-06-19", "2026-06-22"]


def test_open_sessions_between_fallback_mon_fri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tcu, "_calendar_seeded", lambda exchange, probe: False)
    out = tcu.open_sessions_between("2026-06-19", "2026-06-22")
    assert out == ["2026-06-19", "2026-06-22"]


def test_count_and_nth_open_session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tcu, "_calendar_seeded", lambda exchange, probe: False)
    assert tcu.count_open_sessions("2026-06-19", "2026-06-22") == 2
    assert tcu.nth_open_session("2026-06-19", 1) == "2026-06-19"
    assert tcu.nth_open_session("2026-06-19", 2) == "2026-06-22"
    assert tcu.nth_open_session("2026-06-19", 0) is None
    assert tcu.nth_open_session("bad-date", 1) is None
    # Beyond available window -> None (n huge still bounded by 370d span)
    assert tcu.nth_open_session("2026-06-19", 10_000) is None
