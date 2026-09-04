"""health_routes._relax_extra_hours tests (freshness relax on non-trading streaks)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from data_sync_service.api import health_routes as hr

UTC8 = timezone(timedelta(hours=8))


def _opens(days):
    from datetime import date

    return [date.fromisoformat(d) for d in days]


def test_saturday_relaxes_48(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fri 2026-09-04 open; Sat 09-05 → streak 1 → weekend floor 48.
    monkeypatch.setattr(
        "data_sync_service.db.trade_calendar.get_open_dates",
        lambda **kw: _opens(["2026-08-24", "2026-09-04"]),
    )
    now = datetime(2026, 9, 5, 10, 0, tzinfo=UTC8)
    assert hr._relax_extra_hours(now) == 48


def test_holiday_monday_relaxes_72(monkeypatch: pytest.MonkeyPatch) -> None:
    # 3-day weekend+holiday streak → 72.
    monkeypatch.setattr(
        "data_sync_service.db.trade_calendar.get_open_dates",
        lambda **kw: _opens(["2026-09-04"]),
    )
    now = datetime(2026, 9, 7, 10, 0, tzinfo=UTC8)  # Monday holiday
    assert hr._relax_extra_hours(now) == 72


def test_open_day_no_relax(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.trade_calendar.get_open_dates",
        lambda **kw: _opens(["2026-09-04", "2026-09-07", "2026-09-08"]),
    )
    now = datetime(2026, 9, 8, 10, 0, tzinfo=UTC8)  # Tuesday open
    assert hr._relax_extra_hours(now) == 0


def test_unseeded_falls_back_to_weekend_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.trade_calendar.get_open_dates", lambda **kw: []
    )
    assert hr._relax_extra_hours(datetime(2026, 9, 5, 10, 0, tzinfo=UTC8)) == 48
    assert hr._relax_extra_hours(datetime(2026, 9, 7, 10, 0, tzinfo=UTC8)) == 0
