from __future__ import annotations

from datetime import date, timedelta

from data_sync_service.service import regime_switch


def _weekdays(start: date, end: date) -> list[date]:
    out = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _rows(start: date, end: date, *, rising: bool) -> list[tuple[str, float]]:
    days = _weekdays(start, end)
    out = []
    for i, d in enumerate(days):
        base = 3000.0 + i * 1.0 if rising else 5000.0 - i * 1.0
        out.append((d.isoformat(), base))
    return out


def _patch(monkeypatch, rows):
    def fake_fetch(ts_code, as_of_date, days=60):
        assert ts_code == "000300.SH"
        return [r for r in rows if r[0] <= as_of_date]

    def fake_last_trading_day(exchange, on_date):
        d = on_date
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d

    monkeypatch.setattr(regime_switch, "fetch_last_closes_upto", fake_fetch)
    monkeypatch.setattr(regime_switch, "last_trading_day", fake_last_trading_day)


def test_bull_uses_previous_completed_month_end(monkeypatch):
    _patch(monkeypatch, _rows(date(2025, 1, 1), date(2026, 11, 13), rising=True))
    out = regime_switch.get_regime_target("2026-11-13")
    assert out["ok"] is True
    assert out["state"] == "bull"
    assert out["target"] == "starship_robust"
    assert out["confirmedAt"] == "2026-10-30"
    assert out["nextReview"] == "2026-11-30"
    assert out["sgapExposure"] == "high"


def test_bear_targets_starport(monkeypatch):
    _patch(monkeypatch, _rows(date(2025, 1, 1), date(2026, 11, 13), rising=False))
    out = regime_switch.get_regime_target("2026-11-13")
    assert out["ok"] is True
    assert out["state"] == "bear"
    assert out["target"] == "starport"
    assert out["sgapExposure"] == "low"


def test_month_end_as_of_uses_current_month(monkeypatch):
    _patch(monkeypatch, _rows(date(2025, 1, 1), date(2026, 10, 30), rising=True))
    out = regime_switch.get_regime_target("2026-10-30")
    assert out["ok"] is True
    assert out["confirmedAt"] == "2026-10-30"


def test_short_history_fails_open(monkeypatch):
    _patch(monkeypatch, _rows(date(2026, 10, 1), date(2026, 11, 13), rising=True))
    out = regime_switch.get_regime_target("2026-11-13")
    assert out["ok"] is False
    assert "200" in out["error"]
