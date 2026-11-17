"""Regime-target rule: deterministic unit coverage + API shape.

No real market rows are required. The CI database is empty, so the previous
version of this test (which hit ``index_daily`` for 000300.SH) failed there
while passing on a populated dev DB. Both the closes fetch and the trading-day
lookup are patched so the assertions only exercise the rule itself.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]
from data_sync_service.service import regime_switch  # type: ignore[import-not-found]


def _business_days(end: date, count: int) -> list[date]:
    out: list[date] = []
    d = end
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d)
        d -= timedelta(days=1)
    return list(reversed(out))


def _bars(end: date, count: int, *, rising: bool) -> list[tuple[str, float]]:
    rows: list[tuple[str, float]] = []
    close = 3000.0
    for d in _business_days(end, count):
        close *= 1.004 if rising else 0.996
        rows.append((d.isoformat(), round(close, 2)))
    return rows


def _weekday_floor(_exchange: str, d: date) -> date:
    """Calendar-free stand-in for ``trade_calendar.last_trading_day``."""
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


@pytest.fixture(autouse=True)
def _patch_clock_independent_deps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(regime_switch, "last_trading_day", _weekday_floor)


def _patch_bars(monkeypatch: pytest.MonkeyPatch, rows: list[tuple[str, float]]) -> None:
    monkeypatch.setattr(regime_switch, "fetch_last_closes_upto", lambda *_a, **_k: rows)


def test_bull_month_end_targets_robust_starship(monkeypatch: pytest.MonkeyPatch) -> None:
    # 400 rising weekday bars ending on Friday 2026-10-30 (last trading day of
    # October 2026); a mid-November as-of keeps that month-end as confirmed.
    _patch_bars(monkeypatch, _bars(date(2026, 10, 30), 400, rising=True))

    payload = regime_switch.get_regime_target("2026-11-13")

    assert payload["ok"] is True
    assert payload["confirmedAt"] == "2026-10-30"
    assert payload["state"] == "bull"
    assert payload["target"] == "starship_robust"
    assert payload["sgapExposure"] == "high"
    assert payload["close"] > payload["ma200"]
    assert payload["distancePct"] > 0
    assert payload["nextReview"] >= payload["asOf"]


def test_bear_month_end_targets_starport(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_bars(monkeypatch, _bars(date(2026, 10, 30), 400, rising=False))

    payload = regime_switch.get_regime_target("2026-11-13")

    assert payload["ok"] is True
    assert payload["state"] == "bear"
    assert payload["target"] == "starport"
    assert payload["sgapExposure"] == "low"
    assert payload["close"] < payload["ma200"]
    assert "S-gap" not in payload["note"]


def test_insufficient_history_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_bars(monkeypatch, _bars(date(2026, 10, 30), 50, rising=True))

    payload = regime_switch.get_regime_target("2026-11-13")

    assert payload["ok"] is False
    assert "bars < 200" in payload["error"]
    assert payload["rule"] == regime_switch.RULE


def test_missing_previous_month_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    # 250 bars through September, then a single mid-November print: the
    # unfinished month has no completed predecessor with bars.
    rows = _bars(date(2026, 9, 30), 250, rising=True)
    rows.append((date(2026, 11, 10).isoformat(), rows[-1][1]))
    _patch_bars(monkeypatch, rows)

    payload = regime_switch.get_regime_target("2026-11-13")

    assert payload["ok"] is False
    assert payload["error"] == "no previous month-end bar"


def test_not_enough_bars_before_confirmed_month_end(monkeypatch: pytest.MonkeyPatch) -> None:
    # 199 bars up to the end of August, then 10 bars in the (unfinished)
    # September window: the confirmed month-end sits too early for MA200.
    rows = _bars(date(2025, 8, 29), 199, rising=True)
    rows += _bars(date(2025, 9, 12), 10, rising=True)
    _patch_bars(monkeypatch, rows)

    payload = regime_switch.get_regime_target("2025-09-12")

    assert payload["ok"] is False
    assert "not enough bars before confirmed month-end" in payload["error"]


def test_api_fails_open_when_service_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise RuntimeError("db down")

    monkeypatch.setattr(regime_switch, "get_regime_target", _boom)
    client = TestClient(app)

    resp = client.get("/watchlist/regime-target?as_of=2026-11-13")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is False
    assert "db down" in payload["error"]
    assert payload["rule"].startswith("月末确认")


def test_api_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_bars(monkeypatch, _bars(date(2026, 10, 30), 400, rising=True))
    client = TestClient(app)

    resp = client.get("/watchlist/regime-target?as_of=2026-11-13")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["state"] in {"bull", "bear"}
    assert payload["target"] in {"starship_robust", "starport"}
    assert payload["confirmedAt"] <= payload["asOf"]
    assert payload["nextReview"] >= payload["asOf"]
    assert "MA200" in payload["rule"]
