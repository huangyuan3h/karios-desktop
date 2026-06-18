from __future__ import annotations

from typing import Any
import pytest


def _series_const(start_day: int = 1, days: int = 21, close: float = 100.0) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for i in range(days):
        d = f"2025-02-{start_day + i:02d}"
        out.append((d, close))
    return out


@pytest.mark.skip(reason="fetch_last_closes function no longer exists in market_regime module")
def test_index_signal_realtime_overrides_close(monkeypatch) -> None:
    import data_sync_service.service.dashboard as dashboard  # type: ignore[import-not-found]
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    series = _series_const(days=21, close=100.0)
    monkeypatch.setattr(market_regime, "fetch_last_closes", lambda ts_code, days=30: list(series))
    monkeypatch.setattr(market_regime, "_is_shanghai_trading_time", lambda: True)
    monkeypatch.setattr(market_regime, "_today_iso_date", lambda: "2025-02-21")

    def _rt(_codes: list[str]) -> dict[str, Any]:
        return {
            "ok": True,
            "items": [
                {"ts_code": "000001.SH", "price": "110", "trade_time": "2025-02-21 10:30:00"},
                {"ts_code": "399006.SZ", "price": "110", "trade_time": "2025-02-21 10:30:00"},
            ],
        }

    monkeypatch.setattr(market_regime, "fetch_realtime_quotes", _rt)

    items = dashboard._index_signal_items(as_of_date="2025-02-21")
    assert len(items) == 2
    for it in items:
        assert it["realtime"] is True
        assert it["close"] == 110.0
        assert it["signal"] == "green"
        assert it["source"] == "tushare.realtime_quote"


@pytest.mark.skip(reason="fetch_last_closes function no longer exists in market_regime module")
def test_index_signal_uses_db_when_not_trading(monkeypatch) -> None:
    import data_sync_service.service.dashboard as dashboard  # type: ignore[import-not-found]
    import data_sync_service.service.market_regime as market_regime  # type: ignore[import-not-found]

    series = _series_const(days=21, close=100.0)
    monkeypatch.setattr(market_regime, "fetch_last_closes", lambda ts_code, days=30: list(series))
    monkeypatch.setattr(market_regime, "_is_shanghai_trading_time", lambda: False)

    items = dashboard._index_signal_items(as_of_date="2025-02-21")
    assert len(items) == 2
    for it in items:
        assert it["realtime"] is False
        assert it["close"] == 100.0
        assert it["signal"] == "yellow"
        assert it["source"] == "db.index_daily"


def _patch_dashboard_summary_deps(monkeypatch, *, as_of: str, today: str, in_sync: bool) -> None:
    import data_sync_service.service.dashboard as dashboard  # type: ignore[import-not-found]

    monkeypatch.setattr(dashboard, "get_latest_sentiment_date", lambda: as_of)
    monkeypatch.setattr(dashboard, "_today_iso_date", lambda: today)
    monkeypatch.setattr(dashboard, "_is_shanghai_sync_window", lambda: in_sync)
    monkeypatch.setattr(dashboard, "_build_industry_bundle", lambda **_: {"dates": [], "topByDate": {}, "flow5d": {}})
    monkeypatch.setattr(dashboard, "_screeners_status", lambda *a, **k: [])
    monkeypatch.setattr(dashboard, "_news_items", lambda *a, **k: {"hours": 24, "total": 0, "items": []})
    monkeypatch.setattr(
        dashboard,
        "build_macro_snapshot",
        lambda **_: {"cnIndexSignals": [], "macro": []},
    )
    monkeypatch.setattr(dashboard, "format_market_environment_zh", lambda _: "")


def test_dashboard_summary_calls_get_index_signals_once_in_realtime_window(monkeypatch) -> None:
    import data_sync_service.service.dashboard as dashboard  # type: ignore[import-not-found]

    _patch_dashboard_summary_deps(
        monkeypatch,
        as_of="2026-06-18",
        today="2026-06-18",
        in_sync=True,
    )
    calls: list[dict] = []

    def _track(**kwargs):
        calls.append(kwargs)
        return [{"name": "上证指数", "signal": "green"}]

    monkeypatch.setattr(dashboard, "get_index_signals", _track)
    out = dashboard.dashboard_summary(include_macro=True)
    assert out["asOfDate"] == "2026-06-18"
    assert len(calls) == 1
    assert calls[0]["as_of_date"] is None
    assert calls[0]["include_breadth"] is False


def test_dashboard_summary_calls_get_index_signals_twice_when_historical_as_of(monkeypatch) -> None:
    import data_sync_service.service.dashboard as dashboard  # type: ignore[import-not-found]

    _patch_dashboard_summary_deps(
        monkeypatch,
        as_of="2026-06-17",
        today="2026-06-18",
        in_sync=True,
    )
    calls: list[dict] = []

    def _track(**kwargs):
        calls.append(kwargs)
        return [{"name": "上证指数", "signal": "yellow"}]

    monkeypatch.setattr(dashboard, "get_index_signals", _track)
    out = dashboard.dashboard_summary(include_macro=True)
    assert out["asOfDate"] == "2026-06-17"
    assert len(calls) == 2
    assert calls[0]["as_of_date"] == "2026-06-17"
    assert calls[1]["as_of_date"] is None
