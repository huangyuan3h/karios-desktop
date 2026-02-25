from __future__ import annotations

from typing import Any


def _series_const(start_day: int = 1, days: int = 21, close: float = 100.0) -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    for i in range(days):
        d = f"2025-02-{start_day + i:02d}"
        out.append((d, close))
    return out


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
