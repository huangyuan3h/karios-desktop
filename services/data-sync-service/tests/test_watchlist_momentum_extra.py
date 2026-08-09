"""service/watchlist_momentum_alerts.py coverage."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from data_sync_service.service import watchlist_momentum_alerts as wma


def _bars(n: int = 88, vol: float = 1e6) -> list[tuple[str, str, str, str, str, str]]:
    d0 = date(2026, 1, 1)
    prev = 100.0
    closes = []
    for i in range(n):
        if i % 6 == 5 and i < n - 1:
            prev = prev * 0.97
        else:
            prev = prev * 1.015
        closes.append(prev)
    bars = []
    for i, c in enumerate(closes):
        dt = (d0 + timedelta(days=i)).isoformat()
        v = vol * 1.5 if i == n - 1 else vol
        bars.append((dt, f"{c * 0.998:.3f}", f"{c * 1.004:.3f}", f"{c * 0.993:.3f}", f"{c:.3f}", f"{v:.0f}"))
    return bars


def _crash_bars(n: int = 80) -> list[tuple[str, str, str, str, str, str]]:
    d0 = date(2026, 1, 1)
    bars = []
    for i in range(n):
        if i < 60:
            close = 100.0 * (1.015 ** i)
        else:
            close = 100.0 * (1.015 ** 60) * (1.0 - 0.05 * (i - 60))
        dt = (d0 + timedelta(days=i)).isoformat()
        bars.append((dt, f"{close * 0.998:.3f}", f"{close * 1.002:.3f}", f"{close * 0.995:.3f}", f"{close:.3f}", f"{1e6:.0f}"))
    return bars


class TestHelpers:
    def test_safe_float(self) -> None:
        assert wma._safe_float(None) is None
        assert wma._safe_float("1.5") == 1.5
        assert wma._safe_float("x") is None
        assert wma._safe_float(float("nan")) is None

    def test_get_regime(self, monkeypatch) -> None:
        monkeypatch.setattr(wma, "get_market_regime", lambda **kw: {"regime": "Strong"})
        assert wma._get_regime("2026-08-07") == "Strong"
        monkeypatch.setattr(wma, "get_market_regime", lambda **kw: (_ for _ in ()).throw(RuntimeError("down")))
        assert wma._get_regime(None) == "Weak"

    def test_latest_bar_date(self) -> None:
        assert wma._latest_bar_date({}) is None
        assert wma._latest_bar_date({"a": [], "b": [("2026-08-01",), ("2026-08-02",)]}) == "2026-08-02"

    def test_regime_target(self) -> None:
        assert wma._regime_target("Strong") == 0.25
        assert wma._regime_target("Diverging") == 0.15
        assert wma._regime_target("Weak") == 0.05
        assert wma._regime_target("Unknown") == 0.05

    def test_next_tranche(self) -> None:
        assert wma._next_tranche(0.0, 0.0) == 0.0
        base = 0.15
        step = 0.05
        assert wma._next_tranche(0.0, base) == pytest.approx(step)
        assert wma._next_tranche(step * 0.95, base) == pytest.approx(step * 2)
        assert wma._next_tranche(step * 2.5, base) == pytest.approx(base)

    def test_quote_trade_date(self) -> None:
        assert wma._quote_trade_date({}) is None
        assert wma._quote_trade_date({"trade_time": "2026-08-07 15:00:00"}) == "2026-08-07"
        assert wma._quote_trade_date({"trade_time": "20260807 15:00:00"}) == "2026-08-07"
        assert wma._quote_trade_date({"trade_time": "junk"}) is None

    def test_merge_realtime_bar(self) -> None:
        bars = _bars(5)
        merged = wma._merge_realtime_bar(bars, {"price": "150.0", "trade_time": "2026-08-06 15:00:00", "volume": "5"})
        assert len(merged) == 6 and merged[-1][4] == "150.0"
        same = wma._merge_realtime_bar(bars, {"price": "150.0", "trade_time": f"{bars[-1][0]} 15:00:00"})
        assert len(same) == 5
        assert wma._merge_realtime_bar(bars, {"price": None}) == bars
        assert wma._merge_realtime_bar([], {"price": "1.0"}) == []
        older = wma._merge_realtime_bar(bars, {"price": "1.0", "trade_time": "2026-01-01 00:00:00"})
        assert len(older) == 5


class TestCompute:
    def _patch(self, monkeypatch, bars_by_code=None, regime="Strong"):
        monkeypatch.setattr(wma, "symbol_to_ts_code", lambda sym: {
            "CN:600519": "600519.SH", "CN:000001": "000001.SZ", "US:APPL": None,
        }.get(sym))
        bars_by_code = bars_by_code or {
            "600519.SH": _bars(),
            "000001.SZ": _crash_bars(),
        }
        monkeypatch.setattr(wma, "fetch_last_ohlcv_batch", lambda codes, days: {c: bars_by_code.get(c, []) for c in codes})
        monkeypatch.setattr(wma, "fetch_realtime_quotes", lambda codes: {"ok": True, "items": [
            {"ts_code": "600519.SH", "price": "150.0", "trade_time": "2026-08-07 14:00:00", "volume": "2e6"},
        ]})
        monkeypatch.setattr(wma, "_get_regime", lambda d: regime)

    def test_empty(self, monkeypatch) -> None:
        assert wma.compute_watchlist_momentum_alerts([]) == []
        assert wma.compute_watchlist_momentum_alerts([{}]) == []

    def test_full_breakout(self, monkeypatch) -> None:
        self._patch(monkeypatch, regime="Strong")
        monkeypatch.setattr(wma, "fetch_realtime_quotes", lambda codes: {"ok": False})
        items = [{"symbol": "CN:600519", "position_pct": 0.05, "entry_price": 100.0, "max_price": 120.0}]
        out = wma.compute_watchlist_momentum_alerts(items, realtime=True)
        assert out[0]["action"] == "buy_add"
        assert out[0]["regime"] == "Strong"

    def test_full_exit(self, monkeypatch) -> None:
        self._patch(monkeypatch, regime="Weak")
        items = [{"symbol": "CN:000001", "position_pct": 0.1, "entry_price": 110.0}]
        out = wma.compute_watchlist_momentum_alerts(items, realtime=False)
        assert out[0]["action"] == "exit"
        assert out[0]["reason"] == "trend_weak"
        assert out[0]["targetPct"] == 0.0

    def test_hold_when_target_reached(self, monkeypatch) -> None:
        self._patch(monkeypatch, regime="Strong")
        items = [{"symbol": "CN:600519", "position_pct": 0.30, "entry_price": 100.0, "max_price": 120.0}]
        out = wma.compute_watchlist_momentum_alerts(items)
        assert out[0]["action"] == "hold"
        assert out[0]["reason"] == "no_action"
        assert out[0]["targetPct"] == pytest.approx(0.3)

    def test_unsupported_market(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        out = wma.compute_watchlist_momentum_alerts([{"symbol": "US:APPL"}])
        assert out[0]["missingData"] == ["unsupported_market"]

    def test_no_bars(self, monkeypatch) -> None:
        self._patch(monkeypatch, bars_by_code={"600519.SH": []})
        out = wma.compute_watchlist_momentum_alerts([{"symbol": "CN:600519"}])
        assert out[0]["missingData"] == ["no_bars"]

    def test_bars_lt_60(self, monkeypatch) -> None:
        self._patch(monkeypatch, bars_by_code={"600519.SH": _bars(20)})
        out = wma.compute_watchlist_momentum_alerts([{"symbol": "CN:600519"}])
        assert out[0]["missingData"] == ["bars_lt_60"]

    def test_realtime_not_ok(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        monkeypatch.setattr(wma, "fetch_realtime_quotes", lambda codes: {"ok": False})
        out = wma.compute_watchlist_momentum_alerts([{"symbol": "CN:600519", "position_pct": 0.05}], realtime=True)
        assert out[0]["action"] in ("buy_add", "hold")

    def test_position_clamped(self, monkeypatch) -> None:
        self._patch(monkeypatch)
        out = wma.compute_watchlist_momentum_alerts([{"symbol": "CN:600519", "position_pct": 5.0}])
        assert out[0]["currentPct"] == 1.0
