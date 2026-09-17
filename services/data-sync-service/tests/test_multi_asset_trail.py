"""Live ETF trail8 (peak −8% → REPO) for Watchlist multi_asset_sleeve."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

import pytest

from data_sync_service.service import multi_asset_sleeve as mas


def _bars_peak_then_drop(*, entry: str, day: str, peak: float = 100.0, last: float = 91.0):
    """Bars from entry..day: climb to peak then finish at ``last`` on ``day``."""
    d0 = date.fromisoformat(entry)
    d1 = date.fromisoformat(day)
    bars = []
    n = (d1 - d0).days
    for i in range(n):
        d = d0 + timedelta(days=i)
        # mid ramp to peak
        c = peak if i >= n // 2 else peak * 0.95
        bars.append({"date": d.isoformat(), "trade_date": d.isoformat(), "close": c})
    bars.append({"date": day, "trade_date": day, "close": last})
    return bars


def _adj(bars):
    """Patch seam: engine-basis series from the fake bars."""
    return lambda ts: {b["date"]: float(b["close"]) for b in bars}


def test_etf_trail_exit_fires_on_peak_drawdown():
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2026-01-01",
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=91.0)
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2026-03-01")
    assert out is not None
    assert out["action"] == "SELL_TO_REPO"
    assert "峰值回撤" in out["message"]


def test_etf_trail_exit_skips_when_within_band():
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2026-01-01",
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=95.0)
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2026-03-01")
    assert out is None


def test_trail_beats_rotate_when_holding_etf():
    """Holding ETF in drawdown → SELL_TO_REPO even if another ETF would win."""
    held = {
        "symbol": "ETF:518880",
        "ts_code": "518880.SH",
        "entryDate": "2026-01-01",
        "positionPct": 30,
    }
    pick = {
        "key": "NASDAQ",
        "symbol": "ETF:513100",
        "name": "纳指",
        "mom60": 12.0,
        "above_ma200": True,
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=90.0)
    cn = {
        "regime": "Weak",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
        "holdings": [held],
    }
    with (
        patch.object(mas, "_pick", return_value=pick),
        patch.object(mas, "_adjusted_series", _adj(bars)),
    ):
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=cn, holdings_override=[held])
    assert out["action"] == "SELL_TO_REPO"
    assert "峰值回撤" in out["message"]


def test_trail_ignores_bars_after_decision_day():
    """Regression (look-ahead): closes after `day` must not enter the peak.

    Bars 100 → 99 (day) → 200 (future). Without the as-of cut the fake peak
    200 would trigger a phantom -50.5% trail exit.
    """
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2026-01-01",
    }
    bars = [
        {"date": "2026-01-10", "trade_date": "2026-01-10", "close": 100.0},
        {"date": "2026-03-01", "trade_date": "2026-03-01", "close": 99.0},
        {"date": "2026-04-01", "trade_date": "2026-04-01", "close": 200.0},
    ]
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2026-03-01")
    assert out is None


def test_trail_falls_back_to_raw_daily_when_panel_empty():
    """Engine-basis panel unavailable → raw daily path still works."""
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2026-01-01",
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=91.0)
    with (
        patch.object(mas, "_adjusted_series", lambda ts: {}),
        patch("data_sync_service.service.multi_asset_sleeve.fetch_last_bars", return_value=bars),
    ):
        out = mas._etf_trail_exit(held, day="2026-03-01")
    assert out is not None and out["action"] == "SELL_TO_REPO"


def test_trail_peak_starts_at_signal_day_close():
    """Engine parity (2024-08-02 NASDAQ case): the frozen replay holds the leg
    from the rotation signal-day close, so the peak reference must include the
    session before the Live fill day.

    Signal-day close 7.3178 → fill day 6.9827 → 08-05 6.4374: engine drawdown
    12.03% (exit), fill-day-only 7.81% (no exit). Live must reproduce 12.03%.
    """
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2024-08-02",  # fill day (signal day = 2024-08-01)
    }
    bars = [
        {"date": "2024-08-01", "trade_date": "2024-08-01", "close": 7.3178},
        {"date": "2024-08-02", "trade_date": "2024-08-02", "close": 6.9827},
        {"date": "2024-08-05", "trade_date": "2024-08-05", "close": 6.4374},
    ]
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2024-08-05")
    assert out is not None and out["action"] == "SELL_TO_REPO"
    assert "12.0%" in out["message"]


def test_rotate_park_pct_includes_held_leg():
    """ROTATE target size = idle + the leg being replaced (not 0% when parked)."""
    held = {
        "symbol": "ETF:518880",
        "ts_code": "518880.SH",
        "entryDate": "2026-01-01",
        "sleeve_pct": 40.0,
    }
    stock = {"symbol": "CN:600000", "ts_code": "600000.SH", "sleeve_pct": 50.0}
    pick = {
        "key": "NASDAQ",
        "symbol": "ETF:513100",
        "name": "纳指",
        "mom60": 12.0,
        "above_ma200": True,
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=95.0)
    cn = {
        "regime": "Weak",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
    }
    with (
        patch.object(mas, "_pick", return_value=pick),
        patch.object(mas, "_adjusted_series", _adj(bars)),
    ):
        out = mas.build_multi_asset_sleeve(
            day="2026-03-01", cn_block=cn, holdings_override=[held, stock]
        )
    assert out["action"] == "ROTATE"
    assert out["idlePct"] == pytest.approx(10.0)
    assert out["parkPct"] == pytest.approx(50.0)
