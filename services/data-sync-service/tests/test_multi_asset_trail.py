"""Live ETF trail8 (peak −8% → REPO) for Watchlist multi_asset_sleeve."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

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


def test_etf_trail_exit_fires_on_peak_drawdown():
    held = {
        "symbol": "ETF:513100",
        "ts_code": "513100.SH",
        "entryDate": "2026-01-01",
    }
    bars = _bars_peak_then_drop(entry="2026-01-01", day="2026-03-01", peak=100.0, last=91.0)
    with patch("data_sync_service.service.multi_asset_sleeve.fetch_last_bars", return_value=bars):
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
    with patch("data_sync_service.service.multi_asset_sleeve.fetch_last_bars", return_value=bars):
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
        patch.object(mas, "_stock_basket_mom_from_holdings", return_value=None),
        patch("data_sync_service.service.multi_asset_sleeve.fetch_last_bars", return_value=bars),
    ):
        out = mas.build_multi_asset_sleeve(day="2026-03-01", cn_block=cn, holdings_override=[held])
    assert out["action"] == "SELL_TO_REPO"
    assert "峰值回撤" in out["message"]
