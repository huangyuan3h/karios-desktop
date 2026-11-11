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


def test_etf_trail_exit_pending_next_morning_without_today_bar():
    """2026-11-05 hole: 11-03 close breached, no 11-05 bar yet.

    The latest completed close at/before ``day`` owns the signal; a morning
    rebuild must still say SELL_TO_REPO (pending open sell), never HOLD.
    """
    held = {
        "symbol": "ETF:513350",
        "ts_code": "513350.SH",
        "entryDate": "2026-10-09",
    }
    bars = [
        {"date": "2026-10-09", "trade_date": "2026-10-09", "close": 1.420},
        {"date": "2026-10-30", "trade_date": "2026-10-30", "close": 1.480},
        {"date": "2026-11-03", "trade_date": "2026-11-03", "close": 1.353},  # -8.6%
    ]
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2026-11-05")
    assert out is not None
    assert out["action"] == "SELL_TO_REPO"
    assert out["pendingOpenSell"] is True
    assert out["signalDate"] == "2026-11-03"
    assert "待开盘执行" in out["message"]
    assert "待开盘" in out["executeHint"]


def test_etf_trail_exit_stays_sticky_after_bounce():
    """Once fired, the exit stays pending until the leg leaves holdings."""
    held = {
        "symbol": "ETF:513350",
        "ts_code": "513350.SH",
        "entryDate": "2026-10-09",
    }
    bars = [
        {"date": "2026-10-09", "trade_date": "2026-10-09", "close": 1.420},
        {"date": "2026-10-30", "trade_date": "2026-10-30", "close": 1.480},
        {"date": "2026-11-03", "trade_date": "2026-11-03", "close": 1.353},
        {"date": "2026-11-04", "trade_date": "2026-11-04", "close": 1.470},  # bounce inside band
    ]
    with patch.object(mas, "_adjusted_series", _adj(bars)):
        out = mas._etf_trail_exit(held, day="2026-11-05")
    assert out is not None and out["action"] == "SELL_TO_REPO"
    assert out["pendingOpenSell"] is True
    assert out["signalDate"] == "2026-11-03"


def test_morning_sleeve_returns_pending_sell_not_hold():
    """build_multi_asset_sleeve: pick still matches the held leg (HOLD path)
    but a prior trail breach must win on the next morning."""
    held = {
        "symbol": "ETF:513350",
        "ts_code": "513350.SH",
        "entryDate": "2026-10-09",
        "positionPct": 70,
    }
    pick = {
        "key": "OIL",
        "ts": "513350.SH",
        "symbol": "ETF:513350",
        "name": "富国油气QDII",
        "mom60": 4.98,
        "above_ma200": True,
    }
    bars = [
        {"date": "2026-10-09", "trade_date": "2026-10-09", "close": 1.420},
        {"date": "2026-10-30", "trade_date": "2026-10-30", "close": 1.480},
        {"date": "2026-11-03", "trade_date": "2026-11-03", "close": 1.353},
    ]
    with (
        patch.object(mas, "_pick", return_value=pick),
        patch.object(mas, "_adjusted_series", _adj(bars)),
    ):
        out = mas.build_multi_asset_sleeve(day="2026-11-05", cn_block={}, holdings_override=[held])
    assert out["action"] == "SELL_TO_REPO"
    assert out["pendingOpenSell"] is True
    assert out["signalDate"] == "2026-11-03"
    assert out["symbol"] == "ETF:513350"
    assert out["executeHint"].startswith("待开盘")


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
        # Unified H2 gate: incumbent well behind the challenger -> rotate.
        patch.object(mas, "_held_leg_mom", return_value=-1.0),
    ):
        out = mas.build_multi_asset_sleeve(
            day="2026-03-01", cn_block=cn, holdings_override=[held, stock]
        )
    assert out["action"] == "ROTATE"
    assert out["idlePct"] == pytest.approx(10.0)
    assert out["parkPct"] == pytest.approx(50.0)


# ------------------------------------------------- 14:30 intraday trail watch --


def _preserved(entry: str, day: str, *, peak: float = 100.0, last: float = 95.0):
    """Closes that never breach at the close (peak → -5%): trail stays HOLD."""
    return [
        {"date": entry, "trade_date": entry, "close": peak},
        {"date": day, "trade_date": day, "close": last},
    ]


def test_intraday_watch_flags_print_under_threshold_without_changing_close() -> None:
    held = {"symbol": "ETF:513350", "ts_code": "513350.SH", "entryDate": "2026-10-01"}
    bars = _preserved("2026-10-01", "2026-11-03", peak=100.0, last=95.0)
    with patch.object(mas, "_raw_series", lambda ts, **kw: _adj(bars)(ts)):
        warn = mas.intraday_trail_watch(held, day="2026-11-04", px=91.5)
        near = mas.intraday_trail_watch(held, day="2026-11-04", px=92.5)
        calm = mas.intraday_trail_watch(held, day="2026-11-04", px=96.0)
    assert warn is not None
    assert warn["status"] == "breached"
    assert warn["thresholdPx"] == pytest.approx(92.0, abs=1e-6)
    assert warn["px"] == pytest.approx(91.5)
    assert "现价已达卖出条件" in warn["message"]
    assert "门槛价" in warn["message"]
    assert "卖出阈值价" not in warn["message"]
    # Near-miss band is a warning only, with the same next-open clock.
    assert near is not None and near["status"] == "near"
    assert "逼近" in near["message"]
    assert "明日开盘" in near["executeHint"]
    assert calm is None


def test_build_sleeve_keeps_hold_but_carries_intraday_warn() -> None:
    """A 14:30 print under the line must not flip `action`, only warn."""
    held = {
        "symbol": "ETF:513350",
        "ts_code": "513350.SH",
        "entryDate": "2026-10-01",
        "positionPct": 63.5,
    }
    pick = {
        "key": "OIL",
        "symbol": "ETF:513350",
        "name": "富国油气",
        "mom60": 4.98,
        "above_ma200": True,
    }
    cn = {
        "regime": "Strong",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
        "holdings": [held],
    }
    bars = _preserved("2026-10-01", "2026-11-03", peak=100.0, last=95.0)
    with (
        patch.object(mas, "_pick", return_value=pick),
        patch.object(mas, "_adjusted_series", _adj(bars)),
        patch.object(mas, "_raw_series", lambda ts, **kw: _adj(bars)(ts)),
        patch.object(mas, "latest_1430_print", return_value=91.2),
    ):
        out = mas.build_multi_asset_sleeve(day="2026-11-04", cn_block=cn, holdings_override=[held])
    assert out["action"] == "HOLD"  # close signal unchanged (no bar yet for 11-04)
    warn = out.get("intradayWarn") or {}
    assert warn.get("active") is True
    assert warn.get("status") == "breached"
    assert warn.get("thresholdPx") == pytest.approx(92.0, abs=1e-6)
    assert warn.get("px") == pytest.approx(91.2)


def test_zero_position_registry_row_is_not_a_held_leg() -> None:
    """A 0% parking ETF in the registry must not shadow the real parked leg.

    2026-11-05: the registry carried 513110 at 0% (watchlist row) plus 513350 at
    63.5%. The sleeve picked 513110 → no entryDate → "轮动 513110→513350" and
    the OIL trail8 never ran.
    """
    ghost = {"symbol": "ETF:513110", "ts_code": "513110.SH", "positionPct": 0.0}
    oil = {
        "symbol": "ETF:513350",
        "ts_code": "513350.SH",
        "entryDate": "2026-10-01",
        "positionPct": 63.5,
    }
    pick = {
        "key": "OIL",
        "symbol": "ETF:513350",
        "name": "富国油气",
        "mom60": 9.94,
        "above_ma200": True,
    }
    cn = {
        "regime": "Strong",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "s3Candidates": [],
    }
    bars = _bars_peak_then_drop(entry="2026-10-01", day="2026-11-03", peak=100.0, last=91.5)
    with (
        patch.object(mas, "_pick", return_value=pick),
        patch.object(mas, "_adjusted_series", _adj(bars)),
    ):
        out = mas.build_multi_asset_sleeve(
            day="2026-11-03", cn_block=cn, holdings_override=[ghost, oil]
        )
    assert out["action"] == "SELL_TO_REPO"
    assert out["symbol"] == "ETF:513350"
    assert out["trailExit"] is True
    assert "峰值回撤" in out["message"]


def test_intraday_watch_falls_back_to_latest_intraday_print() -> None:
    """No live_1430 row for the symbol → latest stored print still warns."""
    held = {"symbol": "ETF:513350", "ts_code": "513350.SH", "entryDate": "2026-10-01"}
    bars = _preserved("2026-10-01", "2026-11-03", peak=100.0, last=95.0)
    with (
        patch.object(mas, "_raw_series", lambda ts, **kw: _adj(bars)(ts)),
        patch.object(mas, "latest_1430_print", return_value=None),
        patch.object(mas, "latest_intraday_print", return_value=91.4),
    ):
        warn = mas.intraday_trail_watch_for_day(held, day="2026-11-04")
    assert warn is not None
    assert warn["status"] == "breached"
    assert warn["px"] == pytest.approx(91.4)
    assert warn["pxSource"] == "bar_5min_latest"

    with (
        patch.object(mas, "_raw_series", lambda ts, **kw: _adj(bars)(ts)),
        patch.object(mas, "latest_1430_print", return_value=91.0),
        patch.object(mas, "latest_intraday_print", return_value=91.4),
    ):
        warn2 = mas.intraday_trail_watch_for_day(held, day="2026-11-04")
    assert warn2 is not None and warn2["pxSource"] == "live_1430"
    assert warn2["px"] == pytest.approx(91.0)
