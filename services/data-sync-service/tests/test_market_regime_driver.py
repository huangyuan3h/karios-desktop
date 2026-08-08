"""market_regime: technical helpers + context drivers."""

from __future__ import annotations

import datetime

from data_sync_service.service import market_regime as mr


def test_ema_short_input() -> None:
    assert mr._ema([1.0, 2.0], 5) == []


def test_ema_produces_series() -> None:
    out = mr._ema([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 3)
    assert len(out) == 4  # 6 - 3 + 1


def test_macd_histogram_short_input() -> None:
    assert mr._macd_histogram([1.0] * 30, slow=26, signal=9) == []


def test_macd_histogram_ok() -> None:
    closes = [10.0 + i * 0.1 for i in range(60)]
    hist = mr._macd_histogram(closes)
    assert isinstance(hist, list)
    assert len(hist) > 0


def test_get_trade_minutes_buckets() -> None:
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 9, 0)) == 0
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 10, 0)) == 30
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 11, 30)) == 120
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 12, 0)) == 120
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 14, 0)) == 180
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 15, 0)) == 240
    assert mr._get_trade_minutes(datetime.datetime(2026, 8, 4, 20, 0)) == 240


def test_estimate_full_day_volume() -> None:
    assert mr._estimate_full_day_volume(1000.0, 0) is None
    assert mr._estimate_full_day_volume(1000.0, 120) == 2000.0


def test_trade_date_from_trade_time() -> None:
    assert mr._trade_date_from_trade_time(None) is None
    assert mr._trade_date_from_trade_time("2026-08-04T10:00:00+08:00") == "2026-08-04"
    assert mr._trade_date_from_trade_time("garbage") is None


def test_hsi_series_stale() -> None:
    import datetime as dt

    today = dt.date.today().isoformat()
    assert mr._hsi_series_stale([(today, 100.0)]) is False
    assert mr._hsi_series_stale([("2020-01-01", 100.0)]) is True
    assert mr._hsi_series_stale([]) is True


def test_merge_on_demand_into_series() -> None:
    series = [("2026-08-01", 100.0), ("2026-08-03", 102.0)]
    merged = mr._merge_on_demand_into_series(
        series, {"asOfDate": "2026-08-04", "close": 104.0}
    )
    assert dict(merged)["2026-08-04"] == 104.0
    # same-day replacement
    replaced = mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-03", "close": 103.0})
    assert dict(replaced)["2026-08-03"] == 103.0
    # stale asOf → unchanged
    assert mr._merge_on_demand_into_series(series, {"asOfDate": "2020-01-01", "close": 1.0}) == series
    # missing close → unchanged
    assert mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-05"}) == series


def test_hsi_source_label() -> None:
    assert mr._hsi_source_label(used_realtime=False, on_demand_src=None) == "db.macro_daily"
    assert mr._hsi_source_label(used_realtime=True, on_demand_src=None) == "tushare.realtime_quote"
    assert mr._hsi_source_label(used_realtime=False, on_demand_src="manual") == "manual"


def test_safe_float_and_realtime_pct() -> None:
    assert mr._safe_float(None) is None
    assert mr._safe_float("12.5") == 12.5
    assert mr._safe_float("bad") is None

    assert mr._realtime_pct_or_price({"pct_chg": 1.5, "price": 10.0}) == (1.5, 10.0)
    assert mr._realtime_pct_or_price({"price": 10.0, "pre_close": 8.0}) == (25.0, 10.0)
    assert mr._realtime_pct_or_price({}) == (None, None)
