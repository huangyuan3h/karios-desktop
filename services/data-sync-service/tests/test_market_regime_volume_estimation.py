"""Tests for volume estimation logic in index signals.

Key functions:
- _get_trade_minutes(): elapsed trading minutes
- _estimate_full_day_volume(): estimate full-day volume from partial
- Signal logic using estimated volume for yellow/green/deep_green
"""

from datetime import datetime
from zoneinfo import ZoneInfo


def _mock_liquidity_not_ok(**kwargs):
    return {
        "total_turnover_cny": 0.0,
        "max_industry_inflow": 0.0,
        "turnover_above_1_5T": False,
        "mainline_inflow_above_5B": False,
    }


def _mock_liquidity_ok(**kwargs):
    return {
        "total_turnover_cny": 2.0e12,
        "max_industry_inflow": 6e9,
        "turnover_above_1_5T": True,
        "mainline_inflow_above_5B": True,
    }


class TestGetTradeMinutes:
    """Test _get_trade_minutes() at various time points."""

    def test_before_open(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 9, 15, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 0

    def test_at_open_930(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 9, 30, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 0

    def test_10min_after_open(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 9, 40, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 10

    def test_mid_morning(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 10, 30, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 60

    def test_at_morning_close_1130(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 11, 30, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 120

    def test_lunch_break_12pm(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 12, 0, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 120

    def test_lunch_break_1230(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 12, 30, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 120

    def test_afternoon_open_13pm(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 13, 0, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 120

    def test_mid_afternoon(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 14, 0, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 180

    def test_at_close_3pm(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 15, 0, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 240

    def test_after_close_4pm(self) -> None:
        import data_sync_service.service.market_regime as mr

        tz = ZoneInfo("Asia/Shanghai")
        t = datetime(2026, 4, 14, 16, 0, 0, tzinfo=tz)
        assert mr._get_trade_minutes(t) == 240


class TestEstimateFullDayVolume:
    """Test _estimate_full_day_volume() with various scenarios."""

    def test_no_trade_minutes_returns_none(self) -> None:
        import data_sync_service.service.market_regime as mr

        result = mr._estimate_full_day_volume(1e9, 0)
        assert result is None

    def test_half_day_estimate(self) -> None:
        import data_sync_service.service.market_regime as mr

        current_vol = 1e9
        trade_minutes = 120
        result = mr._estimate_full_day_volume(current_vol, trade_minutes)
        assert result is not None
        assert result == 1e9 * 240.0 / 120.0
        assert result == 2e9

    def test_quarter_day_estimate(self) -> None:
        import data_sync_service.service.market_regime as mr

        current_vol = 5e8
        trade_minutes = 60
        result = mr._estimate_full_day_volume(current_vol, trade_minutes)
        assert result is not None
        assert result == 5e8 * 240.0 / 60.0
        assert result == 2e9

    def test_full_day_no_change(self) -> None:
        import data_sync_service.service.market_regime as mr

        current_vol = 1e9
        trade_minutes = 240
        result = mr._estimate_full_day_volume(current_vol, trade_minutes)
        assert result is not None
        assert result == 1e9

    def test_one_minute_estimate(self) -> None:
        import data_sync_service.service.market_regime as mr

        current_vol = 1e6
        trade_minutes = 1
        result = mr._estimate_full_day_volume(current_vol, trade_minutes)
        assert result is not None
        assert result == 1e6 * 240.0

    def test_very_small_volume(self) -> None:
        import data_sync_service.service.market_regime as mr

        current_vol = 0.0
        trade_minutes = 120
        result = mr._estimate_full_day_volume(current_vol, trade_minutes)
        assert result is not None
        assert result == 0.0


class TestVolumeThresholdLogic:
    """Test volume threshold logic for yellow/green/deep_green."""

    def test_yellow_threshold_is_0_8x(self) -> None:
        import data_sync_service.service.market_regime as mr

        avg_vol_5 = 1e9
        estimated_vol = avg_vol_5 * 0.79
        assert estimated_vol < avg_vol_5 * 0.8
        assert mr._estimate_full_day_volume(estimated_vol * 60 / 240.0, 60) == estimated_vol

    def test_green_threshold_is_above_0_8x(self) -> None:
        import data_sync_service.service.market_regime as mr

        avg_vol_5 = 1e9
        estimated_vol = avg_vol_5 * 0.9
        assert estimated_vol > avg_vol_5 * 0.8
        assert mr._estimate_full_day_volume(estimated_vol * 60 / 240.0, 60) == estimated_vol

    def test_deep_green_threshold_is_1_3x(self) -> None:
        import data_sync_service.service.market_regime as mr

        avg_vol_5 = 1e9
        estimated_vol = avg_vol_5 * 1.29
        assert estimated_vol < avg_vol_5 * 1.3
        assert mr._estimate_full_day_volume(estimated_vol * 60 / 240.0, 60) == estimated_vol

    def test_deep_green_triggered_above_1_3x(self) -> None:
        import data_sync_service.service.market_regime as mr

        avg_vol_5 = 1e9
        estimated_vol = avg_vol_5 * 1.5
        assert estimated_vol > avg_vol_5 * 1.3
        assert mr._estimate_full_day_volume(estimated_vol * 60 / 240.0, 60) == estimated_vol


class TestSignalWithEstimatedVolumeOffline:
    """Test signal logic using as_of_date (offline mode with DB volume)."""

    def _make_series_uptrend(
        self, days: int = 80, base: float = 100.0, drift: float = 0.8, base_vol: float = 1e9
    ) -> list[tuple[str, float, float]]:
        """Create (date, close, vol) series with valid dates."""
        out: list[tuple[str, float, float]] = []
        for i in range(days):
            day = i + 1
            month = 1 if day <= 31 else 2
            dom = day if day <= 31 else day - 31
            if month == 2 and dom > 28:
                month = 3
                dom = dom - 28
            d = f"2026-{month:02d}-{dom:02d}"
            close = base + i * drift
            vol = base_vol + i * 1e7
            out.append((d, close, vol))
        return out

    def test_yellow_when_db_vol_below_threshold(self, monkeypatch) -> None:
        """Yellow signal when DB volume < MA5_Vol * 1.0 (offline mode)."""
        import data_sync_service.service.market_regime as mr

        series = self._make_series_uptrend(base_vol=1e9)
        last_date = series[-1][0]

        flat_vol = sum([v for _, _, v in series[-20:]]) / 20.0
        series = [(d, c, flat_vol) for d, c, _ in series]

        monkeypatch.setattr(
            mr,
            "fetch_last_closes_vol_upto",
            lambda ts_code, as_of, days=80: series,
        )
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})

        signals = mr.get_index_signals(as_of_date=last_date)
        assert len(signals) >= 1
        s = signals[0]
        assert s["signal"] == "yellow"
        assert s["realtime"] is False

    def test_green_when_db_vol_above_threshold(self, monkeypatch) -> None:
        """Green signal when DB volume > MA5_Vol * 1.0 (offline mode)."""
        import data_sync_service.service.market_regime as mr

        series = self._make_series_uptrend(base_vol=1e9)
        last_date = series[-1][0]

        monkeypatch.setattr(
            mr,
            "fetch_last_closes_vol_upto",
            lambda ts_code, as_of, days=80: series,
        )
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})
        monkeypatch.setattr(mr, "_get_market_liquidity_and_mainline", _mock_liquidity_not_ok)

        signals = mr.get_index_signals(as_of_date=last_date)
        assert len(signals) >= 1
        s = signals[0]
        assert s["signal"] == "green"
        assert s["realtime"] is False

    def test_deep_green_when_db_vol_above_1_2x(self, monkeypatch) -> None:
        """Deep green when liquidity ok and breadth ok (offline mode)."""
        import data_sync_service.service.market_regime as mr

        series = self._make_series_uptrend(base_vol=1e9)
        series = [
            (d, c, v if i < len(series) - 3 else v * 2.0)
            for i, (d, c, v) in enumerate(series)
        ]
        last_date = series[-1][0]

        monkeypatch.setattr(
            mr,
            "fetch_last_closes_vol_upto",
            lambda ts_code, as_of, days=80: series,
        )
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.75, "total": 100, "above_count": 75})
        monkeypatch.setattr(mr, "_get_market_liquidity_and_mainline", _mock_liquidity_ok)

        signals = mr.get_index_signals(as_of_date=last_date)
        assert len(signals) >= 1
        s = signals[0]
        assert s["signal"] == "deep_green"
        assert s["realtime"] is False


class TestReturnValueFields:
    """Test that get_index_signals returns volRatio and estimatedVol fields."""

    def _make_series_uptrend(
        self, days: int = 80, base: float = 100.0, drift: float = 0.8, base_vol: float = 1e9
    ) -> list[tuple[str, float, float]]:
        """Create (date, close, vol) series with valid dates."""
        out: list[tuple[str, float, float]] = []
        for i in range(days):
            day = i + 1
            month = 1 if day <= 31 else 2
            dom = day if day <= 31 else day - 31
            if month == 2 and dom > 28:
                month = 3
                dom = dom - 28
            d = f"2026-{month:02d}-{dom:02d}"
            close = base + i * drift
            vol = base_vol + i * 1e7
            out.append((d, close, vol))
        return out

    def test_vol_ratio_in_output_offline(self, monkeypatch) -> None:
        import data_sync_service.service.market_regime as mr

        series = self._make_series_uptrend(base_vol=1e9)
        last_date = series[-1][0]

        monkeypatch.setattr(
            mr,
            "fetch_last_closes_vol_upto",
            lambda ts_code, as_of, days=80: series,
        )
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})

        signals = mr.get_index_signals(as_of_date=last_date)
        assert len(signals) >= 1
        s = signals[0]
        assert "volRatio" in s
        assert "estimatedVol" in s
        assert s["estimatedVol"] is None
        assert s["volRatio"] is not None
        assert s["volRatio"] > 0

    def test_non_trading_hours_no_estimated_vol(self, monkeypatch) -> None:
        import data_sync_service.service.market_regime as mr

        series = self._make_series_uptrend(base_vol=1e9)
        last_date = series[-1][0]

        monkeypatch.setattr(
            mr,
            "fetch_last_closes_vol_upto",
            lambda ts_code, as_of, days=80: series,
        )
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.7, "total": 100, "above_count": 70})
        monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: False)

        signals = mr.get_index_signals(as_of_date=last_date)
        assert len(signals) >= 1
        s = signals[0]
        assert s["estimatedVol"] is None
        assert s["volRatio"] is not None