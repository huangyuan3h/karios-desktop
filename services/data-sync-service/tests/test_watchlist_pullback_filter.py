"""filter_pullback_window: 52W pullback gate computed from DB K-lines.

Regression for 2026-08-02+: TV Scanner API `High.Interval52Week` returns empty
values, which zeroed the pullback gate and forced the fallback universe path.
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.watchlist_automation import filter_pullback_window


def _bars(
    highs: list[float],
    closes: list[float],
    start_day: int = 1,
    ts_code: str = "601088.SH",
) -> dict[str, list[tuple[str, str, str, str, str, str]]]:
    """Build OHLCV tuples: (date, open, high, low, close, volume) asc by date."""
    assert len(highs) == len(closes)
    out = {}
    for i, (h, c) in enumerate(zip(highs, closes, strict=False)):
        day = start_day + i
        out.setdefault(ts_code, []).append(
            (f"2026-06-{day:02d}", "10", str(h), "9", str(c), "1000")
        )
    return out


def _two_hundred_bars(
    high: float,
    close: float,
    ts_code: str = "601088.SH",
) -> dict[str, list[tuple[str, str, str, str, str, str]]]:
    """200 bars, all with the same high/close (satisfies PULLBACK_MIN_BARS=60)."""
    return _bars([high] * 200, [close] * 200, start_day=1, ts_code=ts_code)


class TestFilterPullbackWindow:
    def test_in_window(self):
        # high 52w = 10.0, close 9.2 → ratio -0.08 (inside [-0.15, -0.05]).
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_two_hundred_bars(10.0, 9.2),
        ):
            res = filter_pullback_window(["CN:601088"])
        assert res["ok"] is True
        r = res["results"][0]
        assert r["symbol"] == "CN:601088"
        assert r["tsCode"] == "601088.SH"
        assert r["inWindow"] is True
        assert r["missing"] is False
        assert abs(r["pullbackRatio"] - (-0.08)) < 1e-6
        assert abs(r["price"] - 9.2) < 1e-9
        assert abs(r["high52w"] - 10.0) < 1e-9

    def test_out_of_window_high(self):
        # Only -3% pullback → outside window (too close to 52W high).
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_two_hundred_bars(10.0, 9.7),
        ):
            res = filter_pullback_window(["CN:601088"])
        assert res["results"][0]["inWindow"] is False

    def test_out_of_window_deep(self):
        # -30% pullback → outside window (too deep).
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_two_hundred_bars(10.0, 7.0),
        ):
            res = filter_pullback_window(["CN:601088"])
        assert res["results"][0]["inWindow"] is False

    def test_52w_window_uses_max_high_across_bars(self):
        # high52w must be the max over the window, not the first/last bar.
        highs = [10.0] * 50 + [12.0] * 150  # max = 12
        closes = [11.0] * 200  # ratio = (11-12)/12 = -0.0833 → in window
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_bars(highs, closes, start_day=1),
        ):
            res = filter_pullback_window(["CN:601088"])
        r = res["results"][0]
        assert abs(r["high52w"] - 12.0) < 1e-9
        assert abs(r["pullbackRatio"] - (-1 / 12)) < 1e-6
        assert r["inWindow"] is True

    def test_insufficient_bars_missing(self):
        # 10 bars only → window too thin → missing, excluded.
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_bars([10.0] * 10, [9.2] * 10, start_day=1),
        ):
            res = filter_pullback_window(["CN:601088"])
        r = res["results"][0]
        assert r["missing"] is True
        assert r["inWindow"] is False

    def test_no_kline_rows_missing(self):
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value={},
        ):
            res = filter_pullback_window(["CN:601088"])
        assert res["results"][0]["missing"] is True

    def test_hk_symbol_resolution(self):
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value=_two_hundred_bars(100.0, 92.0, ts_code="00700.HK"),
        ):
            res = filter_pullback_window(["HK:00700"])
        assert res["results"][0]["tsCode"] == "00700.HK"
        assert res["results"][0]["inWindow"] is True

    def test_unparsed_symbols_reported(self):
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value={},
        ):
            res = filter_pullback_window(["not-a-symbol", "CN:601088"])
        assert "not-a-symbol" in res["unparsed"]
        assert len(res["results"]) == 1

    def test_empty_input(self):
        with patch(
            "data_sync_service.db.daily.fetch_last_ohlcv_batch",
            return_value={},
        ):
            res = filter_pullback_window([])
        assert res["ok"] is True
        assert res["results"] == []
