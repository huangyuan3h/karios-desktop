"""market_regime.py coverage: time helpers, index signals, breadth, liquidity, regime."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from data_sync_service.service import market_regime as mr


def _closes_vol(n: int = 80, growth: float = 0.01, vol: float = 1e8, base: float = 3000.0):
    return [(f"2026-{(i // 30) + 1:02d}-{(i % 30) + 1:02d}", base * (1 + growth) ** i, vol) for i in range(n)]


def _cn_series(n: int = 80, growth: float = 0.01, vol: float = 1e8, base: float = 3000.0):
    return {c["ts_code"]: _closes_vol(n, growth, vol, base) for c in mr.INDEX_SIGNALS}


class TestTimeHelpers:
    def test_is_trading_time_weekend(self) -> None:
        sunday = datetime(2026, 8, 9, 10, 0, tzinfo=UTC)
        assert mr._is_shanghai_trading_time_at(sunday) is False

    def test_is_trading_time_hours(self) -> None:
        mon = datetime(2026, 8, 10, 9, 30, tzinfo=UTC)
        assert mr._is_shanghai_trading_time_at(mon) is True
        assert mr._is_shanghai_trading_time_at(datetime(2026, 8, 10, 12, 0, tzinfo=UTC)) is False
        assert mr._is_shanghai_trading_time_at(datetime(2026, 8, 10, 13, 0, tzinfo=UTC)) is True
        assert mr._is_shanghai_trading_time_at(datetime(2026, 8, 10, 15, 1, tzinfo=UTC)) is False
        assert mr._is_shanghai_trading_time_at(datetime(2026, 8, 10, 11, 31, tzinfo=UTC)) is False

    def test_is_sync_window(self) -> None:
        mon = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
        assert mr._is_shanghai_sync_window_at(mon) is True
        assert mr._is_shanghai_sync_window_at(datetime(2026, 8, 10, 18, 0, tzinfo=UTC)) is True
        assert mr._is_shanghai_sync_window_at(datetime(2026, 8, 10, 21, 0, tzinfo=UTC)) is False
        assert mr._is_shanghai_sync_window_at(datetime(2026, 8, 8, 10, 0, tzinfo=UTC)) is False

    def test_get_trade_minutes(self) -> None:
        mon = datetime(2026, 8, 10, 9, 0, tzinfo=UTC)
        assert mr._get_trade_minutes(mon) == 0
        assert mr._get_trade_minutes(datetime(2026, 8, 10, 10, 0, tzinfo=UTC)) == 30
        assert mr._get_trade_minutes(datetime(2026, 8, 10, 12, 0, tzinfo=UTC)) == 120
        assert mr._get_trade_minutes(datetime(2026, 8, 10, 14, 0, tzinfo=UTC)) == 180
        assert mr._get_trade_minutes(datetime(2026, 8, 10, 16, 0, tzinfo=UTC)) == 240

    def test_estimate_full_day_volume(self) -> None:
        assert mr._estimate_full_day_volume(1e8, 0) is None
        assert mr._estimate_full_day_volume(1e8, 120) == pytest.approx(2e8)

    def test_trade_date_from_trade_time(self) -> None:
        assert mr._trade_date_from_trade_time(None) is None
        assert mr._trade_date_from_trade_time("  ") is None
        assert mr._trade_date_from_trade_time("2026-08-07 15:00:00") == "2026-08-07"
        assert mr._trade_date_from_trade_time("20260807") == "2026-08-07"
        assert mr._trade_date_from_trade_time("garbage") is None

    def test_realtime_pct_or_price(self) -> None:
        assert mr._realtime_pct_or_price({"price": "10.0", "pre_close": "9.5", "pct_chg": "5.26"}) == (5.26, 10.0)
        assert mr._realtime_pct_or_price({"price": "10.0", "pre_close": "9.5"}) == (pytest.approx(5.263, abs=0.01), 10.0)
        assert mr._realtime_pct_or_price({"price": None, "pre_close": "9.5"}) == (None, None)
        assert mr._realtime_pct_or_price({"price": "abc"}) == (None, None)

    def test_merge_on_demand_into_series(self) -> None:
        series = [("2026-08-06", 100.0), ("2026-08-07", 101.0)]
        out = mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-07", "close": 102.0})
        assert out[-1] == ("2026-08-07", 102.0)
        out2 = mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-08", "close": 103.0})
        assert len(out2) == 3 and out2[-1] == ("2026-08-08", 103.0)
        out3 = mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-05", "close": 99.0})
        assert out3 == series
        assert mr._merge_on_demand_into_series(series, {}) == series
        assert mr._merge_on_demand_into_series(series, {"asOfDate": "2026-08-08", "close": "bad"}) == series

    def test_hsi_series_stale(self) -> None:
        assert mr._hsi_series_stale([]) is True
        assert mr._hsi_series_stale([("2026-08-07", 100.0)]) is False

    def test_hsi_source_label(self) -> None:
        assert mr._hsi_source_label(used_realtime=True, on_demand_src=None) == "tushare.realtime_quote"
        assert mr._hsi_source_label(used_realtime=False, on_demand_src="yf") == "yf"
        assert mr._hsi_source_label(used_realtime=False, on_demand_src=None) == "db.macro_daily"

    def test_safe_float(self) -> None:
        assert mr._safe_float("1.5") == 1.5
        assert mr._safe_float(float("nan")) is None
        assert mr._safe_float("x") is None

    def test_quote_error_message(self) -> None:
        assert mr._quote_error_message(None) == "invalid_quote_response"
        assert mr._quote_error_message({"ok": True}) is None
        assert mr._quote_error_message({"error": "  "}) is None
        assert mr._quote_error_message({"error": "boom"}) == "boom"

    def test_ema(self) -> None:
        assert mr._ema([], 5) == []
        assert mr._ema([1.0, 2.0, 3.0], 5) == []
        out = mr._ema(list(range(1, 30)), 5)
        assert len(out) == 25

    def test_macd_histogram(self) -> None:
        assert mr._macd_histogram([1.0] * 20) == []
        hist = mr._macd_histogram(list(range(1, 100)))
        assert len(hist) > 0

    def test_signal_rank(self) -> None:
        assert mr._signal_rank("deep_green") == 3
        assert mr._signal_rank("yellow") == 2
        assert mr._signal_rank("red") == 1
        assert mr._signal_rank("unknown") == 0


class TestRegimeFromSignals:
    def test_strong(self) -> None:
        signals = [{"name": n, "signal": "green"} for n in ("上证指数", "创业板指")]
        assert mr._regime_from_signals(signals) == ("Strong", None)

    def test_diverging(self) -> None:
        signals = [{"name": "上证指数", "signal": "green"}, {"name": "创业板指", "signal": "red"}]
        assert mr._regime_from_signals(signals) == ("Diverging", "mixed")

    def test_weak(self) -> None:
        signals = [{"name": "上证指数", "signal": "red"}, {"name": "创业板指", "signal": "red"}]
        assert mr._regime_from_signals(signals) == ("Weak", None)

    def test_weak_fallback_names(self) -> None:
        signals = [{"tsCode": "000001.SH", "signal": "red"}, {"tsCode": "399006.SZ", "signal": "yellow"}]
        assert mr._regime_from_signals(signals) == ("Weak", None)

    def test_single_signal_weak(self) -> None:
        assert mr._regime_from_signals([{"name": "上证指数", "signal": "green"}]) == ("Weak", None)
        assert mr._regime_from_signals([]) == ("Weak", None)


class TestIndexSignals:
    def _patch_basics(self, monkeypatch, series=None):
        monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: False)
        monkeypatch.setattr(mr, "fetch_last_closes_vol_batch", lambda codes, days, as_of_date=None: _cn_series() if series is None else series)
        monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda series_id, days: [("2026-08-06", 20000.0), ("2026-08-07", 20100.0)])
        monkeypatch.setattr(mr, "fetch_hk_index_on_demand", lambda series_id: ({"close": None, "asOfDate": None}, None))
        monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **kw: {"ratio": 0.7, "total": 100, "above_count": 70})
        monkeypatch.setattr(mr, "_get_market_liquidity_and_mainline", lambda **kw: {
            "total_turnover_cny": 1.6e12, "max_industry_inflow": 6e9,
            "turnover_above_1_5T": True, "mainline_inflow_above_5B": True,
        })

    def test_compute_short_data(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch, series={c["ts_code"]: _closes_vol(10) for c in mr.INDEX_SIGNALS})
        out = mr._compute_index_signals()
        assert out[0]["signal"] == "unknown"
        assert out[0]["rules"] == ["insufficient data for MA20"]
        assert out[0]["source"] == "db.index_daily"

    def test_compute_full_green(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        out = mr._compute_index_signals(as_of_date="2026-08-07")
        cn = [x for x in out if x["tsCode"] in ("000001.SH", "399006.SZ")]
        assert all(x["signal"] in ("green", "deep_green") for x in cn)
        assert cn[0]["ma20"] is not None

    def test_compute_red(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch, series={c["ts_code"]: _closes_vol(80, growth=-0.01) for c in mr.INDEX_SIGNALS})
        out = mr._compute_index_signals(as_of_date="2026-08-07")
        assert out[0]["signal"] == "red"

    def test_compute_no_breadth(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        out = mr._compute_index_signals(as_of_date="2026-08-07", include_breadth=False)
        assert out[0]["totalTurnover"] == 0.0
        assert out[0]["maxIndustryInflow"] == 0.0

    def test_get_index_signals_cache(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        mr.clear_index_signals_cache()
        calls = {"n": 0}
        real = mr._compute_index_signals

        def spy(**kw):
            calls["n"] += 1
            return real(**kw)

        monkeypatch.setattr(mr, "_compute_index_signals", spy)
        mr.get_index_signals(as_of_date="2026-08-07", include_breadth=False)
        mr.get_index_signals(as_of_date="2026-08-07", include_breadth=False)
        assert calls["n"] == 1

    def test_realtime_merge_updates_series(self, monkeypatch) -> None:
        series = _cn_series()
        self._patch_basics(monkeypatch, series=series)
        monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: True)
        monkeypatch.setattr(mr, "_fetch_realtime_quote_map", lambda codes: (
            {c: {"ts_code": c, "price": "3200.0", "pct_chg": "2.0", "trade_time": "2026-04-30 15:00:00", "volume": "1e8"} for c in codes},
            {},
        ))
        out = mr._compute_index_signals()
        assert out[0]["realtime"] is True
        assert out[0]["source"] == "tushare.realtime_quote"
        assert out[0]["close"] == 3200.0

    def test_realtime_merge_quote_errors(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: True)
        monkeypatch.setattr(mr, "_fetch_realtime_quote_map", lambda codes: ({}, {codes[0]: "rate_limited"}))
        out = mr._compute_index_signals(as_of_date=None)
        assert out[0]["quoteError"] == "rate_limited"

    def test_hk_no_data(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda series_id, days: [])
        monkeypatch.setattr(mr, "fetch_hk_index_on_demand", lambda series_id: ({"close": None, "asOfDate": None}, None))
        out = mr._compute_index_signals(as_of_date="2026-08-07")
        hk = [x for x in out if x["tsCode"] in ("HSI", "HSTECH")]
        assert hk[0]["signal"] == "unknown"
        assert hk[0]["rules"] == ["no data in macro_daily"]

    def test_hk_on_demand_merge(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda series_id, days: [])
        monkeypatch.setattr(mr, "fetch_hk_index_on_demand", lambda series_id: ({"close": 21000.0, "asOfDate": "2026-08-07"}, "yf"))
        out = mr._compute_index_signals(as_of_date="2026-08-07")
        hk = [x for x in out if x["tsCode"] in ("HSI", "HSTECH")]
        assert hk[0]["close"] == 21000.0
        assert hk[0]["source"] == "yf"

    def test_get_market_regime_cached(self, monkeypatch) -> None:
        self._patch_basics(monkeypatch)
        mr.clear_market_regime_cache()
        calls = {"n": 0}
        real = mr.get_index_signals

        def spy(**kw):
            calls["n"] += 1
            return real(**kw)

        monkeypatch.setattr(mr, "get_index_signals", spy)
        r1 = mr.get_market_regime(as_of_date="2026-08-07", include_breadth=False)
        r2 = mr.get_market_regime(as_of_date="2026-08-07", include_breadth=False)
        assert r1["regime"] in ("Strong", "Weak", "Diverging")
        assert r2 is r1
        assert calls["n"] == 1

    def test_clear_market_breadth_cache(self) -> None:
        mr._breadth_cache_live["k"] = 1
        mr._breadth_cache_hist["k"] = 2
        mr._liquidity_cache_live["k"] = 3
        mr._liquidity_cache_hist["k"] = 4
        mr.clear_market_breadth_cache()
        assert all(len(c) == 0 for c in (mr._breadth_cache_live, mr._breadth_cache_hist, mr._liquidity_cache_live, mr._liquidity_cache_hist))


class TestBreadthAndLiquidity:
    def test_compute_breadth_above_ma20(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: False)
        monkeypatch.setattr(mr, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: ["600000.SH", "000001.SZ"])
        rising = [("2026-0%d-%02d" % (i // 30 + 1, i % 30 + 1), 5.0 if i < 20 else 10.0, 0.0, 0.0, 5.0 if i < 20 else 10.0, 1e6) for i in range(30)]
        falling = [("2026-0%d-%02d" % (i // 30 + 1, i % 30 + 1), 10.0 if i < 20 else 5.0, 0.0, 0.0, 10.0 if i < 20 else 5.0, 1e6) for i in range(30)]

        def batch(codes, days):
            return {"600000.SH": rising, "000001.SZ": falling}

        monkeypatch.setattr(mr, "fetch_last_ohlcv_batch", batch)
        out = mr._compute_breadth_above_ma20_ratio(as_of_date="2026-08-07")
        assert out["ratio"] == 0.5 and out["total"] == 2 and out["above_count"] == 1

    def test_compute_breadth_no_codes(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(mr, "fetch_stock_ts_codes", lambda: [])
        assert mr._compute_breadth_above_ma20_ratio() == {"ratio": 0.0, "total": 0, "above_count": 0}

    def test_get_breadth_cached(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "_compute_breadth_above_ma20_ratio", lambda **kw: {"ratio": 0.5, "total": 2, "above_count": 1})
        mr.clear_market_breadth_cache()
        assert mr._get_breadth_above_ma20_ratio()["ratio"] == 0.5
        mr._breadth_cache_live.clear()
        assert mr._get_breadth_above_ma20_ratio(as_of_date="2026-08-07")["ratio"] == 0.5

    def test_compute_liquidity_eod(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: {"total_turnover_cny": 1.6e12})
        monkeypatch.setattr(mr, "get_rows_by_date", lambda d: [{"net_inflow": 6e9}, {"net_inflow": 1e9}])
        out = mr._compute_market_liquidity_and_mainline(as_of_date="2026-01-05", breadth_ratio=0.5)
        assert out["turnover_above_1_5T"] is True
        assert out["mainline_inflow_above_5B"] is True
        assert out["total_turnover_cny"] == 1.6e12

    def test_compute_liquidity_intraday_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: {})
        monkeypatch.setattr(mr, "fetch_cn_market_breadth_intraday", lambda dt: {"total_turnover_cny": 2e12})
        monkeypatch.setattr(mr, "get_rows_by_date", lambda d: [])
        out = mr._compute_market_liquidity_and_mainline(as_of_date=None, breadth_ratio=0.5)
        assert out["total_turnover_cny"] == 2e12
        assert out["mainline_inflow_above_5B"] is False

    def test_compute_liquidity_errors(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "fetch_cn_market_breadth_eod", lambda dt: (_ for _ in ()).throw(RuntimeError("down")))
        monkeypatch.setattr(mr, "fetch_cn_market_breadth_intraday", lambda dt: (_ for _ in ()).throw(RuntimeError("down")))
        monkeypatch.setattr(mr, "get_rows_by_date", lambda d: (_ for _ in ()).throw(RuntimeError("down")))
        out = mr._compute_market_liquidity_and_mainline(as_of_date="2026-01-05", breadth_ratio=0.5)
        assert out == {
            "total_turnover_cny": 0.0, "max_industry_inflow": 0.0,
            "turnover_above_1_5T": False, "mainline_inflow_above_5B": False,
        }

    def test_get_liquidity_cached(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "_compute_market_liquidity_and_mainline", lambda **kw: {"x": 1})
        mr.clear_market_breadth_cache()
        assert mr._get_market_liquidity_and_mainline(breadth_ratio=0.5) == {"x": 1}

    def test_fetch_realtime_quote_map_missing_fallback(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "fetch_realtime_quotes", lambda codes: (
            {"ok": True, "items": [{"ts_code": "000001.SH", "price": "10.0"}]}
            if len(codes) == 2 else {"ok": True, "items": []}
        ))
        quotes, errors = mr._fetch_realtime_quote_map(["000001.SH", "399006.SZ"])
        assert "000001.SH" in quotes
        assert "399006.SZ" in errors

    def test_fetch_realtime_quote_map_batch_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(mr, "fetch_realtime_quotes", lambda codes: {"ok": False, "error": "quota"})
        quotes, errors = mr._fetch_realtime_quote_map(["000001.SH"])
        assert quotes == {}
        assert errors["000001.SH"] == "quota"

    def test_fetch_realtime_quote_map_empty(self, monkeypatch) -> None:
        assert mr._fetch_realtime_quote_map([]) == ({}, {})
        assert mr._fetch_realtime_quote_map([" "]) == ({}, {})

    def test_apply_realtime_quotes(self) -> None:
        rt_price, rt_time, rt_pct, rt_vol = {}, {}, {}, {}
        quotes = {"600000.SH": {"price": "11.0", "pct_chg": "1.5", "trade_time": "2026-08-07 14:00:00", "volume": "5e6"}}
        mr._apply_realtime_quotes(quotes, rt_price, rt_time, rt_pct, rt_vol)
        assert rt_price["600000.SH"] == 11.0
        assert rt_pct["600000.SH"] == 1.5
        assert rt_vol["600000.SH"] == 5e6
        rt2, t2, p2, v2 = {}, {}, {}, {}
        mr._apply_realtime_quotes({"x": {"price": None}}, rt2, t2, p2, v2)
        assert rt2 == {}
