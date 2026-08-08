"""trendok.py coverage: pure helpers + _trendok_one branches + compute_trendok_for_symbols."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from data_sync_service.service import trendok as tk

bar_vol = 1_000_000.0


def _dates(n: int, start: str = "2026-01-01") -> list[str]:
    d = date.fromisoformat(start)
    return [(d + timedelta(days=i)).isoformat() for i in range(n)]


def _rising_bars(n: int = 120, start: float = 100.0, growth: float = 0.01, vol: float = bar_vol, vol_tail: float | None = None) -> list[tuple[str, str, str, str, str, str]]:
    dates = _dates(n)
    bars = []
    for i, dt in enumerate(dates):
        close = start * (1 + growth) ** i
        open_ = close * 0.995
        high = close * 1.005
        low = close * 0.99
        v = vol * (vol_tail if vol_tail and i == n - 1 else 1.0)
        bars.append((dt, f"{open_:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{v:.0f}"))
    return bars


def _surge_end(bars: list) -> list:
    dt = bars[-1][0]
    prev_close = float(bars[-2][4])
    new_close = prev_close * 1.05
    open_ = prev_close
    bars[-1] = (dt, f"{open_:.3f}", f"{new_close * 1.005:.3f}", f"{prev_close * 0.99:.3f}", f"{new_close:.3f}", bars[-1][5])
    return bars


def _flat_then_crash(n: int = 120, boom: int = 70, drop_pct: float = 0.03, vol: float = bar_vol) -> list[tuple[str, str, str, str, str, str]]:
    dates = _dates(n)
    bars = []
    peak = 100.0 * (1.01 ** boom)
    for i, dt in enumerate(dates):
        if i < boom:
            close = 100.0 * (1.01 ** i)
        else:
            close = peak * (1.0 - drop_pct * (i - boom))
        open_ = close * 1.002
        high = close * 1.005
        low = close * 0.995
        v = vol * (0.5 if i >= boom else 1.0)
        bars.append((dt, f"{open_:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{v:.0f}"))
    return bars


def _trendok_one(**kw):
    defaults = dict(
        symbol="CN:600519",
        name="贵州茅台",
        industry="白酒",
        bars=_rising_bars(),
        flow_ctx=None,
        market_regime="Strong",
        inst_summary=None,
        buy_seats_by_key=None,
        resolve_stoploss=None,
        index_20d_ret=1.0,
        index_ema20_down=False,
        rt_vwap=None,
        is_alpha_s=False,
        is_held=False,
        cost_price=None,
    )
    defaults.update(kw)
    return tk._trendok_one(**defaults)


class TestPureHelpers:
    def test_ema_empty(self) -> None:
        assert tk._ema([], 5) == []
        assert tk._ema([1.0, 2.0], 0) == []

    def test_ema_values(self) -> None:
        out = tk._ema([1.0, 2.0, 3.0, 4.0, 5.0], 5)
        assert len(out) == 5
        assert out[-1] == pytest.approx(3.395, abs=0.01)

    def test_rsi(self) -> None:
        vals = list(range(20, 60))
        out = tk._rsi(vals, 14)
        assert out[0] == 0.0
        assert out[-1] == pytest.approx(100.0)

    def test_rsi_flat(self) -> None:
        out = tk._rsi([5.0] * 20, 14)
        assert out[-1] == pytest.approx(50.0)

    def test_rsi_bad_period(self) -> None:
        assert tk._rsi([1.0], 14) == []

    def test_macd_empty(self) -> None:
        assert tk._macd([]) == ([], [], [])

    def test_macd_shape(self) -> None:
        m, s, h = tk._macd(list(range(1, 100)), 12, 26, 9)
        assert len(m) == len(s) == len(h)

    def test_atr14_insufficient(self) -> None:
        assert tk._atr14([1.0] * 5, [1.0] * 5, [1.0] * 5, 14) is None
        assert tk._atr14([1.0] * 30, [1.0] * 30, [1.0] * 30, 0) is None

    def test_atr14_value(self) -> None:
        highs = [10.0, 11.0, 12.0, 11.0, 12.0, 13.0, 14.0, 13.0, 14.0, 15.0, 16.0, 15.0, 16.0, 17.0, 18.0, 17.0, 18.0, 19.0]
        lows = [9.0] * 18
        closes = [9.5, 10.5, 11.5, 10.5, 11.5, 12.5, 13.5, 12.5, 13.5, 14.5, 15.5, 14.5, 15.5, 16.5, 17.5, 16.5, 17.5, 18.5]
        v = tk._atr14(highs, lows, closes, 14)
        assert v is not None and v > 0

    def test_parse_float_safe(self) -> None:
        assert tk._parse_float_safe(None) is None
        assert tk._parse_float_safe("1.5") == 1.5
        assert tk._parse_float_safe("abc") is None
        assert tk._parse_float_safe(float("inf")) is None

    def test_clip01(self) -> None:
        assert tk._clip01(-1.0) == 0.0
        assert tk._clip01(2.0) == 1.0
        assert tk._clip01(0.5) == 0.5

    def test_volume_vs_avg10(self) -> None:
        assert tk._volume_vs_avg10([1.0] * 10) is None
        vols = [1.0] * 10 + [3.0]
        assert tk._volume_vs_avg10(vols) == pytest.approx(3.0)

    def test_is_bullish_day(self) -> None:
        assert tk._is_bullish_day([10.0, 11.0], [9.0, 10.0]) is True
        assert tk._is_bullish_day([10.0, 11.0], [9.0, 11.5]) is False
        assert tk._is_bullish_day([10.0], [9.0]) is False

    def test_score_sub_ema(self) -> None:
        s, pts = tk._score_sub_ema(11.0, 10.0, 9.0, 9.5)
        assert s == pytest.approx(1.0)
        s, pts = tk._score_sub_ema(None, 10.0, 9.0, 9.5)
        assert s == 0.0
        s, pts = tk._score_sub_ema(10.0, 10.5, 9.0, 10.0)
        assert s == pytest.approx(0.6)

    def test_score_sub_ema_slope_bonus(self) -> None:
        s, pts = tk._score_sub_ema(11.0, 10.1, 9.0, 10.0)
        assert s == pytest.approx(1.0)

    def test_score_sub_macd(self) -> None:
        s, pts = tk._score_sub_macd(1.0, [0.5, 0.8, 1.2])
        assert s == 1.0
        s, pts = tk._score_sub_macd(None, [])
        assert s == 0.0
        s, pts = tk._score_sub_macd(-1.0, [0.5, 0.8])
        assert s == 0.0

    def test_score_sub_breakout(self) -> None:
        s, pts = tk._score_sub_breakout(10.0, 10.0)
        assert s == pytest.approx(1.0)
        s, pts = tk._score_sub_breakout(None, 10.0)
        assert s == 0.0
        s, pts = tk._score_sub_breakout(9.0, 10.0)
        assert s == pytest.approx(0.5)

    def test_score_sub_rsi(self) -> None:
        s, pts = tk._score_sub_rsi(65.0)
        assert s == pytest.approx(1.0)
        s, pts = tk._score_sub_rsi(85.0)
        assert s == pytest.approx(tk._clip01(1.0 - 20.0 / 15.0) * tk._clip01(1.0 - 5.0 / 10.0))
        s, pts = tk._score_sub_rsi(None)
        assert s == 0.0

    def test_score_sub_volume(self) -> None:
        assert tk._score_sub_volume(0.5)[0] == pytest.approx(0.5)
        assert tk._score_sub_volume(1.1)[0] == pytest.approx(0.75)
        assert tk._score_sub_volume(1.5)[0] == 1.0
        assert tk._score_sub_volume(2.5)[0] == pytest.approx(0.5)
        assert tk._score_sub_volume(5.0)[0] == 0.0
        assert tk._score_sub_volume(None)[0] == 0.0

    def test_bonus_ema20_slope_5d(self) -> None:
        assert tk._score_bonus_ema20_slope_5d([1.0] * 6) == 0.0
        assert tk._score_bonus_ema20_slope_5d(list(range(10, 16))) == 5.0
        assert tk._score_bonus_ema20_slope_5d([1.0, 2.0, 3.0, 4.0, 5.0]) == 0.0

    def test_anti_spike_penalties(self) -> None:
        p, parts = tk._score_anti_spike_penalties(
            close=10.0, ema20=9.0, intraday_chg_pct=7.0, atr14=0.6, vol_today=1e6, avg_vol30=1e5,
        )
        assert p > 40.0 and "penalty_intraday_spike" in parts
        p2, parts2 = tk._score_anti_spike_penalties(
            close=8.0, ema20=10.0, intraday_chg_pct=None, atr14=None, vol_today=1e5, avg_vol30=1e5,
        )
        assert p2 == 30.0 and "penalty_below_ema20" in parts2
        p3, parts3 = tk._score_anti_spike_penalties(
            close=10.0, ema20=9.0, intraday_chg_pct=None, atr14=None, vol_today=1e5, avg_vol30=1e5,
        )
        assert p3 == 0.0 and parts3 == {}

    def test_shanghai_today_iso(self) -> None:
        assert len(tk._shanghai_today_iso()) == 10

    def test_normalize_yyyy_mm_dd(self) -> None:
        assert tk._normalize_yyyy_mm_dd(None) is None
        assert tk._normalize_yyyy_mm_dd("20260808") == "2026-08-08"
        assert tk._normalize_yyyy_mm_dd("2026-08-08") == "2026-08-08"
        d = date(2026, 8, 8)
        assert tk._normalize_yyyy_mm_dd(d) == "2026-08-08"

    def test_quote_trade_date(self) -> None:
        assert tk._quote_trade_date({}) is None
        assert tk._quote_trade_date({"trade_time": "2026-08-07 15:00:00"}) == "2026-08-07"
        assert tk._quote_trade_date({"trade_time": "20260807 15:00:00"}) == "2026-08-07"
        assert tk._quote_trade_date({"trade_time": "bad"}) is None

    def test_pick_str(self) -> None:
        assert tk._pick_str(None, "fb") == "fb"
        assert tk._pick_str("  ", "fb") == "fb"
        assert tk._pick_str("v", "fb") == "v"

    def test_merge_realtime_bar_append(self) -> None:
        bars = _rising_bars(5)
        last_date = bars[-1][0]
        quote = {"price": "150.0", "trade_time": "2026-08-06 15:00:00", "open": "149.0", "high": "151.0", "low": "148.5", "volume": "123456"}
        merged = tk._merge_realtime_bar(bars, quote)
        assert len(merged) == 6
        assert merged[-1][0] == "2026-08-06"

    def test_merge_realtime_bar_replace(self) -> None:
        bars = _rising_bars(5)
        quote = {"price": "150.0", "trade_time": f"{bars[-1][0]} 15:00:00", "volume": "123456"}
        merged = tk._merge_realtime_bar(bars, quote)
        assert len(merged) == 5
        assert merged[-1][4] == "150.0"

    def test_merge_realtime_bar_skips(self) -> None:
        bars = _rising_bars(5)
        merged = tk._merge_realtime_bar(bars, {"price": None})
        assert merged == bars
        merged2 = tk._merge_realtime_bar([], {"price": "1.0"})
        assert merged2 == []

    def test_symbol_to_ts_code(self) -> None:
        assert tk._symbol_to_ts_code("CN:600519") == ("CN", "600519", "600519.SH")
        assert tk._symbol_to_ts_code("CN:000001") == ("CN", "000001", "000001.SZ")
        assert tk._symbol_to_ts_code("HK:700") == ("HK", "00700", "00700.HK")
        assert tk._symbol_to_ts_code("ETF:510300") == ("ETF", "510300", "510300.SH")
        assert tk._symbol_to_ts_code("ETF:159915") == ("ETF", "159915", "159915.SZ")
        assert tk._symbol_to_ts_code("CN:12345") is None
        assert tk._symbol_to_ts_code("") is None
        assert tk._symbol_to_ts_code("US:APPL") is None

    def test_macro_override_lock_active(self) -> None:
        assert tk.macro_override_lock_active("capitulation_v_bottom", 5000) is False
        assert tk.macro_override_lock_active("extreme_caution", 0) is True
        assert tk.macro_override_lock_active(None, 4000) is True
        assert tk.macro_override_lock_active(None, 100) is False
        assert tk.macro_override_lock_active(None, None) is False

    def test_apply_macro_override_lock(self) -> None:
        results = [{"symbol": "CN:600000", "buyAction": "buy", "buyChecks": {}, "riskAlerts": []}]
        out = tk.apply_macro_override_lock(results, "extreme_caution", 5000)
        assert out[0]["buyAction"] == "avoid"
        assert out[0]["buyChecks"]["blocked_macro_lock"] is True
        assert out[0]["macroLock"]["active"] is True
        assert out[0]["riskAlerts"][0]["code"] == "macro_override_lock"

    def test_apply_macro_override_lock_noop(self) -> None:
        results = [{"symbol": "CN:600000", "buyAction": "buy"}]
        assert tk.apply_macro_override_lock(results, "capitulation_v_bottom", 5000) == results

    def test_apply_alpha_s_trend_recovering(self) -> None:
        closes = [10.0 + i * 0.1 for i in range(30)]
        opens = [c - 0.05 for c in closes]
        vols = [1.0] * 19 + [3.0]
        res = {"score": 30.0, "trendOk": False, "scoreParts": {}, "checks": {}}
        tk.apply_alpha_s_trend_recovering(res, closes=closes, opens=opens, vols=vols, is_alpha_s=True)
        assert res["trendOk"] is True
        assert res["score"] == 60.0
        assert res["trendStatus"] == "recovering"
        assert res["scoreParts"]["alpha_s_trend_recovering"] == 60.0

    def test_apply_alpha_s_trend_recovering_not_alpha_s(self) -> None:
        closes = [10.0 + i * 0.1 for i in range(30)]
        opens = [c - 0.05 for c in closes]
        vols = [1.0] * 19 + [3.0]
        res = {"score": 30.0, "trendOk": True, "scoreParts": {}, "checks": {}}
        tk.apply_alpha_s_trend_recovering(res, closes=closes, opens=opens, vols=vols, is_alpha_s=False)
        assert res["trendStatus"] == "ok"
        assert res["checks"]["alphaSTrendRecovering"] is False

    def test_apply_alpha_s_trend_recovering_bad_score(self) -> None:
        closes = [10.0 + i * 0.1 for i in range(30)]
        opens = [c - 0.05 for c in closes]
        vols = [1.0] * 19 + [3.0]
        res = {"score": "abc", "scoreParts": {}, "checks": {}}
        tk.apply_alpha_s_trend_recovering(res, closes=closes, opens=opens, vols=vols, is_alpha_s=True)
        assert res["score"] == 60.0

    def test_volume_vs_avg10_none_short(self) -> None:
        closes = [10.0 + i * 0.1 for i in range(5)]
        opens = [c - 0.05 for c in closes]
        vols = [1.0] * 5
        res = {"score": 30.0, "scoreParts": {}, "checks": {}}
        tk.apply_alpha_s_trend_recovering(res, closes=closes, opens=opens, vols=vols, is_alpha_s=True)
        assert res["trendStatus"] is None

    def test_score_for_momentum_surge_gate(self) -> None:
        res = {"score": 90.0, "scoreParts": {"penalty_intraday_spike": -20.0}}
        assert tk._score_for_momentum_surge_gate(res) == pytest.approx(110.0)
        res2 = {"score": 90.0, "scoreParts": {}}
        assert tk._score_for_momentum_surge_gate(res2) == 90.0
        res3 = {"score": None}
        assert tk._score_for_momentum_surge_gate(res3) is None
        res4 = {"score": "x"}
        assert tk._score_for_momentum_surge_gate(res4) is None
        res5 = {"score": 90.0, "scoreParts": {"penalty_intraday_spike": "bad"}}
        assert tk._score_for_momentum_surge_gate(res5) == 90.0

    def test_is_momentum_surge_eligible(self) -> None:
        res = {"buyMode": "B_momentum", "trendOk": True, "score": 90.0, "scoreParts": {}}
        assert tk._is_momentum_surge_eligible(res, intraday_pct=7.0) is True
        assert tk._is_momentum_surge_eligible(res, intraday_pct=10.0) is False
        res2 = {"buyMode": "A_pullback", "trendOk": True, "score": 90.0, "scoreParts": {}}
        assert tk._is_momentum_surge_eligible(res2, intraday_pct=7.0) is False
        res3 = {"buyMode": "B_momentum", "trendOk": False, "score": 90.0, "scoreParts": {}}
        assert tk._is_momentum_surge_eligible(res3, intraday_pct=7.0) is False
        res4 = {"buyMode": "B_momentum", "trendOk": True, "score": 80.0, "scoreParts": {}}
        assert tk._is_momentum_surge_eligible(res4, intraday_pct=7.0) is False


class TestTrendokOne:
    def test_unsupported_market(self) -> None:
        res = _trendok_one(symbol="US:APPL", bars=_rising_bars())
        assert "unsupported_market" in res["missingData"]

    def test_no_bars(self) -> None:
        res = _trendok_one(bars=[])
        assert "no_bars" in res["missingData"]
        assert res["trendOk"] is None

    def test_bars_lt_60(self) -> None:
        res = _trendok_one(bars=_rising_bars(30))
        assert "bars_lt_60" in res["missingData"]

    def test_strong_uptrend(self) -> None:
        res = _trendok_one()
        assert res["asOfDate"] == _dates(120)[-1]
        assert res["score"] is not None and res["score"] > 0
        assert res["values"]["ema5"] > res["values"]["ema20"]
        assert res["values"]["industry"] == "白酒"
        assert res["buyRefPrice"] == res["values"]["close"]
        assert res["stopLossPrice"] is not None
        assert res["checks"]["emaOrder"] is True

    def test_buy_b_momentum_signal(self) -> None:
        bars = _rising_bars(60, start=100.0, growth=0.02, vol=1_000_000.0, vol_tail=3.0)
        res = _trendok_one(bars=bars, market_regime="Strong")
        assert res["buyMode"] in ("B_momentum",)
        assert res["buyChecks"]["mode_b_allowed"] is True

    def test_buy_mode_b_blocked_not_strong(self) -> None:
        res = _trendok_one(market_regime="Weak")
        assert res["buyChecks"]["mode_b_blocked"] is True
        assert res["buyChecks"]["in_trend"] is False

    def test_crash_exit_now(self) -> None:
        bars = _flat_then_crash()
        res = _trendok_one(bars=bars, market_regime="Weak")
        assert res["stopLossParts"]["exit_now"] is True
        assert res["buyAction"] == "avoid"
        assert "exit_display" in res["stopLossParts"]

    def test_stoploss_missing_inputs(self) -> None:
        res = _trendok_one(bars=_rising_bars(10))
        assert res["stopLossPrice"] is None
        assert "stoploss_missing_inputs" in res["missingData"]

    def test_etf_fallback_no_ema20(self) -> None:
        res = _trendok_one(symbol="ETF:510300", bars=_rising_bars(10))
        assert res["stopLossParts"]["etf_fallback"] is True
        assert res["stopLossPrice"] is not None

    def test_momentum_exhaustion_exit(self) -> None:
        n = 120
        boom = 80
        dates = _dates(n)
        bars = []
        peak = 100.0 * (1.01 ** boom)
        for i, dt in enumerate(dates):
            if i < boom:
                close = 100.0 * (1.01 ** i)
            else:
                close = peak * (1.0 - 0.06 * (i - boom))
            open_ = close * 1.01
            high = close * 1.02
            low = close * 0.98
            v = bar_vol * (0.3 if i >= boom else 1.0)
            bars.append((dt, f"{open_:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{v:.0f}"))
        res = _trendok_one(bars=bars)
        assert res["stopLossParts"]["exit_now"] is True
        assert res["stopLossParts"]["exit_reasons"]

    def test_momentum_warning_reduce_half(self) -> None:
        n = 120
        boom = 90
        dates = _dates(n)
        bars = []
        peak = 100.0 * (1.01 ** boom)
        for i, dt in enumerate(dates):
            if i < boom:
                close = 100.0 * (1.01 ** i)
            else:
                close = peak * (1.0 - 0.004 * (i - boom))
            open_ = close * 1.002
            high = close * 1.004
            low = close * 0.996
            v = bar_vol * (0.5 if i >= boom else 1.0)
            bars.append((dt, f"{open_:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{v:.0f}"))
        res = _trendok_one(bars=bars)
        if res["stopLossParts"].get("warn_hist_shrink_cnt_3", 0) >= 2:
            assert res["stopLossParts"]["warn_reduce_half"] is True

    def test_etf_trim_warning_not_exit(self) -> None:
        bars = _flat_then_crash()
        res = _trendok_one(symbol="ETF:510300", bars=bars)
        assert res["stopLossParts"]["exit_now"] is False
        assert res["stopLossParts"]["warn_reduce_half"] is True

    def test_held_with_cost_price(self) -> None:
        res = _trendok_one(is_held=True, cost_price=120.0)
        assert "hard_stop_entry" in res["stopLossParts"]

    def test_volatility_bins(self) -> None:
        steady = _rising_bars(growth=0.001)
        res = _trendok_one(bars=steady)
        assert res["stopLossParts"]["vol_bin"] == "low"
        noisy = []
        for i in range(120):
            close = 100.0 * (1.01 ** i) * (1.0 + 0.04 * math.sin(i * 1.7))
            noisy.append((_dates(1, start=(date.fromisoformat("2026-01-01") + timedelta(days=i)).isoformat())[0],
                          f"{close * 0.995:.3f}", f"{close * 1.01:.3f}", f"{close * 0.99:.3f}", f"{close:.3f}", "1000000"))
        res2 = _trendok_one(symbol="ETF:510300", bars=noisy)
        assert res2["stopLossParts"]["vol_bin"] == "high"

    def test_sector_divergence_rejection(self) -> None:
        bars = _surge_end(_rising_bars())
        flow_ctx = {"ok": True, "outflow_today_3": {"白酒"}, "top_today_3": set(), "top_today_5": set(),
                    "top_yesterday_3": set(), "top_5d_3": set(), "bottom_5d_5": set(),
                    "net_today": {}, "net_yesterday": {}}
        res = _trendok_one(bars=bars, flow_ctx=flow_ctx, industry="白酒")
        assert res["checks"]["sector_divergence"] is True
        assert res["buyAction"] == "avoid"
        assert res["trendOk"] is False
        assert res["score"] is not None and res["score"] <= 79.0

    def test_t1_sniper(self) -> None:
        n = 120
        dates = _dates(n)
        bars = []
        t1_close = None
        for i, dt in enumerate(dates):
            if i < 118:
                close = 100.0 * (1.01 ** i)
                open_ = close * 0.995
                v = bar_vol
            elif i == 118:
                close = 100.0 * (1.01 ** i) * 1.08
                open_ = 100.0 * (1.01 ** i) * 1.02
                v = bar_vol * 1.2
            else:
                close = t1_close * 0.985
                open_ = t1_close * 0.99
                v = bar_vol * 1.0
            if i == 118:
                t1_close = close
            high = close * 1.006
            low = close * 0.994
            bars.append((dt, f"{open_:.3f}", f"{high:.3f}", f"{low:.3f}", f"{close:.3f}", f"{v:.0f}"))
        res = _trendok_one(bars=bars, market_regime="Weak")
        assert res["checks"]["t1_surge"] is True
        assert res["checks"]["t1_strong"] is True

    def test_intraday_distribution(self) -> None:
        res = _trendok_one(bars=_surge_end(_rising_bars()), rt_vwap=99999.0)
        assert res["checks"]["intraday_distribution"] is True
        assert res["buyAction"] == "avoid"
        assert res["values"]["rtVwap"] == 99999.0

    def test_rs_leader(self) -> None:
        bars = _rising_bars(growth=0.03)
        res = _trendok_one(bars=bars, index_20d_ret=-5.0, index_ema20_down=True, market_regime="Weak")
        assert res["checks"]["rs_leader"] is True
        assert res["rs"] is not None

    def test_insufficient_indicators(self) -> None:
        res = _trendok_one(bars=_rising_bars(25))
        assert "insufficient_indicators" in res["missingData"]

    def test_finalize_trendok_response_caches_prelock(self, monkeypatch) -> None:
        rows = [{"symbol": "CN:600000", "buyAction": "buy"}]
        monkeypatch.setattr(tk, "_read_latest_sentiment_for_macro_lock", lambda: ("extreme_caution", 5000))
        out = tk._finalize_trendok_response(rows)
        assert out[0]["buyAction"] == "avoid"
        assert rows[0]["buyAction"] == "buy"

    def test_read_latest_sentiment_fail_closed(self, monkeypatch) -> None:
        monkeypatch.setattr(tk, "list_days", lambda **kw: (_ for _ in ()).throw(RuntimeError("db down")))
        tk._macro_lock_cache.clear()
        risk, down = tk._read_latest_sentiment_for_macro_lock()
        assert risk == "extreme_caution"
        assert down == tk.MACRO_LOCK_DOWN_THRESHOLD

    def test_read_latest_sentiment_cache(self, monkeypatch) -> None:
        items = [{"riskMode": "Weak", "downCount": 1200}]
        monkeypatch.setattr(tk, "list_days", lambda **kw: items)
        tk._macro_lock_cache.clear()
        assert tk._read_latest_sentiment_for_macro_lock() == ("Weak", 1200)
        monkeypatch.setattr(tk, "list_days", lambda **kw: [])
        assert tk._read_latest_sentiment_for_macro_lock() == ("Weak", 1200)
        tk._macro_lock_cache.clear()

    def test_read_latest_sentiment_empty(self, monkeypatch) -> None:
        monkeypatch.setattr(tk, "list_days", lambda **kw: [])
        monkeypatch.setattr(tk, "get_latest_date", lambda: None)
        tk._macro_lock_cache.clear()
        assert tk._read_latest_sentiment_for_macro_lock() == (None, None)

    def test_clear_caches(self) -> None:
        tk._trendok_cache["k"] = [1]
        tk.clear_trendok_cache()
        assert len(tk._trendok_cache) == 0

    def test_load_alpha_s_symbols_failure(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "data_sync_service.service.watchlist_automation.load_catalyst_window",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert tk._load_alpha_s_symbols() == set()


class _EmptyCur:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        return self

    def fetchall(self):
        return []


class _EmptyConn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return _EmptyCur()


class TestComputeTrendokForSymbols:
    def _patch_db(self, monkeypatch, bars_by_code=None, names=None, industries=None, inst_by_code=None, regime="Strong"):
        from data_sync_service import db as dbmod
        from data_sync_service.db import index_daily, watchlist_automation
        from data_sync_service.service import market_quotes
        from data_sync_service.service import watchlist_automation as svc_wa

        bars_by_code = bars_by_code or {
            "600519.SH": _rising_bars(),
            "000001.SZ": _flat_then_crash(),
        }
        names = names or {"600519.SH": "贵州茅台", "000001.SZ": "平安银行"}
        industries = industries or {"600519.SH": "白酒", "000001.SZ": "银行"}

        monkeypatch.setattr(tk, "fetch_last_ohlcv_batch", lambda codes, days: {c: bars_by_code.get(c, []) for c in codes})
        monkeypatch.setattr(tk, "ensure_stock_basic", lambda: None)
        monkeypatch.setattr(tk, "lookup_em_industries", lambda codes: {c: industries.get(c) for c in codes if c in industries})
        monkeypatch.setattr(tk, "fetch_summaries_for_codes", lambda codes, trade_date=None: inst_by_code or {})
        monkeypatch.setattr(tk, "fetch_daily_seats_batch", lambda keys: {})
        monkeypatch.setattr(tk, "get_stoploss_batch", lambda codes: {})
        monkeypatch.setattr(tk, "upsert_stoploss_batch", lambda rows: None)
        monkeypatch.setattr(tk, "delete_stoploss_batch", lambda codes: None)
        monkeypatch.setattr(tk, "get_latest_industry_date", lambda: "2026-08-07")
        monkeypatch.setattr(tk, "get_dates_upto", lambda as_of, n: _dates(n)[-n:])
        monkeypatch.setattr(tk, "get_rows_for_dates", lambda dates: [])
        monkeypatch.setattr(tk, "trade_dates_upto", lambda flow_date, n, fallback_dates_fn=None: _dates(n)[-n:])
        monkeypatch.setattr(tk, "get_market_regime", lambda **kw: {"regime": regime, "bias": None, "indexSignals": []})
        monkeypatch.setattr(tk, "build_trendok_flow_context_from_rows", lambda **kw: {"asOfDate": kw.get("flow_date"), "ok": True, "top_today_3": set(), "top_today_5": set(), "top_yesterday_3": set(), "top_5d_3": set(), "bottom_5d_5": set(), "net_today": {}, "net_yesterday": {}, "outflow_today_3": set()})
        monkeypatch.setattr(index_daily, "fetch_last_closes", lambda ts_code, days: [])
        monkeypatch.setattr(watchlist_automation, "list_registry", lambda: [])
        monkeypatch.setattr(market_quotes, "normalize_market_symbol", lambda s: s)
        monkeypatch.setattr(dbmod, "get_connection", lambda: _EmptyConn())
        monkeypatch.setattr(svc_wa, "load_catalyst_window", lambda *a, **kw: (None, None))
        return bars_by_code

    def test_compute_empty_symbols(self, monkeypatch) -> None:
        assert tk.compute_trendok_for_symbols([]) == []
        assert tk.compute_trendok_for_symbols([None, "  "]) == []

    def test_compute_caps_200(self, monkeypatch) -> None:
        self._patch_db(monkeypatch)
        syms = [f"CN:60{i:04d}" for i in range(250)]
        out = tk.compute_trendok_for_symbols(syms)
        assert len(out) == 200

    def test_compute_full(self, monkeypatch) -> None:
        self._patch_db(monkeypatch, bars_by_code={
            "600519.SH": _rising_bars(),
            "000001.SZ": _flat_then_crash(),
            "00700.HK": _rising_bars(),
        })
        out = tk.compute_trendok_for_symbols(["CN:600519", "CN:000001", "HK:700"], realtime=False)
        by = {r["symbol"]: r for r in out}
        assert by["CN:600519"]["score"] is not None
        assert by["CN:000001"]["stopLossParts"]["exit_now"] is True
        assert by["HK:700"]["name"] is None and by["HK:700"]["score"] is not None

    def test_compute_realtime_merge(self, monkeypatch) -> None:
        self._patch_db(monkeypatch)
        monkeypatch.setattr(
            tk, "fetch_realtime_quotes",
            lambda codes: {"ok": True, "items": [{"ts_code": "600519.SH", "price": "199.0", "trade_time": "2026-08-07 14:00:00", "volume": "500000", "amount": "5e8"}]},
        )
        out = tk.compute_trendok_for_symbols(["CN:600519"], realtime=True)
        assert out[0]["values"]["rtVwap"] is not None
        assert out[0]["values"]["close"] == 199.0

    def test_compute_registry_held_stoploss(self, monkeypatch) -> None:
        from data_sync_service.db import watchlist_automation

        self._patch_db(monkeypatch)
        monkeypatch.setattr(watchlist_automation, "list_registry", lambda: [{"symbol": "CN:600519", "positionPct": 50, "costPrice": 110.0}])
        out = tk.compute_trendok_for_symbols(["CN:600519"], realtime=False)
        assert "hard_stop_entry" in out[0]["stopLossParts"]

    def test_compute_unsupported_symbol(self, monkeypatch) -> None:
        self._patch_db(monkeypatch)
        out = tk.compute_trendok_for_symbols(["US:APPL"])
        assert out[0]["missingData"] == ["unsupported_market"]

    def test_compute_alpha_s(self, monkeypatch) -> None:
        from data_sync_service.service import watchlist_automation as svc_wa

        self._patch_db(monkeypatch, bars_by_code={"600519.SH": _rising_bars(30, vol_tail=3.0)}, inst_by_code={})
        monkeypatch.setattr(svc_wa, "load_catalyst_window", lambda *a, **kw: (["CN:600519"], ["CN:600519"]))
        out = tk.compute_trendok_for_symbols(["CN:600519"])
        assert out[0]["trendOk"] is True
        assert out[0]["trendStatus"] == "recovering"

    def test_compute_lookup_failure(self, monkeypatch) -> None:
        from data_sync_service import db as dbmod

        self._patch_db(monkeypatch)
        monkeypatch.setattr(dbmod, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
        out = tk.compute_trendok_for_symbols(["CN:600519"])
        assert out[0]["name"] is None

    def test_compute_macro_lock_applied(self, monkeypatch) -> None:
        self._patch_db(monkeypatch, bars_by_code={"600519.SH": _rising_bars()}, inst_by_code={})
        monkeypatch.setattr(tk, "_read_latest_sentiment_for_macro_lock", lambda: ("extreme_caution", 5000))
        out = tk.compute_trendok_for_symbols(["CN:600519"])
        assert out[0]["macroLock"]["active"] is True
        assert out[0]["buyAction"] == "avoid"

    def test_compute_bad_ts_code_symbol(self, monkeypatch) -> None:
        self._patch_db(monkeypatch)
        out = tk.compute_trendok_for_symbols(["CN:12345"])
        assert out[0]["missingData"] == ["unsupported_market"]
