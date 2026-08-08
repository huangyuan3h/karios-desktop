"""trendok: alpha-S recovering, risk buy blocks, quote merge, industry flow."""

from __future__ import annotations

import datetime

import pandas as pd

from data_sync_service.service import trendok as tk


def test_load_alpha_s_symbols(monkeypatch) -> None:
    import data_sync_service.service.watchlist_automation as wa

    monkeypatch.setattr(wa, "load_catalyst_window", lambda: (["a", "b"], ["s1", "s2"]))
    assert tk._load_alpha_s_symbols() == {"s1", "s2"}
    monkeypatch.setattr(wa, "load_catalyst_window", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert tk._load_alpha_s_symbols() == set()


def test_volume_vs_avg10() -> None:
    assert tk._volume_vs_avg10([1.0] * 10) is None
    vols = [1.0] * 10 + [2.0]
    assert tk._volume_vs_avg10(vols) == 2.0
    assert tk._volume_vs_avg10([0.0] * 10 + [1.0]) is None  # avg 0


def test_is_bullish_day() -> None:
    assert tk._is_bullish_day([10.0, 10.5], [10.2]) is True
    assert tk._is_bullish_day([10.0, 10.4], [10.2]) is True  # close > open and >= prev close
    assert tk._is_bullish_day([10.0], [10.2]) is False
    assert tk._is_bullish_day([10.0, 11.0], []) is False


def test_apply_alpha_s_recovering_hit(monkeypatch) -> None:
    monkeypatch.setattr(tk, "_volume_vs_avg10", lambda vols: 3.0)
    monkeypatch.setattr(tk, "_is_bullish_day", lambda closes, opens: True)
    res = {"score": 40.0, "trendOk": False, "scoreParts": {}}
    tk.apply_alpha_s_trend_recovering(res, closes=[1.0] * 11, opens=[0.5] * 11, vols=[1.0] * 11, is_alpha_s=True)
    assert res["trendOk"] is True
    assert res["score"] == 60.0
    assert res["trendStatus"] == "recovering"
    assert res["checks"]["alphaSTrendRecovering"] is True
    assert res["scoreParts"]["alpha_s_trend_recovering"] == 60.0
    assert res["values"]["volVsAvg10"] == 3.0


def test_apply_alpha_s_recovering_miss_and_bad_parts(monkeypatch) -> None:
    monkeypatch.setattr(tk, "_volume_vs_avg10", lambda vols: 1.0)
    monkeypatch.setattr(tk, "_is_bullish_day", lambda closes, opens: True)
    res = {"score": "bad", "trendOk": True}
    tk.apply_alpha_s_trend_recovering(res, closes=[1.0] * 11, opens=[0.5] * 11, vols=[1.0] * 11, is_alpha_s=True)
    assert res["trendStatus"] == "ok"
    assert res["checks"]["alphaSTrendRecovering"] is False

    res2 = {"score": 70, "trendOk": False, "scoreParts": "not-a-dict"}
    tk.apply_alpha_s_trend_recovering(res2, closes=[1.0] * 11, opens=[0.5] * 11, vols=[1.0] * 11, is_alpha_s=False)
    assert res2["trendStatus"] == "no"


def test_macro_override_lock_active() -> None:
    assert tk.macro_override_lock_active("capitulation_v_bottom", 5000) is False
    assert tk.macro_override_lock_active("extreme_caution", 0) is True
    assert tk.macro_override_lock_active(None, 4000) is True
    assert tk.macro_override_lock_active(None, 100) is False
    assert tk.macro_override_lock_active(None, None) is False


def test_atr14() -> None:
    assert tk._atr14([1.0, 2.0], [1.0, 2.0], [1.0, 2.0]) is None
    highs = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0]
    lows = [9.0, 9.5, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0, 13.5, 14.0, 14.5, 15.0, 15.5, 16.0]
    closes = [9.5, 10.0, 10.5, 11.0, 11.5, 12.0, 12.5, 13.0, 13.5, 14.0, 14.5, 15.0, 15.5, 16.0, 16.5]
    atr = tk._atr14(highs, lows, closes)
    assert atr is not None and atr > 0
    assert tk._atr14(highs, lows, closes, period=0) is None


def test_score_sub_volume(monkeypatch) -> None:
    assert tk._score_sub_volume(None) == (0.0, 0.0)
    s1, _ = tk._score_sub_volume(0.5)
    assert s1 == 0.5
    s2, _ = tk._score_sub_volume(1.1)
    assert 0.5 < s2 < 1.0
    assert tk._score_sub_volume(1.5)[0] == 1.0
    assert tk._score_sub_volume(2.5)[0] == 0.5
    assert tk._score_sub_volume(5.0)[0] == 0.0


def test_score_bonus_ema20_slope_5d() -> None:
    assert tk._score_bonus_ema20_slope_5d([1.0] * 5) == 0.0
    rising = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9]
    assert tk._score_bonus_ema20_slope_5d(rising) == 5.0
    flat = [1.0, 1.1, 1.2, 1.3, 1.4, 1.4]
    assert tk._score_bonus_ema20_slope_5d(flat) == 0.0


def test_apply_inst_flow_risk_buy_blocks() -> None:
    base = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 8.0, "buyChecks": {}}
    inst = {"onBoard": True, "lhasaDominant": True, "instNetBuyYi": -1.0}
    tk._apply_inst_flow_risk_buy_blocks(base, inst_flow=inst)
    assert base["buyAction"] == "avoid"
    assert base["buyChecks"]["blocked_inst_retail_chase"] is True

    base2 = {"stopLossParts": {"exit_now": 1}, "riskMetricsLive": True, "intradayChgPct": 8.0}
    tk._apply_inst_flow_risk_buy_blocks(base2, inst_flow=inst)
    assert "buyAction" not in base2

    base3 = {"stopLossParts": {}, "riskMetricsLive": False, "intradayChgPct": 8.0}
    tk._apply_inst_flow_risk_buy_blocks(base3, inst_flow=inst)
    assert "buyAction" not in base3

    base4 = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 5.0, "buyChecks": {}}
    tk._apply_inst_flow_risk_buy_blocks(base4, inst_flow=inst)
    assert "buyAction" not in base4  # below surge threshold


def test_block_buy_if_entry_at_or_below_stop() -> None:
    res = {"buyZoneHigh": 11.0, "stopLossPrice": 10.0, "buyChecks": {}}
    tk._block_buy_if_entry_at_or_below_stop(res)
    assert "buyAction" not in res  # entry strictly above stop → allowed
    res2 = {"buyZoneHigh": 10.0, "stopLossPrice": 11.0, "buyChecks": {}}
    tk._block_buy_if_entry_at_or_below_stop(res2)
    assert res2["buyAction"] == "avoid"
    res3 = {"buyZoneHigh": "x", "stopLossPrice": 10.0, "buyChecks": {}}
    tk._block_buy_if_entry_at_or_below_stop(res3)
    assert "buyAction" not in res3


def test_score_for_momentum_surge_gate() -> None:
    assert tk._score_for_momentum_surge_gate({}) is None
    assert tk._score_for_momentum_surge_gate({"score": "bad"}) is None
    res = {"score": 90.0, "scoreParts": {"penalty_intraday_spike": -5.0}}
    assert tk._score_for_momentum_surge_gate(res) == 95.0
    res2 = {"score": 90.0, "scoreParts": {}}
    assert tk._score_for_momentum_surge_gate(res2) == 90.0


def test_is_momentum_surge_eligible() -> None:
    base = {"buyMode": "B_momentum", "trendOk": True, "score": 90.0, "scoreParts": {}}
    assert tk._is_momentum_surge_eligible(base, intraday_pct=8.0) is True
    assert tk._is_momentum_surge_eligible(base, intraday_pct=10.0) is False
    base2 = {**base, "buyMode": "FE"}
    assert tk._is_momentum_surge_eligible(base2, intraday_pct=8.0) is False
    base3 = {**base, "trendOk": False}
    assert tk._is_momentum_surge_eligible(base3, intraday_pct=8.0) is False
    base4 = {**base, "score": 80.0}
    assert tk._is_momentum_surge_eligible(base4, intraday_pct=8.0) is False


def test_apply_intraday_risk_buy_blocks() -> None:
    res = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 8.0,
           "buyMode": "B_momentum", "trendOk": True, "score": 90.0, "scoreParts": {}, "buyWhy": ""}
    tk._apply_intraday_risk_buy_blocks(res, market_regime="Weak")
    assert res["buyChecks"]["momentum_surge_allow"] is True
    assert "TIP-007" in res["buyWhy"]

    res2 = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 8.0,
            "buyMode": "FE", "buyChecks": {}}
    tk._apply_intraday_risk_buy_blocks(res2, market_regime="Weak")
    assert res2["buyAction"] == "avoid"
    assert res2["buyChecks"]["blocked_intraday_surge"] is True

    res3 = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 3.0,
            "gapUp": True, "buyMode": "B_momentum", "buyAction": "buy", "buyChecks": {}}
    tk._apply_intraday_risk_buy_blocks(res3, market_regime="Weak")
    assert res3["buyAction"] == "avoid"
    assert res3["buyChecks"]["blocked_gap_up_weak_market"] is True

    res4 = {"stopLossParts": {}, "riskMetricsLive": True, "intradayChgPct": 3.0,
            "gapUp": True, "buyMode": "FE", "buyAction": "hold", "buyChecks": {}, "buyWhy": ""}
    tk._apply_intraday_risk_buy_blocks(res4, market_regime="Weak")
    assert res4["buyChecks"]["blocked_gap_up_weak_market"] is True
    assert "禁止追高" in res4["buyWhy"]

    res5 = {"stopLossParts": {"exit_now": 1}, "riskMetricsLive": True, "intradayChgPct": 8.0, "buyChecks": {}}
    tk._apply_intraday_risk_buy_blocks(res5, market_regime="Weak")
    assert res5["buyChecks"] == {} and "buyAction" not in res5


def test_quote_trade_date_and_pick_str() -> None:
    assert tk._quote_trade_date({"trade_time": "2026-08-07 10:30:00"}) == "2026-08-07"
    assert tk._quote_trade_date({"trade_time": "20260807"}) == "2026-08-07"
    assert tk._quote_trade_date({}) is None
    assert tk._quote_trade_date({"trade_time": "garbage"}) is None
    assert tk._pick_str(None, "fb") == "fb"
    assert tk._pick_str("  ", "fb") == "fb"
    assert tk._pick_str("x", "fb") == "x"


def test_merge_realtime_bar(monkeypatch) -> None:
    bars = [("2026-08-06", "10.0", "10.5", "9.8", "10.2", "100")]
    assert tk._merge_realtime_bar([], {"price": 10.5}) == []
    assert tk._merge_realtime_bar(bars, {"price": "x"}) == bars
    monkeypatch.setattr(tk, "_quote_trade_date", lambda q: "2026-08-05")
    monkeypatch.setattr(tk, "_shanghai_today_iso", lambda: "2026-08-07")
    older = tk._merge_realtime_bar(bars, {"price": 10.5, "trade_time": "2026-08-05 09:30"})
    assert older == bars  # stale date ignored
    monkeypatch.setattr(tk, "_quote_trade_date", lambda q: "2026-08-06")
    same_day = tk._merge_realtime_bar(bars, {"price": 10.8, "open": 10.1, "high": 10.9, "low": 10.0, "volume": 200})
    assert len(same_day) == 1 and same_day[0][0] == "2026-08-06" and same_day[0][4] == "10.8"
    monkeypatch.setattr(tk, "_quote_trade_date", lambda q: "2026-08-07")
    next_day = tk._merge_realtime_bar(bars, {"price": 10.9})
    assert len(next_day) == 2 and next_day[1][0] == "2026-08-07" and next_day[1][5] == "0"
    monkeypatch.undo()


def test_symbol_to_ts_code() -> None:
    assert tk._symbol_to_ts_code("CN:600000") == ("CN", "600000", "600000.SH")
    assert tk._symbol_to_ts_code("CN:000001") == ("CN", "000001", "000001.SZ")
    assert tk._symbol_to_ts_code("HK:700") == ("HK", "00700", "00700.HK")
    assert tk._symbol_to_ts_code("ETF:510300") == ("ETF", "510300", "510300.SH")
    assert tk._symbol_to_ts_code("ETF:159915") == ("ETF", "159915", "159915.SZ")
    assert tk._symbol_to_ts_code("CN:12ab") is None
    assert tk._symbol_to_ts_code("") is None


def test_lookup_stock_basic(monkeypatch) -> None:
    assert tk._lookup_stock_basic([]) == ({}, {})

    class _Cur:
        def execute(self, sql, params):
            pass

        def fetchall(self):
            return [("600000.SH", "浦发银行", "银行"), ("000001.SZ", "平安银行", None)]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    import data_sync_service.db as dbmod

    monkeypatch.setattr(tk, "ensure_stock_basic", lambda: None)
    monkeypatch.setattr(dbmod, "get_connection", lambda: _Conn())
    by_name, by_industry = tk._lookup_stock_basic(["600000.SH"])
    assert by_name["600000.SH"] == "浦发银行"
    assert by_industry["600000.SH"] == "银行"
    assert "000001.SZ" not in by_industry

    monkeypatch.setattr(dbmod, "get_connection", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert tk._lookup_stock_basic(["600000.SH"]) == ({}, {})


def test_lookup_em_industry_boards(monkeypatch) -> None:
    assert tk._lookup_em_industry_boards([]) == {}
    monkeypatch.setattr(tk, "lookup_em_industries", lambda codes: {"600000.SH": "银行"})
    assert tk._lookup_em_industry_boards(["600000.SH"]) == {"600000.SH": "银行"}
    monkeypatch.setattr(tk, "lookup_em_industries", lambda codes: (_ for _ in ()).throw(RuntimeError("x")))
    assert tk._lookup_em_industry_boards(["600000.SH"]) == {}


def test_pick_flow_as_of_date(monkeypatch) -> None:
    monkeypatch.setattr(tk, "get_latest_industry_date", lambda: "2026-08-06")
    assert tk._pick_flow_as_of_date("2026-08-07") == "2026-08-06"
    assert tk._pick_flow_as_of_date("2026-08-05") == "2026-08-05"
    assert tk._pick_flow_as_of_date(None) == "2026-08-06"
    monkeypatch.setattr(tk, "get_latest_industry_date", lambda: None)
    assert tk._pick_flow_as_of_date("2026-08-07") == "2026-08-07"
    assert tk._pick_flow_as_of_date(None) is None


def test_build_industry_flow_context(monkeypatch) -> None:
    monkeypatch.setattr(tk, "_pick_flow_as_of_date", lambda d: None)
    assert tk._build_industry_flow_context("2026-08-07") == {"asOfDate": None, "ok": False}

    monkeypatch.setattr(tk, "_pick_flow_as_of_date", lambda d: "2026-08-07")
    monkeypatch.setattr(tk, "trade_dates_upto", lambda fd, n, fallback_dates_fn=None: [])
    ctx = tk._build_industry_flow_context("2026-08-07")
    assert ctx == {"asOfDate": "2026-08-07", "ok": False}

    monkeypatch.setattr(tk, "trade_dates_upto", lambda fd, n, fallback_dates_fn=None: ["2026-08-07", "2026-08-06"])
    monkeypatch.setattr(tk, "get_rows_for_dates", lambda dates: [{"net_inflow": 1.0}])
    monkeypatch.setattr(tk, "build_trendok_flow_context_from_rows", lambda flow_date, dates_5, rows: {"ok": True, "flow_date": flow_date})
    ctx2 = tk._build_industry_flow_context("2026-08-07")
    assert ctx2["ok"] is True


def test_industry_flow_score_adjustment() -> None:
    ctx = {
        "ok": True,
        "top_5d_3": {"银行"},
        "bottom_5d_5": {"地产"},
        "top_today_3": {"券商"},
        "top_today_5": {"白酒"},
        "top_yesterday_3": {"半导体"},
        "net_today": {"半导体": -2e8, "地产": -1.5e8},
        "net_yesterday": {"地产": -2e8},
    }
    delta, parts, reasons = tk._industry_flow_score_adjustment("银行", ctx)
    assert delta == 10.0
    delta2, parts2, _ = tk._industry_flow_score_adjustment("券商", ctx)
    assert delta2 == 5.0
    delta3, parts3, _ = tk._industry_flow_score_adjustment("白酒", ctx)
    assert delta3 == 3.0
    delta4, parts4, reasons4 = tk._industry_flow_score_adjustment("半导体", ctx)
    assert delta4 == -15.0
    assert "hotspot_falloff_big_outflow" in reasons4
    delta5, parts5, reasons5 = tk._industry_flow_score_adjustment("地产", ctx)
    assert delta5 == -20.0 - 10.0
    delta6, _, _ = tk._industry_flow_score_adjustment("", ctx)
    assert delta6 == 0.0
    delta7, _, _ = tk._industry_flow_score_adjustment("银行", {"ok": False})
    assert delta7 == 0.0


def test_normalize_yyyy_mm_dd() -> None:
    assert tk._normalize_yyyy_mm_dd(None) is None
    assert tk._normalize_yyyy_mm_dd(datetime.date(2026, 8, 7)) == "2026-08-07"
    assert tk._normalize_yyyy_mm_dd("20260807") == "2026-08-07"
    assert tk._normalize_yyyy_mm_dd("2026-08-07") == "2026-08-07"


def test_resolve_inst_summaries_for_trendok(monkeypatch) -> None:
    monkeypatch.setattr(tk, "fetch_summaries_for_codes", lambda codes, trade_date=None: {"600000.SH": {"netBuy": 1}})
    out = tk._resolve_inst_summaries_for_trendok(["600000.SH"], latest_bar_date="2026-08-07")
    assert out["600000.SH"]["netBuy"] == 1

    calls = []

    def fake_fetch(codes, trade_date=None):
        calls.append((list(codes), trade_date))
        if trade_date == "2026-08-07":
            return {"600000.SH": {"netBuy": 1}}
        return {"000001.SZ": {"netBuy": 2}}

    monkeypatch.setattr(tk, "fetch_summaries_for_codes", fake_fetch)
    out2 = tk._resolve_inst_summaries_for_trendok(["600000.SH", "000001.SZ"], latest_bar_date="2026-08-07")
    assert set(out2) == {"600000.SH", "000001.SZ"}
    assert len(calls) == 2  # second call without trade_date for missing
