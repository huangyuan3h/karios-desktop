"""B-T1: TrendOKParams parameterization — default parity + knob behaviour."""

from data_sync_service.service.trendok import (
    _compute_watchlist_score_v4,
    _industry_flow_score_adjustment,
    _score_anti_spike_penalties,
    _score_sub_ema,
    _score_sub_macd,
    _score_sub_volume,
    compute_trendok_for_symbols,
)
from data_sync_service.service.trendok_params import DEFAULT_TRENDOK_PARAMS, TrendOKParams


def _sample_score_kwargs():
    return dict(
        close=10.0,
        ema5=10.5,
        ema20=10.0,
        ema60=9.5,
        ema20s=[9.0, 9.2, 9.4, 9.6, 9.8, 10.0],
        rsi14=65.0,
        avg5=1_000_000,
        avg30=1_000_000,
        volume_ratio=1.5,
        macd_last=0.1,
        hist=[0.05, 0.06, 0.07, 0.08],
        high20_high=11.0,
        highs=[9.0 + i * 0.1 for i in range(20)],
        lows=[8.5 + i * 0.1 for i in range(20)],
        closes=[9.0 + i * 0.1 for i in range(20)],
        vols=[1_000_000] * 20,
        intraday_chg_pct=2.0,
    )


def test_default_parity_weights():
    p = DEFAULT_TRENDOK_PARAMS
    assert p.w_ema == 0.40 and p.w_vol == 0.20
    assert p.failed_score_cap == 79.0
    assert p.flow_5d_top3 == 10.0 and p.flow_5d_bottom5 == -20.0


def test_score_sub_ema_respects_w_ema():
    kw = dict(ema5=10.5, ema20=10.0, ema60=9.5, ema20_prev=9.8)
    _, pts_default = _score_sub_ema(**kw, params=DEFAULT_TRENDOK_PARAMS)
    p2 = TrendOKParams(w_ema=0.20, w_macd=0.20, w_break=0.10, w_rsi=0.10, w_vol=0.40)
    _, pts_half = _score_sub_ema(**kw, params=p2)
    # w_ema halved => pts halved for same s_ema (=1.0)
    assert pts_half == pts_default * 0.5


def test_score_sub_volume_breaks():
    # volume_ratio 1.1: default 1.2 break => 0.5+0.5*(0.1/0.2)=0.75, with tighter 1.1 break => full ramp
    _, pts_default = _score_sub_volume(1.1, params=DEFAULT_TRENDOK_PARAMS)
    p_tight = TrendOKParams(vol_break_2=1.1)
    _, pts_tight = _score_sub_volume(1.1, params=p_tight)
    assert pts_tight != pts_default


def test_bonus_slope_param():
    # 5 consecutive rises => bonus = param
    ema20s = [9.0, 9.1, 9.2, 9.3, 9.4, 9.5]
    from data_sync_service.service.trendok import _score_bonus_ema20_slope_5d

    assert _score_bonus_ema20_slope_5d(ema20s, params=DEFAULT_TRENDOK_PARAMS) == 5.0
    assert _score_bonus_ema20_slope_5d(ema20s, params=TrendOKParams(bonus_ema20_slope_5d=0)) == 0.0
    assert _score_bonus_ema20_slope_5d(ema20s, params=TrendOKParams(bonus_ema20_slope_5d=10)) == 10.0


def test_anti_spike_penalties_param():
    kw = dict(close=10.0, ema20=9.5, intraday_chg_pct=7.0, atr14=0.6, vol_today=4_000_000, avg_vol30=1_000_000)
    pen_default, _ = _score_anti_spike_penalties(**kw, params=DEFAULT_TRENDOK_PARAMS)
    # raise threshold to 8% => 7% no longer triggers intraday penalty (-20)
    pen_high_thr, _ = _score_anti_spike_penalties(**kw, params=TrendOKParams(intraday_surge_threshold_pct=8.0))
    assert pen_high_thr < pen_default
    assert pen_high_thr == pen_default - 20.0


def test_flow_adjustment_param():
    ctx = {
        "ok": True,
        "top_5d_3": {"钢铁"},
        "bottom_5d_5": set(),
        "top_today_3": set(),
        "top_today_5": set(),
        "top_yesterday_3": set(),
        "net_today": {},
        "net_yesterday": {},
    }
    d0, _, _ = _industry_flow_score_adjustment("钢铁", ctx, params=DEFAULT_TRENDOK_PARAMS)
    assert d0 == 10.0
    p2 = TrendOKParams(flow_5d_top3=7.0)
    d1, _, _ = _industry_flow_score_adjustment("钢铁", ctx, params=p2)
    assert d1 == 7.0


def test_compute_score_v4_respects_params():
    kw = _sample_score_kwargs()
    # tweak to avoid 100 cap: use rsi far from 65 to lower s_rsi
    kw["rsi14"] = 80.0
    kw["volume_ratio"] = 0.8
    s0, parts0 = _compute_watchlist_score_v4(**kw, params=DEFAULT_TRENDOK_PARAMS)
    p2 = TrendOKParams(w_ema=0.1, w_vol=0.5, w_macd=0.1, w_break=0.1, w_rsi=0.2)
    s1, parts1 = _compute_watchlist_score_v4(**kw, params=p2)
    assert s0 != s1 or parts0 != parts1


def test_compute_trendok_for_symbols_param_bypass(monkeypatch):
    # Non-default params must bypass cache and reflect in score
    import data_sync_service.service.trendok as tk

    monkeypatch.setattr(tk, "fetch_last_ohlcv_batch", lambda codes, days=120: {c: [("2026-08-01", "10", "11", "9", "10", "1000000")] * 80 for c in codes})
    monkeypatch.setattr(tk, "_lookup_stock_basic", lambda codes: ({c: "Name" for c in codes}, {}))
    monkeypatch.setattr(tk, "_lookup_em_industry_boards", lambda codes: {})
    monkeypatch.setattr(tk, "fetch_summaries_for_codes", lambda codes, trade_date=None: {})
    monkeypatch.setattr(tk, "get_market_regime", lambda as_of_date=None, include_breadth=False: {"regime": "Strong"})
    monkeypatch.setattr(tk, "_build_industry_flow_context", lambda d: {"ok": False})
    monkeypatch.setattr(tk, "fetch_daily_seats_batch", lambda keys: {})
    monkeypatch.setattr(tk, "get_stoploss_batch", lambda codes: {})

    # default
    r0 = compute_trendok_for_symbols(["CN:600000"], realtime=False)
    assert len(r0) == 1
    # experimental: w_ema 0 should change score
    p = TrendOKParams(w_ema=0.0, w_macd=0.0, w_break=0.0, w_rsi=0.0, w_vol=1.0)
    r1 = compute_trendok_for_symbols(["CN:600000"], realtime=False, params=p)
    assert len(r1) == 1
    # scores differ when recipe differs (unless both edge-capped)
    # At least scoreParts differ
    assert r0[0].get("scoreParts") != r1[0].get("scoreParts")
