from data_sync_service.service.trendok import (  # type: ignore[import-not-found]
    _compute_watchlist_score_v4,
    _score_anti_spike_penalties,
    _score_bonus_ema20_slope_5d,
    _score_sub_breakout,
    _score_sub_ema,
    _score_sub_macd,
    _score_sub_rsi,
    _score_sub_volume,
)


def test_score_sub_ema_tiers() -> None:
    _, pts_none = _score_sub_ema(10.0, 12.0, 13.0, 12.0)
    assert pts_none == 0.0

    _, pts_one = _score_sub_ema(13.0, 12.0, 13.0, 12.0)
    assert pts_one == 16.0  # only EMA5>EMA20: 0.4 * 40

    _, pts_two = _score_sub_ema(10.0, 12.0, 11.0, 12.0)
    assert pts_two == 16.0  # only EMA20>EMA60: 0.4 * 40

    _, pts_full = _score_sub_ema(13.0, 12.1, 11.0, 12.0)
    assert pts_full == 40.0  # 1.0 * 40


def test_score_sub_macd() -> None:
    _, pts_underwater = _score_sub_macd(-0.1, [0.1, 0.2])
    assert pts_underwater == 0.0

    _, pts_flat = _score_sub_macd(0.5, [0.2, 0.2])
    assert pts_flat == 0.0

    _, pts_full = _score_sub_macd(0.5, [0.1, 0.2])
    assert pts_full == 20.0


def test_score_sub_volume_segments() -> None:
    _, pts_low = _score_sub_volume(0.5)
    assert pts_low == 10.0  # 0.5 * 20

    _, pts_sweet = _score_sub_volume(1.5)
    assert pts_sweet == 20.0

    _, pts_climax = _score_sub_volume(3.5)
    assert pts_climax == 0.0


def test_score_sub_rsi() -> None:
    _, pts_peak = _score_sub_rsi(65.0)
    assert pts_peak == 10.0

    _, pts_70 = _score_sub_rsi(70.0)
    assert abs(pts_70 - 6.667) < 0.01

    _, pts_80 = _score_sub_rsi(80.0)
    assert pts_80 == 0.0


def test_score_sub_breakout_weight() -> None:
    _, pts_full = _score_sub_breakout(100.0, 100.0)
    assert pts_full == 10.0


def test_anti_spike_penalties() -> None:
    penalty, parts = _score_anti_spike_penalties(
        close=100.0,
        ema20=105.0,
        intraday_chg_pct=8.0,
        atr14=7.0,
        vol_today=400.0,
        avg_vol30=100.0,
    )
    assert parts["penalty_intraday_spike"] == -20.0
    assert parts["penalty_volatility_atr"] == -20.0  # (0.07 - 0.05) * 1000
    assert parts["penalty_volume_climax"] == -15.0
    assert parts["penalty_below_ema20"] == -30.0
    assert penalty == 85.0


def test_bonus_ema20_slope_5d() -> None:
    rising = [10.0, 10.1, 10.2, 10.3, 10.4, 10.5]
    assert _score_bonus_ema20_slope_5d(rising) == 5.0

    flat = [10.0, 10.1, 10.2, 10.3, 10.4, 10.4]
    assert _score_bonus_ema20_slope_5d(flat) == 0.0


def test_compute_watchlist_score_v4_spike_pulls_below_80() -> None:
    """Strong subscores but +8% intraday surge should fail the 80 threshold."""
    ema20s = [98.0, 98.5, 99.0, 99.5, 100.0, 100.5, 101.0]
    score, parts = _compute_watchlist_score_v4(
        close=110.0,
        ema5=112.0,
        ema20=101.0,
        ema60=95.0,
        ema20s=ema20s,
        rsi14=68.0,
        avg5=150.0,
        avg30=100.0,
        volume_ratio=1.5,
        macd_last=1.0,
        hist=[0.5, 0.8, 1.0, 1.2],
        high20_high=110.0,
        highs=[100.0] * 19 + [110.0],
        lows=[99.0] * 20,
        closes=[100.0] * 19 + [110.0],
        vols=[100.0] * 29 + [350.0],
        intraday_chg_pct=8.0,
    )
    assert parts["penalty_intraday_spike"] == -20.0
    assert parts["penalty_volume_climax"] == -15.0
    assert score < 80.0


def test_compute_watchlist_score_v4_smooth_right_side_can_reach_80() -> None:
    """Smooth trend without spike penalties can reach >= 80."""
    ema20s = [98.0, 98.5, 99.0, 99.5, 100.0, 100.5, 101.0]
    score, parts = _compute_watchlist_score_v4(
        close=105.0,
        ema5=106.0,
        ema20=101.0,
        ema60=95.0,
        ema20s=ema20s,
        rsi14=66.0,
        avg5=150.0,
        avg30=100.0,
        volume_ratio=1.5,
        macd_last=1.0,
        hist=[0.5, 0.8, 1.0, 1.2],
        high20_high=105.0,
        highs=[100.0] * 19 + [101.0],
        lows=[99.0] * 20,
        closes=[100.0] * 19 + [101.0],
        vols=[100.0] * 29 + [150.0],
        intraday_chg_pct=3.0,
    )
    assert "penalty_intraday_spike" not in parts
    assert "bonus_ema20_slope_5d" in parts
    assert score >= 80.0


def test_compute_watchlist_score_v4_uses_explicit_volume_ratio() -> None:
    ema20s = [98.0, 98.5, 99.0, 99.5, 100.0, 100.5, 101.0]
    _, parts_low = _compute_watchlist_score_v4(
        close=101.0,
        ema5=102.0,
        ema20=100.0,
        ema60=98.0,
        ema20s=ema20s,
        rsi14=66.0,
        avg5=150.0,
        avg30=100.0,
        volume_ratio=0.8,
        macd_last=1.0,
        hist=[0.5, 0.8, 1.0, 1.2],
        high20_high=105.0,
        highs=[100.0] * 19 + [101.0],
        lows=[99.0] * 20,
        closes=[100.0] * 19 + [101.0],
        vols=[100.0] * 29 + [150.0],
        intraday_chg_pct=3.0,
    )
    _, parts_full = _compute_watchlist_score_v4(
        close=101.0,
        ema5=102.0,
        ema20=100.0,
        ema60=98.0,
        ema20s=ema20s,
        rsi14=66.0,
        avg5=150.0,
        avg30=100.0,
        volume_ratio=1.5,
        macd_last=1.0,
        hist=[0.5, 0.8, 1.0, 1.2],
        high20_high=105.0,
        highs=[100.0] * 19 + [101.0],
        lows=[99.0] * 20,
        closes=[100.0] * 19 + [101.0],
        vols=[100.0] * 29 + [150.0],
        intraday_chg_pct=3.0,
    )

    assert parts_low["volume"] < parts_full["volume"]


def test_score_subs_are_none_safe() -> None:
    """H7 (2026-08-08): scoring sub-functions must tolerate None inputs
    (missing indicators) instead of raising TypeError."""
    from data_sync_service.service.trendok import (
        _clip01,
        _score_sub_breakout,
        _score_sub_ema,
        _score_sub_macd,
        _score_sub_rsi,
        _score_sub_volume,
    )

    assert _clip01(None) == 0.0
    assert _score_sub_ema(None, None, None, None) == (0.0, 0.0)
    assert _score_sub_ema(1.0, 0.5, 0.4, None) == (0.8, 32.0)
    assert _score_sub_macd(None, []) == (0.0, 0.0)
    assert _score_sub_macd(0.5, []) == (0.0, 0.0)
    assert _score_sub_breakout(None, None) == (0.0, 0.0)
    assert _score_sub_breakout(10.0, None) == (0.0, 0.0)
    assert _score_sub_rsi(None) == (0.0, 0.0)
    assert _score_sub_volume(None) == (0.0, 0.0)
    assert _score_sub_volume(0.8) == (0.8, 16.0)
