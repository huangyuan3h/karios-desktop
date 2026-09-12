"""Pure unit tests for backtest_engine uncovered lines (no DB, no network).

Covers ``_board_limit_pct`` / ``_at_limit`` edges, ``_env_position_scale``,
every ``BacktestConfig.__post_init__`` raise not already covered by
``test_backtest_engine.py`` (plus valid-value paths), and the tail helpers
``_summarize`` / ``_window_years`` / ``_sharpe_from_closes`` /
``with_benchmark_excess`` / ``run_sensitivity`` / ``default_sensitivity_grid``
/ ``_calendar_days_between``.

All datasets are synthetic in-memory fakes; ``BacktestData`` is built via
``__new__`` (never ``__init__``, which hits the DB). ``run_sensitivity`` is
tested with ``monkeypatch``-swapped module attributes.
"""

from __future__ import annotations

import statistics
from types import SimpleNamespace

import pytest

from data_sync_service.service import backtest_engine as be
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    BacktestSummary,
    BacktestTrade,
    _at_limit,
    _board_limit_pct,
    _calendar_days_between,
    _sharpe_from_closes,
    _summarize,
    _window_years,
    default_sensitivity_grid,
    run_sensitivity,
    with_benchmark_excess,
)

START = "2026-08-01"
END = "2026-08-07"


def _mk_config(**overrides) -> BacktestConfig:
    return BacktestConfig(start_date=START, end_date=END, **overrides)


def _mk_data(calendar: list[str]) -> BacktestData:
    data = BacktestData.__new__(BacktestData)
    data.calendar = list(calendar)
    return data


def _mk_trade(
    pnl: float,
    close_date: str = "2026-08-04",
    score: float | None = 88.0,
    position_pct: float = 0.05,
) -> BacktestTrade:
    return BacktestTrade(
        symbol="CN:600001",
        market="CN",
        entry_date="2026-08-01",
        entry_price=10.0,
        close_date=close_date,
        close_price=10.0 * (1.0 + pnl / 100.0),
        gross_pnl_pct=pnl + 0.3,
        costs_pct=0.3,
        pnl_pct=pnl,
        holding_days=3,
        close_reason="max_hold",
        score_at_entry=score,
        position_pct=position_pct,
    )


def _mk_summary(annual: float | None = 5.0) -> BacktestSummary:
    return BacktestSummary(
        config={},
        calendar_days=5,
        trades=0,
        closed=0,
        open_at_end=0,
        wins=0,
        losses=0,
        win_rate=None,
        avg_net_pnl_pct=None,
        avg_gross_pnl_pct=None,
        avg_costs_pct=None,
        max_drawdown_pct=0.0,
        total_net_pnl_pct=0.0,
        annual_net_pnl_pct=annual,  # type: ignore[arg-type]
        avg_win_pct=None,
        avg_loss_pct=None,
        sharpe=None,
        excess_vs_best_benchmark_pct=0.0,
        best_benchmark="",
    )


# ---------------------------------------------------------------------------
# _board_limit_pct
# ---------------------------------------------------------------------------


def test_pure_board_limit_main_board_ten_pct() -> None:
    assert _board_limit_pct("600001.SH") == 0.10
    assert _board_limit_pct("000001.SZ") == 0.10


def test_pure_board_limit_chinext_star_twenty_pct() -> None:
    assert _board_limit_pct("300001.SZ") == 0.20
    assert _board_limit_pct("301000.SZ") == 0.20
    assert _board_limit_pct("688001.SH") == 0.20


def test_pure_board_limit_bse_thirty_pct() -> None:
    assert _board_limit_pct("830000.BJ") == 0.30
    assert _board_limit_pct("430000.BJ") == 0.30


def test_pure_board_limit_none_for_etf() -> None:
    # ETFs carry no board limit in this model.
    assert _board_limit_pct("510300.SH") is None
    assert _board_limit_pct("159915.SZ") is None


# ---------------------------------------------------------------------------
# _at_limit
# ---------------------------------------------------------------------------


def _limit_data(series: dict[str, list[tuple[str, float]]]) -> SimpleNamespace:
    return SimpleNamespace(closes_by_ts=dict(series))


def test_pure_at_limit_without_board_limit_is_false() -> None:
    data = _limit_data({"510300.SH": [("2026-08-01", 3.0)]})
    assert _at_limit(data, "510300.SH", "2026-08-04", 3.6, up=True) is False
    assert _at_limit(data, "510300.SH", "2026-08-04", 2.4, up=False) is False


def test_pure_at_limit_missing_series_is_false() -> None:
    assert _at_limit(_limit_data({}), "600001.SH", "2026-08-04", 11.0, up=True) is False
    data = _limit_data({"600001.SH": []})
    assert _at_limit(data, "600001.SH", "2026-08-04", 11.0, up=True) is False


def test_pure_at_limit_without_prior_close_is_false() -> None:
    # Series starts on the day itself: no previous close to derive the limit.
    data = _limit_data({"600001.SH": [("2026-08-04", 10.0)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 11.0, up=True) is False
    # A zero previous close cannot anchor a limit price.
    data = _limit_data({"600001.SH": [("2026-08-01", 0.0)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 0.0, up=False) is False


def test_pure_at_limit_up_pinned_blocks() -> None:
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 11.0)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 11.0, up=True) is True


def test_pure_at_limit_up_not_pinned_passes() -> None:
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 10.5)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 10.5, up=True) is False


def test_pure_at_limit_down_pinned_blocks() -> None:
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 9.0)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 9.0, up=False) is True


def test_pure_at_limit_down_not_pinned_passes() -> None:
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 9.5)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 9.5, up=False) is False


def test_pure_at_limit_chinext_twenty_pct_and_tolerance() -> None:
    data = _limit_data({"300001.SZ": [("2026-08-01", 10.0), ("2026-08-04", 12.0)]})
    assert _at_limit(data, "300001.SZ", "2026-08-04", 12.0, up=True) is True
    assert _at_limit(data, "300001.SZ", "2026-08-04", 11.0, up=True) is False
    # One-cent tolerance absorbs qfq rounding at the pin.
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 10.99)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 10.99, up=True) is True
    data = _limit_data({"600001.SH": [("2026-08-01", 10.0), ("2026-08-04", 9.01)]})
    assert _at_limit(data, "600001.SH", "2026-08-04", 9.01, up=False) is True


# ---------------------------------------------------------------------------
# _env_position_scale
# ---------------------------------------------------------------------------


def test_pure_env_scale_disabled_returns_one() -> None:
    cfg = _mk_config()
    assert cfg.env_position_scale == ""
    assert cfg._env_position_scale("uptrend") == 1.0
    assert cfg._env_position_scale(None) == 1.0


def test_pure_env_scale_matches_entry_env() -> None:
    cfg = _mk_config(env_position_scale="uptrend:1.2,fan:0.8")
    assert cfg._env_position_scale("uptrend") == pytest.approx(1.2)
    assert cfg._env_position_scale("fan") == pytest.approx(0.8)


def test_pure_env_scale_unmapped_env_returns_one() -> None:
    cfg = _mk_config(env_position_scale="uptrend:1.2,fan:0.8")
    assert cfg._env_position_scale("weak") == 1.0
    assert cfg._env_position_scale(None) == 1.0


def test_pure_env_scale_malformed_value_returns_one() -> None:
    cfg = _mk_config(env_position_scale="uptrend:abc")
    assert cfg._env_position_scale("uptrend") == 1.0
    cfg = _mk_config(env_position_scale="uptrend:")
    assert cfg._env_position_scale("uptrend") == 1.0


def test_pure_env_scale_negative_clamped_to_zero() -> None:
    cfg = _mk_config(env_position_scale="uptrend:-2.0")
    assert cfg._env_position_scale("uptrend") == 0.0


def test_pure_env_scale_tolerates_whitespace() -> None:
    cfg = _mk_config(env_position_scale=" uptrend : 1.5 ")
    assert cfg._env_position_scale("uptrend") == pytest.approx(1.5)


def test_pure_trend_guide_defaults_off() -> None:
    # P0-12 DH trend-guide overlay: default OFF → zero behavior change.
    cfg = _mk_config()
    assert cfg.trend_guide_code == ""
    assert cfg.trend_guide_ma == 200
    assert cfg.trend_guide_stop_pct == 0.0
    assert cfg.trend_guide_trail_pct == 0.0
    cfg2 = _mk_config(
        trend_guide_code="000905.SH",
        trend_guide_ma=60,
        trend_guide_stop_pct=-3.0,
        trend_guide_trail_pct=-5.0,
    )
    assert cfg2.trend_guide_code == "000905.SH"
    assert cfg2.trend_guide_ma == 60
    assert cfg2.trend_guide_stop_pct == -3.0
    assert cfg2.trend_guide_trail_pct == -5.0


# ---------------------------------------------------------------------------
# BacktestConfig.__post_init__ validation (one test per raise; the raises for
# market / window / score_threshold / gates / trailing_stop_pct / rs_rank_min /
# trend_score_min / exclude_boards are already covered in test_backtest_engine.py)
# ---------------------------------------------------------------------------


def test_pure_cfg_valid_defaults() -> None:
    cfg = _mk_config()
    assert cfg.market == "CN"
    assert cfg.gates == "full"
    assert cfg.position_pct == 0.05


def test_pure_cfg_valid_profit_trail_combo() -> None:
    cfg = _mk_config(profit_trail_trigger_pct=10.0, profit_trail_pct=-6.0)
    assert cfg.profit_trail_trigger_pct == 10.0


def test_pure_cfg_valid_exclude_boards_set() -> None:
    cfg = _mk_config(exclude_boards="300,688")
    assert cfg.board_exclude_set == frozenset({"300", "688"})


def test_pure_cfg_rejects_profit_trail_trigger_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(profit_trail_trigger_pct=-1.0)


def test_pure_cfg_rejects_profit_trail_pct_positive() -> None:
    with pytest.raises(ValueError):
        _mk_config(profit_trail_pct=5.0)


def test_pure_cfg_rejects_profit_trail_trigger_without_trail() -> None:
    with pytest.raises(ValueError):
        _mk_config(profit_trail_trigger_pct=10.0, profit_trail_pct=0.0)


def test_pure_cfg_rejects_industry_flow_exit_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(industry_flow_exit_days=-1)


def test_pure_cfg_rejects_position_pct_outside_unit_interval() -> None:
    with pytest.raises(ValueError):
        _mk_config(position_pct=0.0)
    with pytest.raises(ValueError):
        _mk_config(position_pct=1.5)


def test_pure_cfg_rejects_max_positions_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(max_positions=0)
    with pytest.raises(ValueError):
        _mk_config(max_positions=101)


def test_pure_cfg_rejects_diverging_scale_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(diverging_scale=-0.1)
    with pytest.raises(ValueError):
        _mk_config(diverging_scale=1.5)


def test_pure_cfg_rejects_drawdown_circuit_positive() -> None:
    with pytest.raises(ValueError):
        _mk_config(drawdown_circuit_pct=1.0)


def test_pure_cfg_rejects_swap_weak_rs_below_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(swap_weak_rs_below=1.5)


def test_pure_cfg_rejects_swap_strong_rs_at_least_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(swap_strong_rs_at_least=-0.1)


def test_pure_cfg_rejects_swap_min_hold_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(swap_min_hold_days=-1)


def test_pure_cfg_rejects_swap_max_per_day_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(swap_max_per_day=-1)


def test_pure_cfg_rejects_pyramid_trigger_pct_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(pyramid_trigger_pct=-1.0)
    with pytest.raises(ValueError):
        _mk_config(pyramid_trigger_pct=201.0)


def test_pure_cfg_rejects_pyramid_add_scale_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(pyramid_add_scale=2.5)


def test_pure_cfg_rejects_pyramid_max_adds_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(pyramid_max_adds=6)


def test_pure_cfg_rejects_atr_size_window_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(atr_size_window=121)


def test_pure_cfg_rejects_atr_size_cap_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(atr_size_cap=0.5)


def test_pure_cfg_rejects_atr_benchmark_pct_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(atr_benchmark_pct=0.1)


def test_pure_cfg_rejects_max_per_industry_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(max_per_industry=101)


def test_pure_cfg_rejects_entry_sort_unknown() -> None:
    with pytest.raises(ValueError):
        _mk_config(entry_sort="rank")


def test_pure_cfg_rejects_breakout_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(breakout_days=-1)


def test_pure_cfg_rejects_volume_breakout_mult_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(volume_breakout_mult=-1.0)


def test_pure_cfg_rejects_ma_slope_min_pct_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(ma_slope_min_pct=-1.0)


def test_pure_cfg_rejects_ma200_min_pct_below_off_sentinel() -> None:
    with pytest.raises(ValueError):
        _mk_config(ma200_min_pct=-2.0)


def test_pure_cfg_rejects_ma_cross_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(ma_cross_days=-1)


def test_pure_cfg_rejects_rsi_reversal_max_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(rsi_reversal_max=-1.0)


def test_pure_cfg_rejects_down_day_reversal_pct_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(down_day_reversal_pct=-1.0)


def test_pure_cfg_rejects_risk_adj_mom_ret_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(risk_adj_mom_ret_days=-1)


def test_pure_cfg_rejects_pead_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(pead_days=-1)


def test_pure_cfg_rejects_risk_adj_mom_vol_days_too_small() -> None:
    with pytest.raises(ValueError):
        _mk_config(risk_adj_mom_vol_days=3)


def test_pure_cfg_rejects_risk_adj_mom_min_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(risk_adj_mom_min=-1.0)


def test_pure_cfg_rejects_ind_mom_days_unsupported() -> None:
    with pytest.raises(ValueError):
        _mk_config(ind_mom_days=30)


def test_pure_cfg_rejects_ind_mom_top_pct_zero() -> None:
    with pytest.raises(ValueError):
        _mk_config(ind_mom_top_pct=0.0)


def test_pure_cfg_rejects_ind_neutral_days_unsupported() -> None:
    with pytest.raises(ValueError):
        _mk_config(ind_neutral_days=30)


def test_pure_cfg_rejects_ind_neutral_rank_pct_zero() -> None:
    with pytest.raises(ValueError):
        _mk_config(ind_neutral_rank_pct=0.0)


def test_pure_cfg_rejects_high_52w_min_pct_out_of_range() -> None:
    with pytest.raises(ValueError):
        _mk_config(high_52w_min_pct=101.0)


def test_pure_cfg_rejects_mom_ret_days_unsupported() -> None:
    with pytest.raises(ValueError):
        _mk_config(mom_ret_days=30)


def test_pure_cfg_rejects_mom_skip_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(mom_skip_days=-1)


def test_pure_cfg_rejects_mom_rank_min_zero() -> None:
    with pytest.raises(ValueError):
        _mk_config(mom_rank_min=0.0)


def test_pure_cfg_rejects_min_avg_amount_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(min_avg_amount=-1.0)


def test_pure_cfg_rejects_max_hold_unprofitable_days_negative() -> None:
    with pytest.raises(ValueError):
        _mk_config(max_hold_unprofitable_days=-1)


def test_pure_cfg_rejects_entry_mode_unknown() -> None:
    with pytest.raises(ValueError):
        _mk_config(entry_mode="open")


def test_pure_cfg_rejects_entry_style_unknown() -> None:
    with pytest.raises(ValueError):
        _mk_config(entry_style="value")


def test_pure_cfg_rejects_negative_market_cap_bounds() -> None:
    with pytest.raises(ValueError):
        _mk_config(min_mv=-1.0)


# ---------------------------------------------------------------------------
# _summarize
# ---------------------------------------------------------------------------


def test_pure_summarize_empty_trades() -> None:
    cfg = _mk_config()
    summary = _summarize(cfg, _mk_data(["2026-08-01"]), [], 0, nav_curve=[1.0])
    assert summary.trades == 0
    assert summary.closed == 0
    assert summary.win_rate is None
    assert summary.avg_net_pnl_pct is None
    assert summary.avg_gross_pnl_pct is None
    assert summary.avg_costs_pct is None
    assert summary.avg_win_pct is None
    assert summary.avg_loss_pct is None
    assert summary.sharpe is None
    assert summary.by_score_bucket == {}
    assert summary.gated_blocks == {}
    assert summary.calendar_days == 1
    assert summary.total_net_pnl_pct == 0.0
    assert summary.max_drawdown_pct == 0.0
    assert summary.annual_net_pnl_pct == 0.0
    assert summary.config["market"] == "CN"


def test_pure_summarize_score_buckets() -> None:
    cfg = _mk_config()
    trades = [
        _mk_trade(2.0, score=95.0),
        _mk_trade(-1.0, score=87.0),
        _mk_trade(3.0, score=82.0),
        _mk_trade(-0.5, score=75.0),
        _mk_trade(1.0, score=65.0),
        _mk_trade(-2.0, score=None),
    ]
    summary = _summarize(
        cfg,
        _mk_data(["2026-08-01", "2026-08-04"]),
        trades,
        0,
        nav_curve=[1.0, 1.02, 1.01, 1.03],
    )
    buckets = summary.by_score_bucket
    assert set(buckets) == {">=90", "85-90", "80-85", "70-80", "<70"}
    assert buckets[">=90"]["trades"] == 1
    assert buckets[">=90"]["winRate"] == 1.0
    assert buckets[">=90"]["avgNet"] == 2.0
    assert buckets["85-90"]["wins"] == 0
    assert buckets["85-90"]["avgNet"] == -1.0
    assert buckets["<70"]["trades"] == 2
    assert buckets["<70"]["wins"] == 1
    assert buckets["<70"]["winRate"] == 0.5
    assert buckets["<70"]["avgNet"] == -0.5
    assert summary.wins == 3
    assert summary.losses == 3
    assert summary.win_rate == 0.5
    assert summary.avg_net_pnl_pct == pytest.approx(0.417)
    assert summary.avg_win_pct == 2.0
    assert summary.avg_loss_pct == pytest.approx(-1.167)
    assert isinstance(summary.sharpe, float)


def test_pure_summarize_wins_only_avg_loss_none() -> None:
    cfg = _mk_config()
    summary = _summarize(cfg, _mk_data(["2026-08-01"]), [_mk_trade(2.0)], 0, nav_curve=[1.0, 1.1])
    assert summary.avg_win_pct == 2.0
    assert summary.avg_loss_pct is None
    assert summary.total_net_pnl_pct == 10.0


def test_pure_summarize_losses_only_avg_win_none() -> None:
    cfg = _mk_config()
    summary = _summarize(cfg, _mk_data(["2026-08-01"]), [_mk_trade(-2.0)], 0, nav_curve=[1.0, 0.9])
    assert summary.avg_win_pct is None
    assert summary.avg_loss_pct == -2.0


def test_pure_summarize_total_and_drawdown() -> None:
    cfg = _mk_config()
    summary = _summarize(
        cfg,
        _mk_data(["2026-08-01"] * 5),
        [_mk_trade(5.0)],
        0,
        nav_curve=[1.0, 1.1, 1.05, 1.2, 1.1],
    )
    assert summary.total_net_pnl_pct == 10.0
    assert summary.max_drawdown_pct == 10.0
    assert summary.annual_net_pnl_pct > 0.0
    assert isinstance(summary.sharpe, float)


def test_pure_summarize_flat_nav_sharpe_none() -> None:
    cfg = _mk_config()
    summary = _summarize(
        cfg, _mk_data(["2026-08-01"]), [_mk_trade(0.0)], 0, nav_curve=[1.0, 1.0, 1.0]
    )
    assert summary.sharpe is None
    assert summary.annual_net_pnl_pct == 0.0


def test_pure_summarize_zero_end_nav_cagr_zero() -> None:
    cfg = _mk_config()
    summary = _summarize(
        cfg, _mk_data(["2026-08-01"]), [_mk_trade(-100.0)], 0, nav_curve=[1.0, 0.0]
    )
    assert summary.annual_net_pnl_pct == 0.0
    assert summary.total_net_pnl_pct == -100.0
    assert summary.max_drawdown_pct == 100.0
    assert summary.sharpe is not None and summary.sharpe < 0.0


def test_pure_summarize_zero_mid_nav_skips_day() -> None:
    cfg = _mk_config()
    summary = _summarize(cfg, _mk_data(["2026-08-01"]), [_mk_trade(-50.0)], 0, nav_curve=[0.0, 0.5])
    assert summary.sharpe is None
    assert summary.annual_net_pnl_pct == pytest.approx(-100.0)
    assert summary.total_net_pnl_pct == -50.0


def test_pure_summarize_gated_blocks_passthrough() -> None:
    cfg = _mk_config()
    summary = _summarize(
        cfg,
        _mk_data(["2026-08-01"]),
        [],
        0,
        gated_blocks={"regime": 2},
        nav_curve=[],
    )
    assert summary.gated_blocks == {"regime": 2}
    assert summary.total_net_pnl_pct == 0.0


def test_pure_trade_and_summary_to_dict() -> None:
    trade = _mk_trade(2.0)
    trade_dict = trade.to_dict()
    assert trade_dict["symbol"] == "CN:600001"
    assert trade_dict["pnl_pct"] == 2.0
    summary = _summarize(
        cfg := _mk_config(), _mk_data(["2026-08-01"]), [trade], 0, nav_curve=[1.0, 1.02]
    )
    summary_dict = summary.to_dict()
    assert summary_dict["closed"] == 1
    assert summary_dict["config"]["market"] == cfg.market


# ---------------------------------------------------------------------------
# _window_years
# ---------------------------------------------------------------------------


def test_pure_window_years_normal_window() -> None:
    cfg = BacktestConfig(start_date="2026-06-18", end_date="2026-08-08")
    assert _window_years(cfg) == pytest.approx(51 / 365.25)


def test_pure_window_years_single_day_floors_at_one_day() -> None:
    cfg = BacktestConfig(start_date="2026-08-01", end_date="2026-08-01")
    assert _window_years(cfg) == pytest.approx(1 / 365.25)


def test_pure_window_years_unparsable_dates_fall_back_to_one() -> None:
    cfg = _mk_config()
    object.__setattr__(cfg, "start_date", "not-a-date")
    assert _window_years(cfg) == 1.0


# ---------------------------------------------------------------------------
# _sharpe_from_closes
# ---------------------------------------------------------------------------


def test_pure_sharpe_from_closes_empty_is_none() -> None:
    assert _sharpe_from_closes([], _mk_config()) is None


def test_pure_sharpe_from_closes_fewer_than_three_days_is_none() -> None:
    cfg = _mk_config()
    trades = [_mk_trade(1.0, close_date="2026-08-04"), _mk_trade(2.0, close_date="2026-08-05")]
    assert _sharpe_from_closes(trades, cfg) is None


def test_pure_sharpe_from_closes_zero_variance_is_none() -> None:
    cfg = _mk_config()
    trades = [_mk_trade(1.0, close_date=f"2026-08-0{d}") for d in (4, 5, 6)]
    assert _sharpe_from_closes(trades, cfg) is None


def test_pure_sharpe_from_closes_weighted_value() -> None:
    cfg = _mk_config()
    trades = [
        _mk_trade(1.0, close_date="2026-08-04", position_pct=0.05),
        _mk_trade(2.0, close_date="2026-08-05", position_pct=0.05),
        _mk_trade(-0.5, close_date="2026-08-06", position_pct=0.10),
    ]
    rets = [1.0 * 0.05, 2.0 * 0.05, -0.5 * 0.10]
    expected = round(statistics.mean(rets) / statistics.stdev(rets) * (252**0.5), 2)
    assert _sharpe_from_closes(trades, cfg) == expected


def test_pure_sharpe_from_closes_aggregates_same_day() -> None:
    cfg = _mk_config()
    trades = [
        _mk_trade(1.0, close_date="2026-08-04"),
        _mk_trade(1.0, close_date="2026-08-04"),
        _mk_trade(2.0, close_date="2026-08-05"),
        _mk_trade(-0.5, close_date="2026-08-06"),
    ]
    rets = [2.0 * 0.05, 2.0 * 0.05, -0.5 * 0.05]
    expected = round(statistics.mean(rets) / statistics.stdev(rets) * (252**0.5), 2)
    assert _sharpe_from_closes(trades, cfg) == expected


# ---------------------------------------------------------------------------
# with_benchmark_excess
# ---------------------------------------------------------------------------


def test_pure_benchmark_excess_empty_benchmarks() -> None:
    cfg = _mk_config()
    summary = _summarize(cfg, _mk_data(["2026-08-01"]), [], 0, nav_curve=[1.0, 1.1])
    out = with_benchmark_excess(summary, [])
    assert out is summary
    assert out.best_benchmark == ""
    assert out.excess_vs_best_benchmark_pct == round(float(summary.annual_net_pnl_pct), 2)


def test_pure_benchmark_excess_picks_best_annual() -> None:
    summary = _mk_summary(annual=8.0)
    out = with_benchmark_excess(
        summary,
        [
            {"name": "CSI300", "annual_pct": 5.0},
            {"name": "ChiNext", "annual_pct": None},
            {"name": "SSE50", "annual_pct": 3.0},
        ],
    )
    assert out.best_benchmark == "CSI300"
    assert out.excess_vs_best_benchmark_pct == 3.0


def test_pure_benchmark_excess_missing_names_and_annual() -> None:
    summary = _mk_summary(annual=None)
    out = with_benchmark_excess(summary, [{"annual_pct": 7.0}])
    assert out.best_benchmark == ""
    assert out.excess_vs_best_benchmark_pct == -7.0


# ---------------------------------------------------------------------------
# run_sensitivity (BacktestData / simulate swapped out — no DB)
# ---------------------------------------------------------------------------


def test_pure_run_sensitivity_empty_grid(monkeypatch) -> None:
    monkeypatch.setattr(be, "BacktestData", _mk_config)  # must never be constructed
    monkeypatch.setattr(be, "simulate", lambda *a, **k: None)
    assert run_sensitivity([]) == []


def test_pure_run_sensitivity_reuses_data_per_window(monkeypatch) -> None:
    built: list[BacktestConfig] = []
    calls: list[tuple[BacktestConfig, object]] = []

    class _FakeData:
        def __init__(self, config: BacktestConfig) -> None:
            built.append(config)

    def _fake_simulate(config: BacktestConfig, data=None):
        calls.append((config, data))
        return SimpleNamespace(summary=("sum", config.score_threshold, id(data)))

    monkeypatch.setattr(be, "BacktestData", _FakeData)
    monkeypatch.setattr(be, "simulate", _fake_simulate)

    a1 = BacktestConfig(start_date="2026-06-18", end_date="2026-08-08", score_threshold=80.0)
    a2 = BacktestConfig(start_date="2026-06-18", end_date="2026-08-08", score_threshold=90.0)
    b = BacktestConfig(start_date="2026-06-18", end_date="2026-09-08", score_threshold=80.0)
    c = BacktestConfig(
        start_date="2026-06-18", end_date="2026-08-08", market="HK", score_threshold=70.0
    )

    out = run_sensitivity([a1, a2, b, c])

    assert len(built) == 3  # one BacktestData per (window, market)
    assert len(calls) == 4
    assert calls[0][1] is calls[1][1]  # same-window configs share the dataset
    assert calls[0][1] is not calls[2][1]
    assert [s[1] for s in out] == [80.0, 90.0, 80.0, 70.0]  # config order preserved


# ---------------------------------------------------------------------------
# default_sensitivity_grid
# ---------------------------------------------------------------------------


def test_pure_default_sensitivity_grid_custom_gate_level() -> None:
    grid = default_sensitivity_grid("2026-06-18", "2026-08-08", gates_levels=("regime",))
    assert len(grid) == 36  # 4 score x 3 hold x 3 stop x 1 gate level
    assert {c.gates for c in grid} == {"regime"}
    first = grid[0]
    assert first.score_threshold == 70.0
    assert first.max_hold_days == 5
    assert first.stop_loss_pct == -3.0
    assert (first.start_date, first.end_date) == ("2026-06-18", "2026-08-08")


def test_pure_default_sensitivity_grid_default_shape() -> None:
    grid = default_sensitivity_grid("2026-06-18", "2026-08-08")
    assert len(grid) == 4 * 3 * 3 * 2
    assert {c.gates for c in grid} == {"none", "full"}


# ---------------------------------------------------------------------------
# _calendar_days_between
# ---------------------------------------------------------------------------


def test_pure_calendar_days_between_trading_calendar() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    assert _calendar_days_between("2026-06-18", "2026-06-22", calendar) == 2
    assert _calendar_days_between("2026-06-18", "2026-06-18", calendar) == 0
    assert _calendar_days_between("2026-06-22", "2026-06-18", calendar) == 0


def test_pure_calendar_days_between_weekend_entry_falls_back() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    # Saturday entry is not a trading day: plain calendar-day math applies.
    assert _calendar_days_between("2026-06-20", "2026-06-22", calendar) == 2
    assert _calendar_days_between("2026-06-19", "2026-06-21", calendar) == 2


def test_pure_calendar_days_between_garbage_with_calendar_is_zero() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    assert _calendar_days_between("xx", "2026-06-19", calendar) == 0
    assert _calendar_days_between("2026-06-18", "yy", calendar) == 0


def test_pure_calendar_days_between_reversed_without_calendar_is_zero() -> None:
    assert _calendar_days_between("2026-06-22", "2026-06-18") == 0
