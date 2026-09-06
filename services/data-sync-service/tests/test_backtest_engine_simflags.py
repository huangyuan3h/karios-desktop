"""Sim-flag branch coverage for the backtest engine (pure unit tests).

Covers ``simulate()`` config-flag fail-closed edges (entry-price modes, ATR
sizing/stops, ret5/strength helpers, swap candidate skips, score_rs order,
circuit breaker, risk_adj/ind/high52w/mom gates, CN auto style, breakout,
volume, ma_slope/ma200/ma_cross, rsi, down-day reversal, liquidity,
delisted, cash-cap) plus ``BacktestData`` init/recompute edges — all with
small synthetic calendars (5-10 days, 1-3 symbols).

No DB, no network; ``monkeypatch`` only. Asserts trade/no-trade outcomes,
not exact internals.
"""

from __future__ import annotations

import datetime as _dt

import pytest

import data_sync_service.service.backtest_engine as be
from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    BacktestData,
    BacktestSummary,
    BacktestTrade,
    simulate,
)

CN1 = "CN:600001"
TS1 = "600001.SH"
CN2 = "CN:600002"
TS2 = "600002.SH"
CN3 = "CN:000001"
TS3 = "000001.SZ"


def _data(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    prices: dict[str, dict[str, float]],
    *,
    regime: str = "Strong",
    flow_any_positive: bool = True,
    mainline_allow: set[str] | None = None,
    industry_by_ts: dict[str, str] | None = None,
) -> BacktestData:
    """Synthetic BacktestData with every attr ``simulate`` may touch preset."""
    data = BacktestData.__new__(BacktestData)
    data.config = None
    data.calendar = calendar
    data.scores_by_day = scores
    data.ts_codes = []
    data.bars_by_ts = {}
    data.close_by_ts_day = {ts: {d: float(px) for d, px in m.items()} for ts, m in prices.items()}
    data.regime_by_day = {d: regime for d in calendar}
    data.flow_any_positive_by_day = {d: flow_any_positive for d in calendar}
    data.mainline_allow_by_day = {d: set(mainline_allow or {"计算机"}) for d in calendar}
    data.flow5d_by_day = {}
    if industry_by_ts is None:
        industry_by_ts = {ts: "计算机" for ts in prices}
    data.industry_by_ts = dict(industry_by_ts)
    data.sentiment_risk_by_day = {}
    data.light_red_by_day = set()
    data.env_by_day = {d: "unknown" for d in calendar}
    data.closes_by_ts = {
        ts: [(d, float(px)) for d, px in sorted(m.items())] for ts, m in prices.items()
    }
    data.rs_rank_by_day = {}
    data.mv_by_day = {}
    data.avg_amount_by_day = {}
    data.ind_industry_rank_by_day = {}
    data.ind_within_rank_by_day = {}
    data.mom_rank_by_day = {}
    data.st_ts_codes = set()
    data.pead_events = {}
    data.delist_by_ts = {}
    return data


def _days(start: str, n: int) -> list[str]:
    d0 = _dt.date.fromisoformat(start)
    return [(d0 + _dt.timedelta(days=i)).isoformat() for i in range(n)]


def _flat(calendar: list[str], px: float = 10.0) -> dict[str, float]:
    return {d: px for d in calendar}


def _bars(
    dates: list[str],
    closes: dict[str, float] | None = None,
    *,
    vol: str = "100",
    high: float | None = None,
    low: float | None = None,
) -> list[tuple[str, str, str, str, str, str]]:
    out = []
    for d in dates:
        c = (closes or {}).get(d, 10.0)
        out.append((d, str(c), str(high if high is not None else c), str(low if low is not None else c), str(c), vol))
    return out


# ---------------------------------------------------------------------------
# A. BacktestData pure edges (to_dict, trendok validation, optional loaders)
# ---------------------------------------------------------------------------


def test_trade_and_summary_to_dict() -> None:
    trade = BacktestTrade(
        symbol=CN1, market="CN", entry_date="2026-06-18", entry_price=10.0,
        close_date="2026-06-19", close_price=11.0, gross_pnl_pct=10.0,
        costs_pct=0.3, pnl_pct=9.7, holding_days=1, close_reason="target_hit",
        score_at_entry=90.0,
    )
    assert trade.to_dict()["close_reason"] == "target_hit"
    summary = BacktestSummary(
        config={}, calendar_days=2, trades=1, closed=1, open_at_end=0,
        wins=1, losses=0, win_rate=1.0, avg_net_pnl_pct=9.7,
        avg_gross_pnl_pct=10.0, avg_costs_pct=0.3, max_drawdown_pct=0.0,
        total_net_pnl_pct=0.5, annual_net_pnl_pct=1.0, avg_win_pct=9.7,
        avg_loss_pct=None, sharpe=None, excess_vs_best_benchmark_pct=0.0,
        best_benchmark="",
    )
    assert summary.to_dict()["closed"] == 1


def test_init_rejects_unknown_trendok_params(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(be, "_load_calendar", lambda s, e: ["2026-06-18"])
    cfg = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-18",
        trendok_params={"no_such_param": 1.0},
    )
    with pytest.raises(ValueError, match="unknown trendok_params"):
        BacktestData(cfg)


def test_init_loads_all_optional_layers(monkeypatch: pytest.MonkeyPatch) -> None:
    """All gated loaders run; bad/negative bars are skipped, not crashed on."""
    monkeypatch.setattr(be, "_load_calendar", lambda s, e: ["2026-06-18", "2026-06-19"])
    monkeypatch.setattr(be, "_load_scores", lambda s, e, m: {"2026-06-18": {"CN:600001": 90.0}})
    monkeypatch.setattr(be, "_load_delist_dates", lambda ts: {})
    monkeypatch.setattr(
        be, "fetch_ohlcv_batch_between",
        lambda ts, s, e: {"600001.SH": [
            ("2026-06-18", "10", "10", "9", "10", "1000"),
            ("2026-06-19", "10", "10", "9", "BAD", "1000"),  # unparseable -> skip
            ("2026-06-19", "10", "10", "9", "-1", "1000"),  # non-positive -> skip
        ]},
    )
    monkeypatch.setattr(be, "_load_regime_by_day", lambda cfg, cal: dict.fromkeys(cal, "Strong"))
    monkeypatch.setattr(be, "_load_light_red_days", lambda cfg, cal: set())
    monkeypatch.setattr(
        be, "_load_flow_mainline_data",
        lambda cfg, cal: ({d: True for d in cal}, {d: {"计算机"} for d in cal}, {}),
    )
    monkeypatch.setattr(be, "_load_industries", lambda ts: {})
    monkeypatch.setattr(be, "_load_st_names", lambda: set())
    import data_sync_service.db.stock_forecast as sf

    monkeypatch.setattr(sf, "positive_forecast_dates", lambda s, e: {})
    monkeypatch.setattr(be, "_load_rs_ranks", lambda cfg, cal, ts: {})
    monkeypatch.setattr(be, "_load_sentiment_risk", lambda cfg: {})
    import data_sync_service.service.env_label as env_mod

    monkeypatch.setattr(env_mod, "load_env_by_day", lambda s, e: {})
    monkeypatch.setattr(be, "_load_market_caps", lambda cfg, ts: {})
    monkeypatch.setattr(be, "_load_avg_amount", lambda cfg, cal, ts: {})
    monkeypatch.setattr(be, "_load_industry_data", lambda cfg, cal, ts: ({}, {}))
    monkeypatch.setattr(be, "_load_mom_ranks", lambda cfg, cal, ts: {})
    cfg = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", light_red_block=True,
        exclude_st=True, pead_days=5, entry_style="auto", min_avg_amount=1.0,
        ind_mom_days=20, mom_ret_days=60,
    )
    data = BacktestData(cfg)
    assert data.calendar == ["2026-06-18", "2026-06-19"]
    assert data.close_by_ts_day["600001.SH"] == {"2026-06-18": 10.0}


# ---------------------------------------------------------------------------
# A2. recompute_scores_with_params edges (no DB: small pools, mocked trendok)
# ---------------------------------------------------------------------------


def _recompute_data(
    calendar: list[str], scores: dict[str, dict[str, float]], closes: dict[str, dict[str, float]]
) -> BacktestData:
    from types import SimpleNamespace

    data = _data(calendar, scores, closes)
    data.config = SimpleNamespace(market="CN")
    return data


def test_recompute_no_candidates_returns_original(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trendok._build_industry_flow_context", lambda d: {"ok": False}
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 40.0}}  # below both the 65 and 55 pools
    data = _recompute_data(calendar, scores, {TS1: _flat(calendar)})
    assert data.recompute_scores_with_params({}) == scores


def test_recompute_flow_context_failure_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(day: str) -> dict:
        raise RuntimeError("no db")

    monkeypatch.setattr(
        "data_sync_service.service.trendok._build_industry_flow_context", _boom
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 40.0}}
    data = _recompute_data(calendar, scores, {TS1: _flat(calendar)})
    assert data.recompute_scores_with_params({}) == scores


def test_recompute_flow_import_failure_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(
        "data_sync_service.service.trendok._build_industry_flow_context", raising=True
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 40.0}}
    data = _recompute_data(calendar, scores, {TS1: _flat(calendar)})
    assert data.recompute_scores_with_params({}) == scores


def test_recompute_short_bars_return_original(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trendok._build_industry_flow_context", lambda d: {"ok": False}
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}}
    data = _recompute_data(calendar, scores, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: [("2026-06-18", "10", "10", "10", "10", "100")]}
    assert data.recompute_scores_with_params({}) == scores


def _long_bars(day: str, n: int = 79) -> list[tuple[str, str, str, str, str, str]]:
    base = _dt.date.fromisoformat(day)
    out = []
    for i in range(n, 0, -1):
        d = (base - _dt.timedelta(days=i)).isoformat()
        out.append((d, "10", "10", "10", "10", "100"))
    return out


def test_recompute_uses_trendok_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trendok._build_industry_flow_context", lambda d: {"ok": False}
    )
    monkeypatch.setattr(
        "data_sync_service.service.trendok._trendok_one", lambda **kw: {"score": 90.0}
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, CN2: 80.0, "HK:00700": 90.0}}
    closes = {TS1: _flat(calendar), TS2: _flat(calendar)}
    data = _recompute_data(calendar, scores, closes)
    data.bars_by_ts = {
        TS1: _long_bars("2026-06-18") + _bars(["2026-06-18"]),
        TS2: _long_bars("2026-06-18") + _bars(["2026-06-18"]),
    }
    out = data.recompute_scores_with_params({})
    assert out["2026-06-18"][CN1] == 90.0
    assert out["2026-06-18"][CN2] == 90.0
    assert "HK:00700" not in out["2026-06-18"]  # wrong-market symbol skipped


def test_recompute_non_numeric_score_returns_original(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trendok._build_industry_flow_context", lambda d: {"ok": False}
    )
    monkeypatch.setattr(
        "data_sync_service.service.trendok._trendok_one", lambda **kw: {"score": None}
    )
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}}
    data = _recompute_data(calendar, scores, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: _long_bars("2026-06-18") + _bars(["2026-06-18"])}
    assert data.recompute_scores_with_params({}) == scores


# ---------------------------------------------------------------------------
# B1. simulate() with data=None loads via BacktestData
# ---------------------------------------------------------------------------


def test_simulate_loads_data_when_none(monkeypatch: pytest.MonkeyPatch) -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    fake = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    monkeypatch.setattr(be, "BacktestData", lambda cfg: fake)
    run = simulate(BacktestConfig(start_date="2026-06-18", end_date="2026-06-19"), data=None)
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B2. drawdown circuit breaker
# ---------------------------------------------------------------------------


def test_drawdown_circuit_halts_entries_after_losses() -> None:
    calendar = [
        "2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23",
        "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-29",
    ]
    px = [10.0, 9.7, 9.4, 9.1, 8.8, 8.5, 8.2, 8.2]
    data = _data(
        calendar,
        {d: {CN1: 90.0} for d in calendar},
        {TS1: {d: px[i] for i, d in enumerate(calendar)}},
    )
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            stop_loss_pct=-1.0, target_pnl_pct=100.0, max_hold_days=60,
            drawdown_circuit_pct=-5.0, drawdown_circuit_window_days=30,
        ),
        data=data,
    )
    assert run.summary.gated_blocks.get("circuit", 0) >= 1


# ---------------------------------------------------------------------------
# B3. entry_price_for modes and fallbacks
# ---------------------------------------------------------------------------


def test_entry_next_open_without_bars_skips() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_mode="next_open"
        ),
        data=data,
    )
    assert run.summary.closed == 0


def test_entry_next_open_without_next_session_skips() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: [("2026-06-18", "10", "10", "10", "10", "100")]}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_mode="next_open"
        ),
        data=data,
    )
    assert run.summary.closed == 0


def test_entry_next_open_with_bad_open_skips() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {
        TS1: [
            ("2026--06-18", "10", "10", "10", "10", "100"),
            ("2026-06-18", "10", "10", "10", "10", "100"),
            ("2026-06-19", "BAD", "10", "10", "10", "100"),
        ]
    }
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_mode="next_open"
        ),
        data=data,
    )
    assert run.summary.closed == 0


def test_entry_last_hour_without_bar_uses_close() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_mode="last_hour_low",
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].entry_price == 10.0


def test_entry_last_hour_with_bad_bar_uses_close() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: [("2026-06-18", "10", "BAD", "BAD", "10", "100")]}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_mode="last_hour_low",
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].entry_price == 10.0


def test_entry_last_hour_with_invalid_range_uses_close() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: [("2026-06-18", "10", "10", "0", "10", "100")]}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_mode="last_hour_low",
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].entry_price == 10.0


def test_entry_last_hour_hl_midpoint() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: [("2026-06-18", "9", "11", "8", "10", "100")]}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_mode="last_hour_hl",
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].entry_price == pytest.approx(9.5)


# ---------------------------------------------------------------------------
# B4. ATR position sizing fallbacks (scale 1.0, base sleeve kept)
# ---------------------------------------------------------------------------


def test_atr_size_without_bars_keeps_base_sleeve() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", atr_size_window=20
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].position_pct == pytest.approx(0.05)


def test_atr_size_with_short_history_keeps_base_sleeve() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.bars_by_ts = {TS1: _bars(calendar)}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", atr_size_window=20
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].position_pct == pytest.approx(0.05)


def test_atr_size_with_zero_volatility_keeps_base_sleeve() -> None:
    hist = _days("2026-05-20", 21)
    calendar = hist[-3:]
    data = _data(
        calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}}
    )
    data.bars_by_ts = {TS1: _bars(hist)}  # high == low everywhere -> ATR 0
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", atr_size_window=20
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].position_pct == pytest.approx(0.05)


# ---------------------------------------------------------------------------
# B5. ATR14 stop helper fallbacks (fixed line applies)
# ---------------------------------------------------------------------------


def _atr_stop_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none",
        stop_loss_pct=-5.0, target_pnl_pct=100.0, max_hold_days=60,
        trailing_stop_pct=0.0, atr_stop_mult=2.0,
    )


def test_atr14_without_bars_uses_fixed_stop() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(
        calendar, {"2026-06-18": {CN1: 90.0}},
        {TS1: {"2026-06-18": 10.0, "2026-06-19": 9.3}},
    )
    run = simulate(_atr_stop_config(calendar), data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"


def test_atr14_with_short_bars_uses_fixed_stop() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(
        calendar, {"2026-06-18": {CN1: 90.0}},
        {TS1: {"2026-06-18": 10.0, "2026-06-19": 9.3}},
    )
    data.bars_by_ts = {TS1: _bars(calendar, high=10.2, low=9.8)}
    run = simulate(_atr_stop_config(calendar), data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"


def test_atr14_skips_bad_bars() -> None:
    hist = _days("2026-06-08", 12)
    calendar = hist[-2:]
    closes = {d: 10.0 for d in hist}
    closes[calendar[1]] = 9.1
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    bars = _bars(hist, high=10.2, low=9.8)
    bars[3] = (hist[3], "10", "BAD", "BAD", "10", "100")  # skipped, rest still size ATR
    data.bars_by_ts = {TS1: bars}
    run = simulate(_atr_stop_config(calendar), data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"


def test_atr14_all_bad_bars_uses_fixed_stop() -> None:
    hist = _days("2026-06-08", 12)
    calendar = hist[-2:]
    closes = {d: 10.0 for d in hist}
    closes[calendar[1]] = 9.3
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    data.bars_by_ts = {TS1: [(d, "10", "BAD", "BAD", "10", "100") for d in hist]}
    run = simulate(_atr_stop_config(calendar), data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"


# ---------------------------------------------------------------------------
# B6. ret5 helper edges (style_ret fail-closed)
# ---------------------------------------------------------------------------


def test_ret5_without_closes_blocks_style() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.closes_by_ts = {}
    data.rs_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_style="momentum",
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("style_ret", 0) >= 1


def test_ret5_with_zero_base_blocks_style() -> None:
    hist = _days("2026-06-10", 8)
    calendar = hist[-3:]
    closes = {d: 10.0 for d in hist}
    closes[hist[0]] = 0.0  # 5 sessions before entry -> no return computable
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    data.rs_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            entry_style="momentum",
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("style_ret", 0) >= 1


# ---------------------------------------------------------------------------
# B7. strength helper (cache hit + exception fallback)
# ---------------------------------------------------------------------------


def test_strength_cached_across_positions(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.market_regime as mr

    monkeypatch.setattr(mr, "regime_strength_score", lambda **kw: {"strength": 70.0})
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, CN2: 90.0}}
    prices = {TS1: _flat(calendar), TS2: _flat(calendar)}
    data = _data(calendar, scores, prices)
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            atr_stop_strength_min=60.0,
        ),
        data=data,
    )
    assert run.summary.closed == 2  # both legs held through the window


def test_strength_exception_falls_back_to_fixed(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.market_regime as mr

    def _boom(**kw: object) -> dict:
        raise RuntimeError("no market data")

    monkeypatch.setattr(mr, "regime_strength_score", _boom)
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            stop_loss_pct=-15.0, atr_stop_mult=2.0, atr_stop_strength_min=60.0,
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "end_of_window"


# ---------------------------------------------------------------------------
# B8. swap candidate skips
# ---------------------------------------------------------------------------


def _swap_base() -> tuple[list[str], dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 88.0}, "2026-06-19": {CN1: 88.0, CN2: 90.0}}
    prices = {TS1: _flat(calendar), TS2: _flat(calendar)}
    return calendar, scores, prices


def _swap_config(**kw: object) -> BacktestConfig:
    base = {
        "start_date": "2026-06-18", "end_date": "2026-06-22", "score_threshold": 65.0,
        "gates": "none", "swap_weak_rs_below": 0.3, "swap_strong_rs_at_least": 0.8,
        "swap_min_hold_days": 1, "swap_max_per_day": 2,
    }
    base.update(kw)
    return BacktestConfig(**base)  # type: ignore[arg-type]


def _swap_rs(calendar: list[str]) -> dict[str, dict[str, float]]:
    return {d: {TS1: 0.1, TS2: 0.9} for d in calendar}


def test_swap_ignores_below_threshold_and_held_candidates() -> None:
    calendar, scores, prices = _swap_base()
    scores["2026-06-19"][CN3] = 10.0  # below threshold -> skipped as swap cand
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = _swap_rs(calendar)
    run = simulate(_swap_config(), data=data)
    swapped = [t for t in run.trades if t.close_reason == "swapped"]
    assert len(swapped) == 1
    assert swapped[0].symbol == CN1


def test_swap_skipped_during_panic_cooldown() -> None:
    calendar, scores, prices = _swap_base()
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = _swap_rs(calendar)
    data.sentiment_risk_by_day = {"2026-06-19": "extreme_caution"}
    run = simulate(_swap_config(panic_cooldown_days=5), data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]
    assert run.summary.gated_blocks.get("panic_cooldown", 0) >= 1


def test_swap_skips_unresolvable_and_wrong_market() -> None:
    calendar, scores, prices = _swap_base()
    scores["2026-06-19"]["GARBAGE"] = 95.0
    scores["2026-06-19"]["HK:00700"] = 95.0
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = _swap_rs(calendar)
    run = simulate(_swap_config(), data=data)
    assert len([t for t in run.trades if t.close_reason == "swapped"]) == 1
    assert not [t for t in run.trades if t.symbol in ("GARBAGE", "HK:00700")]


def test_swap_skipped_when_gate_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 88.0}, "2026-06-19": {CN2: 90.0}}
    prices = {TS1: _flat(calendar), TS2: _flat(calendar)}
    data = _data(calendar, scores, prices, regime="Strong")
    data.regime_by_day = {"2026-06-18": "Strong", "2026-06-19": "Weak", "2026-06-22": "Weak"}
    data.rs_rank_by_day = _swap_rs(calendar)
    run = simulate(_swap_config(gates="full"), data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]


def test_swap_skipped_when_candidate_has_no_price() -> None:
    calendar, scores, prices = _swap_base()
    del prices[TS2]["2026-06-19"]  # candidate unpriceable on swap day
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = _swap_rs(calendar)
    run = simulate(_swap_config(), data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]


def test_swap_skipped_when_held_leg_has_no_close() -> None:
    calendar, scores, prices = _swap_base()
    del prices[TS1]["2026-06-19"]  # held leg unpriceable on swap day
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = _swap_rs(calendar)
    run = simulate(_swap_config(), data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]


# ---------------------------------------------------------------------------
# B9. entry_sort score_rs blend
# ---------------------------------------------------------------------------


def test_entry_sort_score_rs_blends_score_and_rs() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, CN2: 88.0}}
    prices = {TS1: _flat(calendar), TS2: _flat(calendar)}
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = {"2026-06-18": {TS1: 0.1, TS2: 0.9}, "2026-06-19": {}}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, max_positions=1, entry_sort="score_rs",
        ),
        data=data,
    )
    # 90 x 0.6 = 54 < 88 x 1.4 = 123.2 -> CN2 wins the single sleeve
    assert run.summary.closed == 1
    assert run.trades[0].symbol == CN2


# ---------------------------------------------------------------------------
# B10. risk-adjusted momentum edges
# ---------------------------------------------------------------------------


def _risk_config(calendar: list[str], **kw: object) -> BacktestConfig:
    base: dict[str, object] = {
        "start_date": calendar[0], "end_date": calendar[-1], "gates": "none",
        "score_threshold": 65.0, "risk_adj_mom_ret_days": 60,
        "risk_adj_mom_vol_days": 30, "risk_adj_mom_min": 1.0,
    }
    base.update(kw)
    return BacktestConfig(**base)  # type: ignore[arg-type]


def test_risk_adj_mom_short_history_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_risk_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("risk_adj_mom", 0) == 1


def test_risk_adj_mom_early_index_blocks() -> None:
    hist = _days("2026-01-05", 100)
    calendar = hist[5:7]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_risk_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("risk_adj_mom", 0) == 1


def test_risk_adj_mom_gappy_vol_window_blocks() -> None:
    hist = _days("2026-01-05", 20)
    calendar = hist[-3:]
    closes = {d: 10.0 for d in hist}
    for d in hist[11:16]:
        closes[d] = 0.0  # zero bases -> daily returns uncomputable in vol window
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    run = simulate(
        _risk_config(calendar, risk_adj_mom_ret_days=10, risk_adj_mom_vol_days=5), data=data
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("risk_adj_mom", 0) == 1


def test_risk_adj_mom_zero_vol_blocks() -> None:
    hist = _days("2026-01-05", 20)
    calendar = hist[-3:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(
        _risk_config(calendar, risk_adj_mom_ret_days=10, risk_adj_mom_vol_days=5), data=data
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("risk_adj_mom", 0) == 1


# ---------------------------------------------------------------------------
# B11. industry momentum / within-industry gates
# ---------------------------------------------------------------------------


def test_ind_mom_missing_rank_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, ind_mom_days=20,
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ind_mom", 0) == 1


def test_ind_mom_top_rank_passes() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.ind_industry_rank_by_day = {d: {"计算机": 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, ind_mom_days=20,
        ),
        data=data,
    )
    assert run.summary.closed == 1


def test_ind_neutral_missing_rank_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, ind_neutral_days=20,
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ind_neutral", 0) == 1


def test_ind_neutral_top_rank_passes() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.ind_within_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, ind_neutral_days=20,
        ),
        data=data,
    )
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B12. 52-week-high proximity gate
# ---------------------------------------------------------------------------


def _high52w_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none",
        score_threshold=65.0, high_52w_min_pct=80.0,
    )


def test_high52w_missing_closes_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.closes_by_ts = {}
    run = simulate(_high52w_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("high52w", 0) == 1


def test_high52w_short_history_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_high52w_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("high52w", 0) == 1


def test_high52w_far_from_high_blocks() -> None:
    hist = _days("2025-06-01", 260)
    calendar = hist[-3:]
    closes = {d: (20.0 if d < hist[-10] else 10.0) for d in hist}
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    run = simulate(_high52w_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("high52w", 0) == 1


def test_high52w_near_high_passes() -> None:
    hist = _days("2025-06-01", 260)
    calendar = hist[-3:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_high52w_config(calendar), data=data)
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B13. mid-horizon momentum gate
# ---------------------------------------------------------------------------


def test_mom_missing_rank_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, mom_ret_days=60,
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("mom", 0) == 1


def test_mom_top_rank_passes() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.mom_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none",
            score_threshold=65.0, mom_ret_days=60,
        ),
        data=data,
    )
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B14. CN auto style mapping + dip/blocked styles
# ---------------------------------------------------------------------------


def test_cn_auto_uptrend_maps_to_momentum() -> None:
    hist = _days("2026-06-10", 8)
    calendar = hist[-3:]
    closes = {d: (10.0 if d < calendar[0] else 10.5) for d in hist}
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    data.env_by_day = {d: "uptrend" for d in calendar}
    data.rs_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_style="auto"
        ),
        data=data,
    )
    assert run.summary.closed == 1


def test_dip_style_without_pullback_blocks() -> None:
    hist = _days("2026-06-10", 8)
    calendar = hist[-3:]
    closes = {d: (10.0 if d < calendar[0] else 10.1) for d in hist}
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    data.rs_rank_by_day = {d: {TS1: 0.9} for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_style="dip"
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("style_dip", 0) == 1


def test_auto_style_blocked_in_weak_env() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.env_by_day = {d: "weak" for d in calendar}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", entry_style="auto"
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("style_blocked", 0) == 1


# ---------------------------------------------------------------------------
# B15. breakout gate fail-closed edges
# ---------------------------------------------------------------------------


def test_breakout_missing_closes_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.closes_by_ts = {}
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", breakout_days=3
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("breakout", 0) == 1


def test_breakout_day_missing_from_series_blocks() -> None:
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18"]
    data = _data(
        calendar, {"2026-06-15": {CN1: 90.0}},
        {TS1: {d: 10.0 for d in calendar}},
    )
    data.closes_by_ts[TS1] = [(d, c) for d, c in data.closes_by_ts[TS1] if d != "2026-06-15"]
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", breakout_days=3
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("breakout", 0) == 1


# ---------------------------------------------------------------------------
# B16. volume breakout fail-closed edges
# ---------------------------------------------------------------------------


def _volume_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none",
        volume_breakout_mult=2.0,
    )


def test_volume_without_bars_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


def test_volume_with_unparseable_priors_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[-2:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    bars = [(d, "10", "10", "10", "10", "BAD") for d in hist[:-1]]
    bars.append((hist[-1], "10", "10", "10", "10", "300"))
    data.bars_by_ts = {TS1: bars}
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


def test_volume_without_today_bar_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[-2:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "100") for d in hist if d != calendar[0]]}
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


def test_volume_with_unparseable_today_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[-2:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    bars = [(d, "10", "10", "10", "10", "100") for d in hist]
    bars[hist.index(calendar[0])] = (calendar[0], "10", "10", "10", "10", "BAD")
    data.bars_by_ts = {TS1: bars}
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


def test_volume_without_spike_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[-2:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "100") for d in hist]}
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


def test_volume_with_zero_average_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[-2:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    bars = [(d, "10", "10", "10", "10", "0") for d in hist]
    bars[hist.index(calendar[0])] = (calendar[0], "10", "10", "10", "10", "100")
    data.bars_by_ts = {TS1: bars}
    run = simulate(_volume_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("volume", 0) == 1


# ---------------------------------------------------------------------------
# B17. MA slope fail-closed edges
# ---------------------------------------------------------------------------


def _slope_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none", ma_slope_min_pct=2.0
    )


def test_ma_slope_short_history_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_slope_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma_slope", 0) == 1


def test_ma_slope_zero_base_ma_blocks() -> None:
    hist = _days("2026-04-20", 50)
    calendar = hist[-3:]
    closes = {d: (0.0 if d < hist[28] else 10.0) for d in hist}
    closes.update({d: 10.0 for d in calendar})
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: closes})
    run = simulate(_slope_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma_slope", 0) == 1


def test_ma_slope_flat_ma_blocks() -> None:
    hist = _days("2026-04-20", 50)
    calendar = hist[-3:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_slope_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma_slope", 0) == 1


# ---------------------------------------------------------------------------
# B18. MA200 fail-closed edges
# ---------------------------------------------------------------------------


def _ma200_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none", ma200_min_pct=0.0
    )


def test_ma200_short_history_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_ma200_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma200", 0) == 1


def test_ma200_early_index_blocks() -> None:
    hist = _days("2025-06-01", 210)
    calendar = hist[5:7]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_ma200_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma200", 0) == 1


def test_ma200_zero_average_blocks() -> None:
    hist = _days("2025-06-01", 210)
    calendar = hist[-3:]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 0.0 for d in hist}})
    data.close_by_ts_day[TS1] = {d: 10.0 for d in calendar}  # entry price still valid
    run = simulate(_ma200_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma200", 0) == 1


# ---------------------------------------------------------------------------
# B19. MA cross fail-closed edges
# ---------------------------------------------------------------------------


def _cross_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none", ma_cross_days=5
    )


def test_ma_cross_short_history_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_cross_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma_cross", 0) == 1


def test_ma_cross_early_index_blocks() -> None:
    hist = _days("2026-04-20", 45)
    calendar = hist[5:7]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_cross_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("ma_cross", 0) == 1


# ---------------------------------------------------------------------------
# B20. RSI reversal fail-closed edges
# ---------------------------------------------------------------------------


def _rsi_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none", rsi_reversal_max=30.0
    )


def test_rsi_missing_closes_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.closes_by_ts = {}
    run = simulate(_rsi_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("rsi_reversal", 0) == 1


def test_rsi_early_index_blocks() -> None:
    hist = _days("2026-05-20", 25)
    calendar = hist[5:7]
    data = _data(calendar, {calendar[0]: {CN1: 90.0}}, {TS1: {d: 10.0 for d in hist}})
    run = simulate(_rsi_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("rsi_reversal", 0) == 1


# ---------------------------------------------------------------------------
# B21. down-day reversal fail-closed edges
# ---------------------------------------------------------------------------


def _down_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none",
        down_day_reversal_pct=5.0,
    )


def test_down_day_missing_closes_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.closes_by_ts = {}
    run = simulate(_down_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("down_day_reversal", 0) == 1


def test_down_day_first_day_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_down_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("down_day_reversal", 0) == 1


# ---------------------------------------------------------------------------
# B22. liquidity floor
# ---------------------------------------------------------------------------


def _liq_config(calendar: list[str]) -> BacktestConfig:
    return BacktestConfig(
        start_date=calendar[0], end_date=calendar[-1], gates="none",
        score_threshold=65.0, min_avg_amount=5.0,
    )


def test_liquidity_missing_data_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    run = simulate(_liq_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("liquidity", 0) == 1


def test_liquidity_below_floor_blocks() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.avg_amount_by_day = {d: {TS1: 1.0} for d in calendar}
    run = simulate(_liq_config(calendar), data=data)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("liquidity", 0) == 1


def test_liquidity_above_floor_passes() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.avg_amount_by_day = {d: {TS1: 10.0} for d in calendar}
    run = simulate(_liq_config(calendar), data=data)
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B23. delisted entry block + B24. cash cap
# ---------------------------------------------------------------------------


def test_delisted_symbol_blocked_at_entry() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: _flat(calendar)})
    data.delist_by_ts = {TS1: "2026-06-18"}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="none"),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("delisted", 0) == 1


def test_cash_cap_blocks_second_sleeve() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, CN2: 90.0}}
    prices = {TS1: _flat(calendar), TS2: _flat(calendar)}
    data = _data(calendar, scores, prices)
    run = simulate(
        BacktestConfig(
            start_date=calendar[0], end_date=calendar[-1], gates="none", position_pct=0.6
        ),
        data=data,
    )
    assert run.summary.gated_blocks.get("cash_cap", 0) == 1
    assert run.summary.closed == 1


# ---------------------------------------------------------------------------
# B25. delisted force-close (with / without a last close)
# ---------------------------------------------------------------------------


def test_delisted_force_close_uses_last_close() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(
        calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: {"2026-06-18": 10.0}}
    )
    data.delist_by_ts = {TS1: "2026-06-19"}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="none"),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "delisted"
    assert run.trades[0].close_price == 10.0


def test_delisted_without_last_close_holds_to_window_end() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    data = _data(
        calendar, {"2026-06-18": {CN1: 90.0}}, {TS1: {"2026-06-18": 10.0}}
    )
    data.closes_by_ts = {TS1: []}  # no last close available
    data.delist_by_ts = {TS1: "2026-06-19"}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="none"),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "end_of_window"
