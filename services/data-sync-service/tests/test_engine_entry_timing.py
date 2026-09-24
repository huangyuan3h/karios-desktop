"""OPT-211 P1: next_open fills must not be exit-evaluated on the signal day.

A position filled at NEXT session's open has no market exposure on the
signal day. Evaluating stops / peak / pyramid / MTM against the signal-day
close reads the future fill price (ghost same-day stop-outs on overnight
gaps >= 5%). Pure unit tests: fully injected data, no DB, no network.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from data_sync_service.service.backtest_engine import BacktestConfig, simulate

TS = "600000.SH"
SYM = "CN:600000"


def _data(
    bars: list[tuple[str, str, str, str, str, str]],
    scores: dict[str, dict[str, float]],
    regime: str = "Strong",
    px1430: dict[str, dict[str, float]] | None = None,
) -> SimpleNamespace:
    cal = [b[0] for b in bars]
    closes = {b[0]: float(b[4]) for b in bars}
    return SimpleNamespace(
        calendar=cal,
        scores_by_day=scores,
        close_by_ts_day={TS: dict(closes)},
        closes_by_ts={TS: [(b[0], float(b[4])) for b in bars]},
        bars_by_ts={TS: list(bars)},
        px1430_by_ts=px1430 or {},
        regime_by_day={d: regime for d in cal},
        national_team_by_day={},
        guide_down_by_day={},
        light_red_by_day=set(),
        flow_any_positive_by_day={},
        mainline_allow_by_day={},
        flow5d_by_day={},
        industry_by_ts={},
        st_ts_codes=set(),
        pead_events={},
        rs_rank_by_day={},
        sentiment_risk_by_day={},
        env_by_day={},
        mv_by_day={},
        avg_amount_by_day={},
        ind_industry_rank_by_day={},
        ind_within_rank_by_day={},
        mom_rank_by_day={},
        value_comp_by_day={},
    )


def _cfg(**over: object) -> BacktestConfig:
    base: dict[str, object] = {
        "start_date": "2024-01-02",
        "end_date": "2024-01-05",
        "score_threshold": 65.0,
        "market": "CN",
        "gates": "none",
        "entry_mode": "next_open",
        "stop_loss_pct": -5.0,
        "trailing_stop_pct": 0.0,
        "target_pnl_pct": 100.0,
        "score_floor": 0.0,
        "position_pct": 0.1,
        "max_positions": 10,
        "max_hold_days": 60,
    }
    base.update(over)
    return BacktestConfig(**base)  # type: ignore[arg-type]


def _gap_up_bars() -> list[tuple[str, str, str, str, str, str]]:
    # D1 signal close 100; D2 opens 106 (+6% overnight gap) then closes 100.
    return [
        ("2024-01-02", "99", "101", "99", "100", "1000"),
        ("2024-01-03", "106", "106", "99", "100", "1000"),
        ("2024-01-04", "100", "101", "99", "100", "1000"),
        ("2024-01-05", "100", "101", "99", "100", "1000"),
    ]


def test_next_open_gap_up_has_no_same_day_exit() -> None:
    """T+1 (2026-09-17 audit): the fill day cannot sell what it just bought.

    Signal D1, fill at D2 open 106 (+6% gap), D2 closes 100. The old engine
    stopped out at the D2 close (same session as the buy — impossible in
    A-shares). The earliest legal exit is D3; its close is also 100, so the
    stop fires there.
    """
    data = _data(_gap_up_bars(), {"2024-01-02": {SYM: 70.0}})
    run = simulate(_cfg(), data)
    assert len(run.trades) == 1
    t = run.trades[0]
    assert t.entry_date == "2024-01-02" and t.entry_price == pytest.approx(106.0)
    assert t.close_date == "2024-01-04" and t.close_reason == "stop_hit"
    assert t.holding_days == 2
    snap = run.positions_by_day[0]
    assert snap["date"] == "2024-01-02"
    assert snap["positions"][0]["entry_price"] == pytest.approx(106.0)


def test_next_open_limit_gate_uses_the_signal_day_close() -> None:
    """A one-word board on the fill day must not fill after a down signal day.

    Reference limit for D+1 = D close x 1.10 = 9.50 x 1.10 = 10.45; the old
    check used the close BEFORE D (10.00) -> limit 11.00 and bought the
    unfillable board at 10.45. Also the positive control: a +3% gap that is
    NOT pinned relative to the signal-day close still fills.
    """
    bars = [
        ("2024-01-02", "10.00", "10.10", "9.90", "10.00", "1000"),
        ("2024-01-03", "9.60", "9.80", "9.40", "9.50", "1000"),  # signal day down
        ("2024-01-04", "10.45", "10.45", "10.45", "10.45", "1000"),  # one-word board
    ]
    data = _data(bars, {"2024-01-03": {SYM: 70.0}})
    run = simulate(_cfg(start_date="2024-01-03", end_date="2024-01-04"), data)
    assert run.trades == []  # board pinned at 9.50 x 1.10 -> no fill

    up = [
        ("2024-01-02", "9.90", "10.00", "9.80", "9.90", "1000"),
        ("2024-01-03", "10.10", "10.30", "10.00", "10.20", "1000"),  # signal day up
        ("2024-01-04", "10.50", "10.60", "10.40", "10.55", "1000"),
    ]
    data2 = _data(up, {"2024-01-03": {SYM: 70.0}})
    run2 = simulate(_cfg(start_date="2024-01-03", end_date="2024-01-04"), data2)
    assert len(run2.trades) == 1  # 10.50 < 10.20 x 1.10 -> fillable
    assert run2.trades[0].entry_price == pytest.approx(10.50)


def test_next_open_stop_fires_on_a_later_session() -> None:
    bars = [
        ("2024-01-02", "99", "101", "99", "100", "1000"),
        ("2024-01-03", "106", "106", "103", "104", "1000"),
        ("2024-01-04", "104", "104", "99", "100", "1000"),
        ("2024-01-05", "100", "101", "99", "100", "1000"),
    ]
    data = _data(bars, {"2024-01-02": {SYM: 70.0}})
    run = simulate(_cfg(), data)
    assert len(run.trades) == 1
    t = run.trades[0]
    assert t.entry_date == "2024-01-02" and t.entry_price == pytest.approx(106.0)
    assert t.close_date == "2024-01-04" and t.close_reason == "stop_hit"
    assert t.pnl_pct == pytest.approx(100.0 / 106.0 * 100 - 100 - 0.32282, abs=0.01)


def test_next_open_signal_day_nav_has_no_mark_noise() -> None:
    data = _data(_gap_up_bars(), {"2024-01-02": {SYM: 70.0}})
    run = simulate(_cfg(), data)
    # Marked at cost: committed capital (0.1 sleeve + entry-side cost) at
    # ratio 1.0, zero open/close-gap noise (the close-100 vs open-106 gap
    # would otherwise print a fake -0.57pt dip). Entry cost = explicit 3.641bp
    # + 10bp slippage floor at the 106 open = 1.3641bp.
    assert run.nav_curve[0] == pytest.approx(1.0 - 0.1 * 1.0013641 + 0.1 * 1.0)


def test_close_mode_same_day_evaluation_unchanged() -> None:
    flat = [
        ("2024-01-02", "99", "101", "99", "100", "1000"),
        ("2024-01-03", "99", "101", "99", "100", "1000"),
    ]
    data = _data(flat, {"2024-01-02": {SYM: 70.0}})
    run = simulate(_cfg(entry_mode="close", end_date="2024-01-03"), data)
    # Close fills evaluate the same close (no future price involved): the only
    # exit here is the window-end liquidation, and MTM still marks day one.
    assert len(run.trades) == 1
    assert run.trades[0].close_reason == "end_of_window"
    assert run.nav_curve[0] == pytest.approx(1.0 - 0.1 * 1.0013641 + 0.1)


# ---------------------------------------------------------------------------
# H-CLOCK-1430 (2026-09-18): entry_mode="next_1430" — same signal/lag as
# next_open, fill price = the fill session's 14:30 print (already qfq).
# ---------------------------------------------------------------------------


def _print_bars() -> list[tuple[str, str, str, str, str, str]]:
    # D1 signal close 100; D2 opens 106 but the 14:30 print is 104.5.
    return [
        ("2024-01-02", "99", "101", "99", "100", "1000"),
        ("2024-01-03", "106", "106", "104", "105", "1000"),
        ("2024-01-04", "105", "106", "104", "105", "1000"),
        ("2024-01-05", "105", "106", "104", "105", "1000"),
    ]


def test_next_1430_fills_at_the_fill_session_print() -> None:
    data = _data(
        _print_bars(),
        {"2024-01-02": {SYM: 70.0}},
        px1430={TS: {"2024-01-03": 104.5}},
    )
    run = simulate(_cfg(entry_mode="next_1430"), data)
    snap = run.positions_by_day[0]
    assert snap["date"] == "2024-01-02"
    assert snap["positions"][0]["entry_price"] == pytest.approx(104.5)
    assert len(run.trades) == 1
    assert run.trades[0].entry_date == "2024-01-02"
    assert run.trades[0].entry_price == pytest.approx(104.5)


def test_next_1430_missing_print_skips_the_fill() -> None:
    data = _data(_print_bars(), {"2024-01-02": {SYM: 70.0}}, px1430={TS: {}})
    run = simulate(_cfg(entry_mode="next_1430"), data)
    assert run.trades == []
    assert run.summary.open_at_end == 0
    assert run.summary.gated_blocks.get("no_1430_print", 0) >= 1


def test_next_1430_limit_locked_print_skips_the_fill() -> None:
    # 14:30 print pinned at the signal-day-close limit (100 x 1.10 = 110).
    data = _data(
        _print_bars(),
        {"2024-01-02": {SYM: 70.0}},
        px1430={TS: {"2024-01-03": 110.5}},
    )
    run = simulate(_cfg(entry_mode="next_1430"), data)
    assert run.trades == []
    assert run.summary.open_at_end == 0
    assert run.summary.gated_blocks.get("limit_up", 0) >= 1


def test_next_1430_no_same_session_exit() -> None:
    """Fill at the D2 print; a D2-close stop must be discarded (A-share T+1).

    Entry 106 at the D2 14:30 print, D2 closes 100 (-5.7% would stop) but the
    fill session cannot sell; the D3 close (100) fires the stop at holding=2,
    exactly like next_open. Exit price stays the decision-day close (there is
    no 14:30 exit leg in the engine).
    """
    data = _data(
        _gap_up_bars(),
        {"2024-01-02": {SYM: 70.0}},
        px1430={TS: {"2024-01-03": 106.0}},
    )
    run = simulate(_cfg(entry_mode="next_1430"), data)
    assert len(run.trades) == 1
    t = run.trades[0]
    assert t.entry_date == "2024-01-02" and t.entry_price == pytest.approx(106.0)
    assert t.close_date == "2024-01-04" and t.close_reason == "stop_hit"
    assert t.holding_days == 2
