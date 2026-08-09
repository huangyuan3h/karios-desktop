"""OPT-063 tests: backtest engine v0 (signal replay + live close logic).

The engine reads DB datasets; tests mock the DB layer so they run anywhere:
- ``_load_calendar`` / ``_load_scores`` are module functions hitting the DB —
  tests patch the loader functions and the engine internals with in-memory
  fixtures instead (pure ``simulate`` path).
- Close-condition reuse is asserted against ``_pick_close_reason`` semantics:
  stop/target on NET pnl, score_floor on the AS-OF score, max_hold on days.
"""

from __future__ import annotations

import pytest

from data_sync_service.service.backtest_engine import (
    CLOSE_REASON_END_OF_WINDOW,
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.execution_gate import REGIME_DIVERGING, REGIME_WEAK

# ---------------------------------------------------------------------------
# Fixtures: in-memory BacktestData
# ---------------------------------------------------------------------------


def _bars(price_map: dict[str, float]) -> list[tuple[str, str, str, str, str, str]]:
    """Build OHLCV tuples from {date: close} (open/high/low == close, vol=1000)."""
    out = []
    for d in sorted(price_map):
        px = str(price_map[d])
        out.append((d, px, px, px, px, "1000"))
    return out


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
    if industry_by_ts is None:
        industry_by_ts = {ts: "计算机" for ts in prices}
    data.industry_by_ts = dict(industry_by_ts)
    return data


CN1 = "CN:600001"
TS1 = "600001.SH"


# ---------------------------------------------------------------------------
# simulate: entries + closes
# ---------------------------------------------------------------------------


def test_simulate_enters_on_score_threshold_and_closes_on_target() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 88.0},
        "2026-06-19": {CN1: 88.0},
        "2026-06-22": {CN1: 88.0},
    }
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.6, "2026-06-22": 12.0}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22")

    run = simulate(config, data=data)
    s = run.summary
    assert s.closed == 1
    t = run.trades[0]
    # Entry at 06-18 close 10.0; +6% gross on 06-19 (net +5.7), no close;
    # +20% gross on 06-22 (net +19.7) >= target 10 → target_hit.
    assert t.entry_price == 10.0
    assert t.close_reason == "target_hit"
    assert t.entry_date == "2026-06-18"
    assert t.close_date == "2026-06-22"
    assert abs(t.gross_pnl_pct - 20.0) < 0.01
    assert abs(t.pnl_pct - (20.0 - 0.3)) < 0.01  # CN round-trip cost 0.30%


def test_simulate_stop_hits_on_net_pnl() -> None:
    """Gross -4.8% does NOT stop; net -5.1% (costs) DOES — same as live paper."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {CN1: 90.0},
    }
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 9.52}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19")

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"
    assert abs(run.trades[0].gross_pnl_pct - (-4.8)) < 0.05


def test_simulate_score_floor_uses_as_of_score() -> None:
    """A score recorded BELOW the floor on day D closes with score_floor;
    a missing score fails open (never closes on score_floor)."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 88.0},
        "2026-06-19": {CN1: 88.0},
        "2026-06-22": {CN1: 20.0},  # dropped below SCORE_FLOOR (30)
    }
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.1, "2026-06-22": 10.2}}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22")

    run = simulate(config, data=_data(calendar, scores, prices))
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "score_floor"

    # Missing score on day 22 → fail open, then end_of_window at window end.
    scores_nofloor = {
        "2026-06-18": {CN1: 88.0},
        "2026-06-19": {CN1: 88.0},
        "2026-06-22": {},  # no score recorded
    }
    run2 = simulate(config, data=_data(calendar, scores_nofloor, prices))
    assert run2.trades[0].close_reason == CLOSE_REASON_END_OF_WINDOW


def test_simulate_max_hold_closes() -> None:
    """Entry 06-18 → 06-23 is 5 calendar days → max_hold fires on 06-23."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {d: {CN1: 88.0} for d in calendar}
    prices = {TS1: {d: 10.0 for d in calendar}}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-23", max_hold_days=5)

    run = simulate(config, data=_data(calendar, scores, prices))
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "max_hold"
    assert run.trades[0].holding_days == 5


def test_simulate_no_second_entry_while_held() -> None:
    """A symbol stays in the daily signals — it must not get a second entry
    while the first position is still open."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {CN1: 92.0},
        "2026-06-19": {CN1: 95.0},
    }
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.0}}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19")

    run = simulate(config, data=_data(calendar, scores, prices))
    assert run.summary.trades == 1


def test_simulate_suspension_day_holds() -> None:
    """No bar on a calendar day (suspension) — the position survives to the
    next priced day instead of closing or erroring."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {d: {CN1: 88.0} for d in calendar}
    # 06-22 close = +10.5% gross → net +10.2% >= target 10 → target_hit.
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-22": 11.05}}  # no bar on 06-19
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22")

    run = simulate(config, data=_data(calendar, scores, prices))
    assert run.summary.closed == 1
    assert run.trades[0].close_date == "2026-06-22"
    assert run.trades[0].close_reason == "target_hit"


def test_simulate_score_bucket_breakdown() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {CN1: 92.0, "CN:600002": 86.0},
        "2026-06-19": {},
    }
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0},
        "600002.SH": {"2026-06-18": 10.0, "2026-06-19": 9.0},
    }
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19")

    run = simulate(config, data=_data(calendar, scores, prices))
    buckets = run.summary.by_score_bucket
    assert buckets[">=90"]["trades"] == 1
    assert buckets["85-90"]["trades"] == 1


def test_simulate_threshold_filters_entries() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {CN1: 84.0, "CN:600002": 88.0},
        "2026-06-19": {},
    }
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5},
        "600002.SH": {"2026-06-18": 10.0, "2026-06-19": 10.5},
    }
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", score_threshold=85.0)

    run = simulate(config, data=_data(calendar, scores, prices))
    assert run.summary.closed == 1
    assert run.trades[0].symbol == "CN:600002"


def test_config_validation() -> None:
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-07", end_date="2026-08-01")
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", market="US")
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", score_threshold=150)
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", gates="all")


# ---------------------------------------------------------------------------
# v1.5 entry gates (OPT-070)
# ---------------------------------------------------------------------------


def _gate_data(calendar: list[str], **overrides) -> dict:
    base = {
        "regime": "Strong",
        "flow_any_positive": True,
        "mainline_allow": {"计算机"},
        "industry_by_ts": {"600001.SH": "计算机"},
    }
    base.update(overrides)
    return base


def _run_with_gates(calendar, scores, prices, gates: str, **gate_overrides):
    return simulate(
        BacktestConfig(
            start_date=calendar[0],
            end_date=calendar[-1],
            score_threshold=85.0,
            gates=gates,
        ),
        data=_data(calendar, scores, prices, **_gate_data(calendar, **gate_overrides)),
    )


def test_gates_none_ignores_all_gate_data() -> None:
    """gates=none keeps v0 behaviour: no regime/flow/mainline filtering."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    run = _run_with_gates(
        calendar,
        scores,
        prices,
        gates="none",
        regime="REGIME_WEAK",
        flow_any_positive=False,
        mainline_allow=set(),
        industry_by_ts={},
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}


def test_gates_regime_blocks_non_strong_market() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}

    for bad_regime in (REGIME_WEAK, REGIME_DIVERGING):
        run = _run_with_gates(calendar, scores, prices, gates="regime", regime=bad_regime)
        assert run.summary.closed == 0
        assert run.summary.gated_blocks == {"regime": 2}  # one attempt per score day

    run = _run_with_gates(calendar, scores, prices, gates="regime")
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}


def test_gates_full_flow_blocks_only_when_all_industries_non_positive() -> None:
    """sectorOutflowBlock mirrors live rule: every SW L1 industry <= 0 blocks."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}

    # no positive industry that day → flow blocks (fail-closed on missing too)
    run = _run_with_gates(calendar, scores, prices, gates="full", flow_any_positive=False)
    assert run.summary.closed == 0
    assert run.summary.gated_blocks == {"flow": 2}  # one attempt per score day

    # at least one positive industry → flow passes, mainline decides
    run = _run_with_gates(
        calendar,
        scores,
        prices,
        gates="full",
        flow_any_positive=True,
        mainline_allow={"电子"},
        industry_by_ts={"600001.SH": "计算机"},
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks == {"mainline": 2}

    # industry allowed → entry happens
    run = _run_with_gates(calendar, scores, prices, gates="full")
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}


def test_gates_full_missing_data_fails_closed() -> None:
    """Missing gate data blocks the entry (live fail-closed posture)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}

    # no industry mapping → mainline blocks
    run = _run_with_gates(
        calendar,
        scores,
        prices,
        gates="full",
        flow_any_positive=True,
        industry_by_ts={},
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks == {"mainline": 2}

    # no flow data for the day → flow blocks
    data = _data(calendar, scores, prices, **_gate_data(calendar))
    data.flow_any_positive_by_day = {}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="full"),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks == {"flow": 2}


def test_default_sensitivity_grid_includes_gate_dimension() -> None:
    from data_sync_service.service.backtest_engine import default_sensitivity_grid

    grid = default_sensitivity_grid("2026-06-18", "2026-08-08")
    assert len(grid) == 72  # 4 score x 3 hold x 3 stop x 2 gate levels
    assert {c.gates for c in grid} == {"none", "full"}
    assert all(c.start_date == "2026-06-18" and c.end_date == "2026-08-08" for c in grid)


# ---------------------------------------------------------------------------
# API: /api/backtest/*
# ---------------------------------------------------------------------------


def _fake_summary_dict() -> dict:
    from data_sync_service.service.backtest_engine import BacktestRun, BacktestSummary

    s = BacktestSummary(
        config={
            "start_date": "2026-06-18",
            "end_date": "2026-08-07",
            "score_threshold": 85.0,
            "max_hold_days": 5,
            "stop_loss_pct": -5.0,
            "target_pnl_pct": 10.0,
            "score_floor": 30.0,
            "market": "CN",
        },
        calendar_days=36,
        trades=21,
        closed=21,
        open_at_end=0,
        wins=8,
        losses=13,
        win_rate=0.381,
        avg_net_pnl_pct=-0.879,
        avg_gross_pnl_pct=-0.579,
        avg_costs_pct=0.3,
        max_drawdown_pct=29.7,
        by_score_bucket={">=90": {"trades": 5, "wins": 2, "winRate": 0.4, "avgNet": -1.0}},
    )
    return BacktestRun(summary=s, trades=[])


def test_backtest_run_endpoint(monkeypatch) -> None:
    from unittest.mock import patch

    from fastapi.testclient import TestClient

    from data_sync_service.main import app

    client = TestClient(app)
    with patch(
        "data_sync_service.api.backtest_routes.simulate",
        return_value=_fake_summary_dict(),
    ):
        resp = client.get(
            "/api/backtest/run?start=2026-06-18&end=2026-08-07&score_threshold=85"
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["summary"]["win_rate"] == 0.381
    assert body["summary"]["config"]["market"] == "CN"


def test_backtest_run_rejects_bad_window() -> None:
    from fastapi.testclient import TestClient

    from data_sync_service.main import app

    client = TestClient(app)
    resp = client.get("/api/backtest/run?start=2026-08-07&end=2026-08-01")
    assert resp.status_code == 422


def test_backtest_run_rejects_bad_market() -> None:
    from fastapi.testclient import TestClient

    from data_sync_service.main import app

    client = TestClient(app)
    resp = client.get("/api/backtest/run?start=2026-08-01&end=2026-08-07&market=US")
    assert resp.status_code == 422


def test_backtest_sensitivity_endpoint(monkeypatch) -> None:
    from unittest.mock import patch

    from fastapi.testclient import TestClient

    from data_sync_service.main import app

    client = TestClient(app)
    with patch(
        "data_sync_service.api.backtest_routes.run_sensitivity",
        return_value=[_fake_summary_dict().summary],
    ):
        resp = client.get("/api/backtest/sensitivity?start=2026-06-18&end=2026-08-07")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["configs"] == 72  # default grid: 4 x 3 x 3 x 2 gate levels
    assert len(body["results"]) == 1


# ---------------------------------------------------------------------------
# wave-1 additions: window end, open positions, market filter, summary
# ---------------------------------------------------------------------------


def test_simulate_closes_leftovers_at_window_end() -> None:
    """A position still open on the last day closes with reason end_of_window."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 88.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.2, "2026-06-22": 11.0}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22")
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.summary.open_at_end == 0
    assert run.trades[0].close_reason == "end_of_window"
    assert run.summary.trades == 1
    assert run.summary.calendar_days == 3
    d = run.summary.to_dict()
    assert isinstance(d, dict) and d["closed"] == 1


def test_simulate_no_bars_means_no_entry_no_crash() -> None:
    """No bars for the symbol at all → nothing enters, nothing closes, no crash."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 88.0}}
    data = _data(calendar, scores, {})
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19")
    run = simulate(config, data=data)
    assert run.summary.closed == 0
    assert run.summary.open_at_end == 0
    assert run.summary.trades == 0


def test_simulate_filters_other_market_symbols() -> None:
    """HK symbol must never enter a CN-config simulation."""
    calendar = ["2026-06-18"]
    scores = {"2026-06-18": {"CN:600001": 88.0, "HK:00700": 95.0}}
    prices = {
        "600001.SH": {"2026-06-18": 10.0},
        "00700.HK": {"2026-06-18": 480.0},
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-18")
    run = simulate(config, data=data)
    assert run.summary.trades == 1
    assert run.trades[0].symbol == "CN:600001"


def test_simulate_skips_entry_without_price() -> None:
    calendar = ["2026-06-18"]
    scores = {"2026-06-18": {CN1: 88.0}}
    data = _data(calendar, scores, {"600001.SH": {"2026-06-17": 10.0}})
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-18")
    run = simulate(config, data=data)
    assert run.summary.trades == 0


def test_calendar_days_between_helpers() -> None:
    from data_sync_service.service.backtest_engine import _calendar_days_between

    assert _calendar_days_between("2026-06-18", "2026-06-18") == 0
    assert _calendar_days_between("2026-06-18", "2026-06-22") == 4
    assert _calendar_days_between("not-a-date", "2026-06-22") == 0
    assert _calendar_days_between("2026-06-18", "bad") == 0




@pytest.mark.requires_postgres
def test_backtest_data_loads_from_db() -> None:
    """BacktestData real-DB path: calendar + scores + bars (dev DB, CN)."""
    from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData

    config = BacktestConfig(start_date="2026-07-27", end_date="2026-07-31")
    data = BacktestData(config)
    assert data.calendar, "dev DB should have bars in the window"
    assert data.close_by_ts_day, "bars should map to closes"
    # every calendar day present as key in some close map is not required,
    # but scores must exist for at least one day (watchlist_score_daily)
    assert data.scores_by_day
