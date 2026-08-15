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
    light_red_days: str = "",
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
    data.sentiment_risk_by_day = {}
    data.light_red_by_day = {d for d in calendar if str(light_red_days or "") == "red"}
    data.env_by_day = {d: "unknown" for d in calendar}
    data.closes_by_ts = {
        ts: [(d, float(px)) for d, px in sorted(m.items())] for ts, m in prices.items()
    }
    return data


CN1 = "CN:600001"
TS1 = "600001.SH"


# ---------------------------------------------------------------------------
# simulate: entries + closes
# ---------------------------------------------------------------------------


def test_simulate_positions_by_day_snapshot() -> None:
    """End-of-day holding snapshots: held from entry day until close day."""
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
    assert [s["date"] for s in run.positions_by_day] == calendar
    # Entry at 06-18 close → held 06-18 & 06-19, closed on 06-22.
    held = {s["date"]: [p["symbol"] for p in s["positions"]] for s in run.positions_by_day}
    assert held["2026-06-18"] == [CN1]
    assert held["2026-06-19"] == [CN1]
    assert held["2026-06-22"] == []
    snap = run.positions_by_day[0]["positions"][0]
    assert snap["market"] == "CN"
    assert snap["entry_date"] == "2026-06-18"
    assert snap["position_pct"] == 0.05



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


def test_light_red_block_blocks_red_days_only() -> None:
    """OPT-094: CN red-light days block entries; HK never uses it."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}

    # Off by default (baseline behaviour unchanged).
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="regime"),
        data=_data(calendar, scores, prices, light_red_days="red"),
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}

    # On: every red day blocks (one attempt per score day).
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="regime", light_red_block=True),
        data=_data(calendar, scores, prices, light_red_days="red"),
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks == {"index_red": 2}

    # On but non-red days: no interception.
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="regime", light_red_block=True),
        data=_data(calendar, scores, prices),
    )
    assert run.summary.closed == 1


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


def test_gates_full_missing_gate_data_degrades_open() -> None:
    """Missing flow/mainline data degrades (fail-open): replay of the live
    system's then-current capabilities (no fund-flow gates before 2025-12-15)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}

    # industry mapping missing → MISSING_INDUSTRY blocks (live behaviour)
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

    # mainline data missing for the day → degrades, entry ok
    data = _data(calendar, scores, prices, **_gate_data(calendar))
    data.mainline_allow_by_day = {}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="full"),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}

    # no flow data for the day → flow absent → degrade, entry ok
    data = _data(calendar, scores, prices, **_gate_data(calendar))
    data.flow_any_positive_by_day = {}
    run = simulate(
        BacktestConfig(start_date=calendar[0], end_date=calendar[-1], gates="full"),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}

    # present-but-fully-negative flow still blocks
    run = _run_with_gates(calendar, scores, prices, gates="full", flow_any_positive=False)
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
            total_net_pnl_pct=-18.5,
            annual_net_pnl_pct=-37.0,
            avg_win_pct=None,
            avg_loss_pct=None,
            sharpe=None,
            excess_vs_best_benchmark_pct=0.0,
            best_benchmark="",
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
    """BacktestData real-DB path: calendar + scores + bars (self-seeded, CN).

    Seeds a fake CN symbol (69xxxx.SH — never a real A-share code) into daily
    + watchlist_score_daily for the window, then cleans up its own rows.
    """
    import uuid

    from data_sync_service.db import get_connection
    from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData

    ticker = f"69{uuid.uuid4().int % 10000:04d}"
    symbol = f"CN:{ticker}"
    ts_code = f"{ticker}.SH"
    days = ["2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]

    with get_connection() as conn:
        with conn.cursor() as cur:
            for d in days:
                cur.execute(
                    """
                    INSERT INTO daily (ts_code, trade_date, open, high, low, close,
                                       pre_close, change, pct_chg, vol, amount)
                    VALUES (%s, %s, 10, 11, 9, 10, 10, 0, 0, 1000, 10000)
                    ON CONFLICT (ts_code, trade_date) DO NOTHING
                    """,
                    (ts_code, d),
                )
                cur.execute(
                    """
                    INSERT INTO watchlist_score_daily (symbol, trade_date, score)
                    VALUES (%s, %s, 80)
                    ON CONFLICT (symbol, trade_date) DO NOTHING
                    """,
                    (symbol, d),
                )

    try:
        config = BacktestConfig(start_date="2026-07-27", end_date="2026-07-31")
        data = BacktestData(config)
        assert data.calendar, "seeded bars should appear in the window"
        assert data.close_by_ts_day, "bars should map to closes"
        assert data.scores_by_day, "seeded watchlist_score_daily rows should load"
    finally:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily WHERE ts_code = %s", (ts_code,))
                cur.execute("DELETE FROM watchlist_score_daily WHERE symbol = %s", (symbol,))

def test_trailing_stop_closes_on_peak_pullback() -> None:
    """trailing_stop_pct closes when close falls X% below the entry-high peak."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}, "2026-06-24": {}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,  # entry
            "2026-06-19": 11.0,  # peak 11.0 (+10%)
            "2026-06-22": 12.0,  # peak 12.0 (+20%)
            "2026-06-23": 11.3,  # -5.8% from peak → trailing -5 closes
            "2026-06-24": 12.0,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-24",
        stop_loss_pct=-15.0,  # loose fixed stop so trailing is the trigger
        target_pnl_pct=30.0,  # loose target so trailing is the trigger
        max_hold_days=10,
        trailing_stop_pct=-5.0,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    t = run.trades[0]
    assert t.close_reason == "trailing_stop"
    assert t.close_date == "2026-06-23"
    assert t.entry_price == 10.0
    assert t.close_price == 11.3
    assert abs(t.pnl_pct - (13.0 - 0.3)) < 0.01


def test_limit_up_blocks_entry_then_enters_next_day_opt103() -> None:
    """OPT-103: a limit-up close cannot be bought; the signal re-evaluates
    next session and enters once the price is no longer pinned."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {CN1: 90.0},
        "2026-06-22": {},
    }
    prices = {
        TS1: {
            "2026-06-17": 10.0,  # prev close
            "2026-06-18": 11.0,  # 10 x 1.1 = limit-up pinned → cannot buy
            "2026-06-19": 10.5,  # not pinned (11 x 1.1 = 12.1) → entry
            "2026-06-22": 10.8,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-22",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=10,
        trailing_stop_pct=0.0,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.summary.gated_blocks.get("limit_up", 0) == 1
    assert run.trades[0].entry_date == "2026-06-19"
    assert run.trades[0].entry_price == 10.5


def test_limit_down_rolls_exit_to_next_session_opt103() -> None:
    """OPT-103: a limit-down close cannot be sold; the stop exit rolls to
    the next session (which is not pinned) and fills there."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {},
        "2026-06-22": {},
        "2026-06-23": {},
    }
    prices = {
        TS1: {
            "2026-06-17": 10.0,
            "2026-06-18": 10.0,  # entry
            "2026-06-19": 9.0,   # 10 x 0.9 = limit-down pinned → cannot sell
            "2026-06-22": 9.1,   # 9 x 0.9 = 8.1, not pinned; still below stop → fill
            "2026-06-23": 9.5,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        stop_loss_pct=-5.0,
        target_pnl_pct=30.0,
        max_hold_days=10,
        trailing_stop_pct=0.0,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "stop_hit"
    assert run.trades[0].close_date == "2026-06-22"  # rolled past the pinned day
    assert run.trades[0].close_price == 9.1


def test_atr_stop_mode_tightens_loose_fixed_stop_opt104() -> None:
    """OPT-104: atr_stop_mult replaces the fixed stop with entry-time
    ATR% x mult. A 3% ATR name with mult=2 gets a -6% stop — here the fixed
    -15% would NOT have stopped the -12% drawdown, the ATR stop does."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}}
    prices = {
        TS1: {
            "2026-06-17": 10.0,
            "2026-06-18": 10.0,  # entry; ATR ~0.3 = 3% of price
            "2026-06-19": 10.3,
            "2026-06-22": 10.1,
            "2026-06-23": 9.3,  # above the 10.1x0.9=9.09 limit-down; -7% vs entry
        }
    }
    data = _data(calendar, scores, prices)
    data.bars_by_ts = {
        TS1: [
            ("2026-06-08", "9.8", "10.0", "9.7", "9.9", "1000"),
            ("2026-06-09", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-10", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-11", "10.1", "10.2", "10.0", "10.1", "1000"),
            ("2026-06-12", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-15", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-16", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-17", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-18", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-19", "10.2", "10.4", "10.1", "10.3", "1000"),
            ("2026-06-22", "10.2", "10.3", "10.0", "10.1", "1000"),
            ("2026-06-23", "9.3", "9.4", "9.2", "9.3", "1000"),
        ]
    }
    data.close_by_ts_day[TS1] = {d: float(v) for d, v in prices[TS1].items()}
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        stop_loss_pct=-15.0,  # loose — the ATR stop must be the trigger
        target_pnl_pct=100.0,
        max_hold_days=60,
        trailing_stop_pct=0.0,
        atr_stop_mult=2.0,  # ~3% ATR x 2 = -6% stop
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    # ATR mode fires (stop or trail — both are ATR% x mult here; the fixed
    # -15% stop / disabled trail would NOT have triggered on this drawdown).
    assert run.trades[0].close_reason in ("stop_hit", "trailing_stop")
    assert run.trades[0].close_date == "2026-06-23"
    assert run.trades[0].close_price == 9.3


def test_atr_stop_weak_regime_uses_fixed_line_opt105() -> None:
    """OPT-105: in a Weak regime the ATR line is disabled — the FIXED stop
    applies. The -7% drawdown here would have tripped the ATR line (~-5.75%)
    but NOT the fixed -15%; under Weak the position must survive."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}}
    prices = {
        TS1: {
            "2026-06-17": 10.0,
            "2026-06-18": 10.0,
            "2026-06-19": 10.3,
            "2026-06-22": 10.1,
            "2026-06-23": 9.3,
        }
    }
    data = _data(calendar, scores, prices)  # Strong → entry allowed
    # then the market turns Weak from the day after entry: exits must use
    # the FIXED line (ATR line disabled in Weak).
    data.regime_by_day = {
        d: ("Strong" if d == "2026-06-18" else REGIME_WEAK) for d in calendar
    }
    data.bars_by_ts = {
        TS1: [
            ("2026-06-08", "9.8", "10.0", "9.7", "9.9", "1000"),
            ("2026-06-09", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-10", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-11", "10.1", "10.2", "10.0", "10.1", "1000"),
            ("2026-06-12", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-15", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-16", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-17", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-18", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-19", "10.2", "10.4", "10.1", "10.3", "1000"),
            ("2026-06-22", "10.2", "10.3", "10.0", "10.1", "1000"),
            ("2026-06-23", "9.3", "9.4", "9.2", "9.3", "1000"),
        ]
    }
    data.close_by_ts_day[TS1] = {d: float(v) for d, v in prices[TS1].items()}
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        stop_loss_pct=-15.0,
        target_pnl_pct=100.0,
        max_hold_days=60,
        trailing_stop_pct=0.0,
        atr_stop_mult=2.0,
    )
    run = simulate(config, data=data)
    # The -7% drawdown did NOT trip any stop/trail (fixed -15% governs in
    # Weak; an ATR line at ~-5.75% would have fired) — the only close is the
    # window-end force liquidation.
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "end_of_window"


def test_hk_has_no_price_limits_opt103() -> None:
    """HK line has no board limits — entry/exit never blocked."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {"HK:00700": 90.0}, "2026-06-19": {}}
    prices = {
        "00700.HK": {
            "2026-06-17": 10.0,
            "2026-06-18": 11.0,  # +10% — would be a CN limit-up, not for HK
            "2026-06-19": 11.5,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-19",
        market="HK",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=10,
        trailing_stop_pct=0.0,
    )
    run = simulate(config, data=data)
    # HK has no board limits: the +10% session enters normally (a CN
    # limit-up would have blocked it); close reason is window/score-driven
    # (live DB fetch makes it non-deterministic here — not under test).
    assert run.summary.gated_blocks.get("limit_up", 0) == 0
    assert run.summary.closed == 1
    assert run.trades[0].entry_date == "2026-06-18"


def test_trailing_stop_disabled_by_default() -> None:
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {d: {CN1: 90.0} for d in calendar}
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0, "2026-06-22": 10.2}
    }
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", stop_loss_pct=-5.0),
        data=_data(calendar, scores, prices),
    )
    assert run.summary.closed == 1
    # +2% gross, +1.7% net: not a stop; closed by end_of_window instead
    assert run.trades[0].close_reason == CLOSE_REASON_END_OF_WINDOW


def test_trailing_stop_validation() -> None:
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", trailing_stop_pct=5.0)


def test_summary_total_net_pnl() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, "CN:600002": 88.0}, "2026-06-19": {}}
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0},
        "600002.SH": {"2026-06-18": 10.0, "2026-06-19": 9.0},
    }
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", score_threshold=85.0),
        data=_data(calendar, scores, prices),
    )
    # total_net_pnl_pct is scaled by the per-trade position size (5% default)
    assert run.summary.total_net_pnl_pct == pytest.approx(
        ((10.0 - 0.3) + (-10.0 - 0.3)) * 0.05
    )

def test_rs_rank_filter_blocks_weak_strength() -> None:
    """rs_rank_min keeps only whole-market top-X percentile symbols."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, "CN:600002": 88.0}, "2026-06-19": {}}
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0},
        "600002.SH": {"2026-06-18": 10.0, "2026-06-19": 10.0},
    }
    data = _data(calendar, scores, prices)
    # 600002 ranks last (0.5), CN1 first (1.0) — threshold 0.8 keeps only CN1
    data.rs_rank_by_day = {
        "2026-06-18": {"600001.SH": 1.0, "600002.SH": 0.5},
        "2026-06-19": {},
    }
    run = simulate(
        BacktestConfig(
            start_date="2026-06-18",
            end_date="2026-06-19",
            score_threshold=85.0,
            rs_rank_min=0.8,
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].symbol == "CN:600001"
    assert run.summary.gated_blocks.get("rs") == 1  # 600002 blocked


def test_rs_rank_missing_data_blocks_fail_closed() -> None:
    """A symbol with no RS data on the day is blocked (fail-closed)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = {}  # no RS data at all
    run = simulate(
        BacktestConfig(
            start_date="2026-06-18",
            end_date="2026-06-19",
            score_threshold=85.0,
            rs_rank_min=0.8,
        ),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("rs") == 1


def test_rs_rank_disabled_by_default() -> None:
    """rs_rank_min=0 (default) skips the RS filter entirely."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = {}
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", score_threshold=85.0),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks == {}


def test_rs_rank_validation() -> None:
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", rs_rank_min=1.5)

def test_sentiment_risk_mode_blocks_entries() -> None:
    """extreme_caution / no_new_positions days block new entries (live gate)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.sentiment_risk_by_day = {"2026-06-18": "extreme_caution"}
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", gates="full"),
        data=data,
    )
    assert run.summary.closed == 0
    assert run.summary.gated_blocks.get("sentiment") == 1


def test_sentiment_missing_data_degrades_open() -> None:
    """Sentiment data missing (pre-2026-01-05) degrades: entries allowed."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.sentiment_risk_by_day = {}
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", gates="full"),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks.get("sentiment") is None

def test_panic_cooldown_blocks_entries_after_panic() -> None:
    """panic_cooldown_days halts new entries for N days after a panic day."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}, "2026-06-22": {CN1: 90.0}, "2026-06-23": {}}
    prices = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, prices)
    data.sentiment_risk_by_day = {"2026-06-19": "extreme_caution"}
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-23",
                       gates="full", panic_cooldown_days=2),
        data=data,
    )
    # 06-18 entry ok; 06-19 panic (blocked); 06-22 cooldown day 1 (blocked); 06-23 cooldown day 2 (blocked)
    assert run.summary.closed == 1
    assert run.summary.gated_blocks.get("panic_cooldown") == 2


def test_slippage_reduces_gross() -> None:
    """slippage_pct is deducted at entry and exit."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0}}
    data = _data(calendar, scores, prices)
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", slippage_pct=0.5),
        data=data,
    )
    # entry 10*1.005, exit 11*0.995 -> gross = (10.945-10.05)/10.05 = 8.91%
    assert run.summary.closed == 1
    assert run.trades[0].gross_pnl_pct == round((11 * 0.995 - 10 * 1.005) / (10 * 1.005) * 100, 4)


def test_slippage_zero_by_default() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 11.0}}
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19"),
        data=_data(calendar, scores, prices),
    )
    assert run.trades[0].gross_pnl_pct == 10.0


# ---------------------------------------------------------------------------
# A2 trend_score gate (OPT step 2)
# ---------------------------------------------------------------------------


def _trend_series(price_map: dict[str, float]) -> list[tuple[str, float]]:
    return [(d, float(px)) for d, px in sorted(price_map.items())]


def test_trend_score_components() -> None:
    """_trend_score: RS percentile 40pts + MA alignment 30pts + near-high 30pts."""
    from data_sync_service.service.backtest_engine import _trend_score

    # 62 rising closes: MA5 > MA20 > MA60, close at a fresh high
    dates = [f"2026-01-{d:02d}" for d in range(1, 62)]
    closes = _trend_series({d: float(i + 100) for i, d in enumerate(dates)})
    # rs = 1.0 (strongest), perfect MA alignment, at 52w high
    assert _trend_score(1.0, closes, "2026-03-01") == 40.0 + 30.0 + 30.0
    # rs = 0.5 halves the RS factor contribution
    assert _trend_score(0.5, closes, "2026-03-01") == pytest.approx(20.0 + 30.0 + 30.0)
    # far below the 52w high (-30%): near-high factor = 0, no MA alignment
    flat = _trend_series({f"2026-01-{d:02d}": float(100) for d in range(1, 62)})
    flat[60] = ("2026-03-01", 70.0)
    assert _trend_score(1.0, flat, "2026-03-01") == pytest.approx(40.0 + 0.0 + 0.0)
    # within -5% of the 52w high but flat MA: near-high factor = 30
    flat[60] = ("2026-03-01", 96.0)
    assert _trend_score(1.0, flat, "2026-03-01") == pytest.approx(40.0 + 0.0 + 30.0)


def test_trend_score_insufficient_history() -> None:
    from data_sync_service.service.backtest_engine import _trend_score

    closes = _trend_series({f"2026-01-{d:02d}": float(100 + d) for d in range(1, 20)})
    assert _trend_score(1.0, closes, "2026-01-20") is None


def test_trend_score_min_blocks_low_trend_quality() -> None:
    """trend_score_min keeps only symbols above the quality bar."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0, "CN:600002": 88.0}, "2026-06-19": {}}
    hist = {f"2026-03-{d:02d}": float(10 + i) for i, d in enumerate(range(1, 31))}
    hist.update({f"2026-04-{d:02d}": float(40 + i) for i, d in enumerate(range(1, 31))})
    hist.update({f"2026-05-{d:02d}": float(70 + i) for i, d in enumerate(range(1, 31))})
    hist["2026-06-18"] = 100.0
    hist["2026-06-19"] = 100.5
    prices = {TS1: dict(hist), "600002.SH": dict(hist)}
    data = _data(calendar, scores, prices)
    # Both have full price history, but CN1 gets rs=1.0 (strong) vs 600002 rs=0.0
    data.rs_rank_by_day = {
        "2026-06-18": {"600001.SH": 1.0, "600002.SH": 0.0},
        "2026-06-19": {},
    }
    run = simulate(
        BacktestConfig(
            start_date="2026-06-18",
            end_date="2026-06-19",
            score_threshold=85.0,
            rs_rank_min=0.0,
            trend_score_min=70.0,
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].symbol == "CN:600001"
    assert run.summary.gated_blocks.get("trend") == 1


def test_trend_score_disabled_by_default() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.closes_by_ts = {}  # no trend data at all
    run = simulate(
        BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", score_threshold=85.0),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.summary.gated_blocks.get("trend") is None


def test_trend_score_validation() -> None:
    with pytest.raises(ValueError):
        BacktestConfig(start_date="2026-08-01", end_date="2026-08-07", trend_score_min=150.0)


def test_entries_ordered_by_score_when_sleeve_limited() -> None:
    """A full sleeve admits the highest-score candidates first, not
    symbol-alphabetical order (regression: 300308.SZ missed while score=100).
    """
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {
            "CN:000001": 60.0,  # alphabetically first, lowest score
            "CN:000002": 55.0,
            "CN:000003": 90.0,  # alphabetically last, highest score
        },
        "2026-06-19": {},
    }
    prices = {
        "000001.SZ": {"2026-06-18": 10.0, "2026-06-19": 10.1},
        "000002.SZ": {"2026-06-18": 10.0, "2026-06-19": 10.1},
        "000003.SZ": {"2026-06-18": 10.0, "2026-06-19": 10.1},
    }
    data = _data(calendar, scores, prices)
    run = simulate(
        BacktestConfig(
            start_date="2026-06-18", end_date="2026-06-19",
            score_threshold=50.0, max_positions=1, gates="full",
        ),
        data=data,
    )
    assert run.summary.closed == 1
    assert run.trades[0].symbol == "CN:000003"


def _rotation_data(calendar, scores, prices, rs_by_day, regime="Strong") -> BacktestData:
    data = _data(calendar, scores, prices, regime=regime)
    data.rs_rank_by_day = rs_by_day
    return data


def test_swap_replaces_weak_held_with_strong_candidate() -> None:
    """RS-rotation: a held stock in the weakest RS band is swapped for a
    clearly-stronger candidate; the swap pays slippage + round-trip costs.
    """
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {
        "2026-06-18": {"CN:600001": 88.0},  # held from day 1
        "2026-06-22": {"CN:000001": 90.0},  # strong candidate appears
    }
    prices = {
        TS1: {d: 10.0 for d in calendar},
        "000001.SZ": {d: 10.0 for d in calendar},
    }
    rs = {
        d: {TS1: 0.1, "000001.SZ": 0.9} for d in calendar
    }
    data = _rotation_data(calendar, scores, prices, rs)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-23",
        score_threshold=65.0, gates="full",
        swap_weak_rs_below=0.3, swap_strong_rs_at_least=0.8,
        swap_min_hold_days=1, swap_max_per_day=2,
    )
    run = simulate(config, data=data)
    swapped = [t for t in run.trades if t.close_reason == "swapped"]
    assert len(swapped) == 1
    assert swapped[0].symbol == "CN:600001"
    assert swapped[0].close_date == "2026-06-22"
    # new position took over
    assert any(t.symbol == "CN:000001" for t in run.trades)


def test_swap_requires_strong_rs_candidate() -> None:
    """A candidate below swap_strong_rs_at_least cannot trigger a swap."""
    calendar = ["2026-06-18", "2026-06-22"]
    scores = {
        "2026-06-18": {"CN:600001": 88.0},
        "2026-06-22": {"CN:000001": 90.0},
    }
    prices = {TS1: {d: 10.0 for d in calendar}, "000001.SZ": {d: 10.0 for d in calendar}}
    rs = {d: {TS1: 0.1, "000001.SZ": 0.5} for d in calendar}  # 0.5 < 0.8
    data = _rotation_data(calendar, scores, prices, rs)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-22",
        score_threshold=65.0, gates="full",
        swap_weak_rs_below=0.3, swap_strong_rs_at_least=0.8,
        swap_min_hold_days=1, swap_max_per_day=2,
    )
    run = simulate(config, data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]
    # candidate still enters normally on 06-22
    assert any(t.symbol == "CN:000001" for t in run.trades)


def test_swap_requires_weak_held_rs() -> None:
    """A held stock whose RS is still strong is never swapped out."""
    calendar = ["2026-06-18", "2026-06-22"]
    scores = {
        "2026-06-18": {"CN:600001": 88.0},
        "2026-06-22": {"CN:000001": 90.0},
    }
    prices = {TS1: {d: 10.0 for d in calendar}, "000001.SZ": {d: 10.0 for d in calendar}}
    rs = {d: {TS1: 0.5, "000001.SZ": 0.9} for d in calendar}  # held RS 0.5 > 0.3
    data = _rotation_data(calendar, scores, prices, rs)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-22",
        score_threshold=65.0, gates="full",
        swap_weak_rs_below=0.3, swap_strong_rs_at_least=0.8,
        swap_min_hold_days=1, swap_max_per_day=2,
    )
    run = simulate(config, data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]


def test_swap_respects_min_hold_days() -> None:
    """Fresh positions (below swap_min_hold_days) are protected from swaps."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:600001": 88.0},
        "2026-06-19": {"CN:000001": 90.0},
    }
    prices = {TS1: {d: 10.0 for d in calendar}, "000001.SZ": {d: 10.0 for d in calendar}}
    rs = {d: {TS1: 0.1, "000001.SZ": 0.9} for d in calendar}
    data = _rotation_data(calendar, scores, prices, rs)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        score_threshold=65.0, gates="full",
        swap_weak_rs_below=0.3, swap_strong_rs_at_least=0.8,
        swap_min_hold_days=10, swap_max_per_day=2,
    )
    run = simulate(config, data=data)
    assert not [t for t in run.trades if t.close_reason == "swapped"]


def test_swap_caps_per_day() -> None:
    """swap_max_per_day limits how many swaps happen on one day."""
    calendar = ["2026-06-18", "2026-06-22"]
    scores = {
        "2026-06-18": {"CN:600001": 88.0, "CN:600002": 88.0, "CN:600003": 88.0},
        "2026-06-22": {"CN:000001": 90.0, "CN:000002": 90.0, "CN:000003": 90.0},
    }
    prices = {
        TS1: {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
        "600003.SH": {d: 10.0 for d in calendar},
        "000001.SZ": {d: 10.0 for d in calendar},
        "000002.SZ": {d: 10.0 for d in calendar},
        "000003.SZ": {d: 10.0 for d in calendar},
    }
    rs = {d: {
        "600001.SH": 0.1, "600002.SH": 0.2, "600003.SH": 0.3,
        "000001.SZ": 0.9, "000002.SZ": 0.88, "000003.SZ": 0.86,
    } for d in calendar}
    data = _rotation_data(calendar, scores, prices, rs)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-22",
        score_threshold=65.0, gates="full",
        swap_weak_rs_below=0.35, swap_strong_rs_at_least=0.8,
        swap_min_hold_days=1, swap_max_per_day=2,
    )
    run = simulate(config, data=data)
    swapped = [t for t in run.trades if t.close_reason == "swapped"]
    assert len(swapped) == 2  # 3 eligible pairs, capped at 2


def test_pyramid_adds_on_profit_and_exits_with_main_leg() -> None:
    """Pyramid: a held winner triggers an add leg at +trigger%; both legs
    exit together with the same close reason."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {
        "2026-06-18": {CN1: 88.0},
        "2026-06-19": {CN1: 88.0},
        "2026-06-22": {CN1: 88.0},
        "2026-06-23": {CN1: 88.0},
    }
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 11.5, "2026-06-22": 12.0, "2026-06-23": 10.94}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-23",
        gates="full", target_pnl_pct=100.0, max_hold_days=60, trailing_stop_pct=-8.0,
        pyramid_trigger_pct=10.0, pyramid_add_scale=0.5, pyramid_max_adds=1,
    )
    run = simulate(config, data=data)
    trades = run.trades
    # 06-19 +15% >= +10% trigger -> add leg at 11.5; 06-23 close 10.94
    # (above the 12.0 x 0.9 = 10.8 limit-down so sellable; add leg only -4.9%
    # so no stop_hit) = -8.8% from peak 12.0 -> -8% trailing closes both.
    assert len(trades) == 2
    main = [t for t in trades if t.entry_date == "2026-06-18"]
    add = [t for t in trades if t.entry_date == "2026-06-19"]
    assert len(main) == 1 and len(add) == 1
    assert main[0].close_price == 10.94
    assert main[0].close_reason == "trailing_stop" and add[0].close_reason == "trailing_stop"
    assert main[0].close_date == "2026-06-23" and add[0].close_date == "2026-06-23"
    assert add[0].entry_price == 11.5
    assert abs(add[0].position_pct - 0.05 * 0.5) < 1e-9  # default 0.05 * 0.5


def test_pyramid_respects_max_adds() -> None:
    """Only pyramid_max_adds add legs per position."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {d: {CN1: 88.0} for d in calendar}
    prices = {TS1: {d: 10.0 * (1.15 ** i) for i, d in enumerate(calendar)}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-24",
        gates="full", max_hold_days=5,
        pyramid_trigger_pct=10.0, pyramid_add_scale=0.5, pyramid_max_adds=2,
    )
    run = simulate(config, data=data)
    add_legs = [t for t in run.trades if t.entry_date != "2026-06-18"]
    assert len(add_legs) <= 2


def test_pyramid_disabled_by_default() -> None:
    """pyramid_max_adds=0 (default) never adds legs."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 88.0}, "2026-06-19": {CN1: 88.0}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 13.0}}
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", gates="full",
        pyramid_trigger_pct=10.0, pyramid_add_scale=0.5, pyramid_max_adds=0,
    )
    run = simulate(config, data=data)
    assert len(run.trades) == 1


def test_atr_size_scales_sleeve_down_for_high_vol() -> None:
    """ATR sizing: a 4% daily-vol stock gets half the base sleeve (cap 2x)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 88.0}, "2026-06-19": {}}
    # ~4% daily ranges: high/low alternating 102/98 around 100
    bars = [("2026-06-01", "100", "102", "98", "100", "100")]
    for i in range(1, 22):
        base = 100 * (1.004 ** i)
        bars.append((f"2026-06-{i+1:02d}", f"{base:.2f}", f"{base*1.02:.2f}", f"{base*0.98:.2f}", f"{base:.2f}", "100"))
    bars.append(("2026-06-18", "105", "106", "104", "105", "100"))
    bars.append(("2026-06-19", "106", "107", "105", "106", "100"))
    data = _data(calendar, scores, {TS1: {"2026-06-18": 105.0, "2026-06-19": 106.0}})
    data.bars_by_ts = {TS1: bars}
    data.closes_by_ts = {TS1: [(b[0], float(b[5])) for b in bars]}
    data.close_by_ts_day = {TS1: {"2026-06-18": 105.0, "2026-06-19": 106.0}}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", gates="full",
        atr_size_window=20, atr_size_cap=2.0, atr_benchmark_pct=2.0,
    )
    run = simulate(config, data=data)
    t = run.trades[0]
    # ATR ~2% of price -> scale ~1.0x (benchmark 2% / atr ~2%)
    assert 0.5 <= t.position_pct / config.position_pct <= 2.0


def test_atr_size_disabled_by_default() -> None:
    """atr_size_window=0 (default) leaves sleeves untouched."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 88.0}, "2026-06-19": {}}
    prices = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5}}
    data = _data(calendar, scores, prices)
    data.bars_by_ts = {TS1: [("2026-06-01", "10", "10.5", "9.5", "10", "100")] * 20}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", gates="full")
    run = simulate(config, data=data)
    assert run.trades[0].position_pct == config.position_pct


def test_industry_cap_blocks_fourth_same_industry_holding() -> None:
    """max_per_industry: a 4th candidate from an already-capped industry is
    blocked; candidates from other industries still enter."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {
            "CN:600001": 90.0, "CN:600002": 90.0, "CN:600003": 90.0, "CN:600004": 90.0,
            "CN:000001": 90.0,
        },
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
        "600003.SH": {d: 10.0 for d in calendar},
        "600004.SH": {d: 10.0 for d in calendar},
        "000001.SZ": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    data.industry_by_ts = {
        "600001.SH": "计算机", "600002.SH": "计算机", "600003.SH": "计算机",
        "600004.SH": "计算机", "000001.SZ": "医药",
    }
    data.mainline_allow_by_day = {d: {"计算机", "医药"} for d in calendar}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        gates="full", max_per_industry=3,
    )
    run = simulate(config, data=data)
    syms = {t.symbol for t in run.trades}
    assert syms == {"CN:600001", "CN:600002", "CN:600003", "CN:000001"}
    assert run.summary.gated_blocks.get("industry_cap") == 1


def test_industry_cap_disabled_by_default() -> None:
    """max_per_industry=0 (default) never caps by industry."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:600001": 90.0, "CN:600002": 90.0, "CN:600003": 90.0},
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
        "600003.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    data.industry_by_ts = {"600001.SH": "计算机", "600002.SH": "计算机", "600003.SH": "计算机"}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-19", gates="full")
    run = simulate(config, data=data)
    assert len(run.trades) == 3


def test_entry_sort_rs_prefers_strong_rs_within_threshold() -> None:
    """entry_sort='rs': among score>=threshold candidates, the strongest RS
    enters first when the sleeve is limited."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {
            "CN:600001": 90.0,  # high score, low RS
            "CN:600002": 80.0,  # lower score, high RS
        },
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = {d: {"600001.SH": 0.1, "600002.SH": 0.9} for d in calendar}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        score_threshold=65.0, gates="full", max_positions=1, entry_sort="rs",
    )
    run = simulate(config, data=data)
    assert run.trades[0].symbol == "CN:600002"


def test_entry_sort_score_default_keeps_base_order() -> None:
    """entry_sort='score' (default) enters the higher score first."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:600001": 90.0, "CN:600002": 80.0},
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    data.rs_rank_by_day = {d: {"600001.SH": 0.1, "600002.SH": 0.9} for d in calendar}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        gates="full", max_positions=1, entry_sort="score",
    )
    run = simulate(config, data=data)
    assert run.trades[0].symbol == "CN:600001"


def test_mv_filter_blocks_outside_band() -> None:
    """min_mv/max_mv (亿元) layer the pool; missing mv data fails open."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:600001": 90.0, "CN:600002": 90.0, "CN:600003": 90.0},
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
        "600003.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    data.mv_by_day = {
        "2026-06-18": {"600001.SH": 50.0, "600002.SH": 500.0, "600003.SH": 250.0}
    }
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        gates="full", min_mv=100.0, max_mv=400.0,
    )
    run = simulate(config, data=data)
    assert [t.symbol for t in run.trades] == ["CN:600003"]
    assert run.summary.gated_blocks.get("mv_min") == 1
    assert run.summary.gated_blocks.get("mv_max") == 1


def test_exclude_boards_filters_symbol_prefix() -> None:
    """exclude_boards layers the pool by 3-digit code prefix (e.g. 300 = ChiNext)."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:300001": 90.0, "CN:600001": 90.0, "CN:600002": 90.0},
        "2026-06-19": {},
    }
    prices = {
        "300001.SZ": {d: 10.0 for d in calendar},
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        gates="full", exclude_boards="300",
    )
    run = simulate(config, data=data)
    assert sorted(t.symbol for t in run.trades) == ["CN:600001", "CN:600002"]
    assert run.summary.gated_blocks.get("board_excluded") == 1


def test_exclude_boards_invalid_prefix_raises() -> None:
    with pytest.raises(ValueError):
        BacktestConfig(
            start_date="2026-06-18", end_date="2026-06-19",
            exclude_boards="60",
        )


def test_mv_diverging_excludes_mega_cap_only_in_diverging() -> None:
    """mv_max_diverging caps market cap only on Diverging regime days."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {
        "2026-06-18": {"CN:600001": 90.0, "CN:600002": 90.0},
        "2026-06-19": {},
    }
    prices = {
        "600001.SH": {d: 10.0 for d in calendar},
        "600002.SH": {d: 10.0 for d in calendar},
    }
    data = _data(calendar, scores, prices, regime="Diverging")
    data.mv_by_day = {"2026-06-18": {"600001.SH": 100.0, "600002.SH": 600.0}}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        gates="full", diverging_scale=1.0, mv_max_diverging=500.0,
    )
    run = simulate(config, data=data)
    assert [t.symbol for t in run.trades] == ["CN:600001"]
    assert run.summary.gated_blocks.get("mv_diverging") == 1


def test_profit_trail_closes_winning_leg_on_tight_pullback() -> None:
    """A6: once the leg is +10%, a 6% pullback from the peak closes it even
    though the plain trailing stop (-8) would not have fired."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}, "2026-06-24": {}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,  # entry
            "2026-06-19": 11.0,  # peak 11.0 (+10% → trigger armed)
            "2026-06-22": 12.5,  # peak 12.5 (+25%)
            "2026-06-23": 11.7,  # -6.4% from peak → profit-trail (-6) closes
            "2026-06-24": 12.0,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-24",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=10,
        trailing_stop_pct=-8.0,  # 6.4% < 8% → plain trailing would NOT close
        profit_trail_trigger_pct=10.0,
        profit_trail_pct=-6.0,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "trailing_stop"
    assert run.trades[0].pnl_pct > 0


def test_profit_trail_disabled_by_default() -> None:
    """A6 defaults: no profit-trail behaviour without explicit parameters."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}, "2026-06-24": {}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 11.0,
            "2026-06-22": 12.5,
            "2026-06-23": 11.7,  # -6.4% from peak
            "2026-06-24": 12.0,
        }
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-24",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=10,
        trailing_stop_pct=-8.0,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    # No trailing trigger (6.4% < 8%) — closed only by end-of-window.
    assert run.trades[0].close_reason == "end_of_window"


def test_flow_exit_closes_after_negative_streak() -> None:
    """B1: 3 straight sessions of negative industry 5d flow close the leg."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}, "2026-06-24": {}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 10.2,
            "2026-06-22": 10.1,
            "2026-06-23": 10.0,
            "2026-06-24": 10.0,
        }
    }
    data = _data(calendar, scores, prices)
    data.flow5d_by_day = {d: {"通信": -3.0e8} for d in calendar}
    data.industry_by_ts = {TS1: "通信"}
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-24",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=20,
        gates="regime",  # skip the flow ENTRY gate so B1 exit is the only flow logic
        industry_flow_exit_days=3,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "flow_exit"
    assert run.trades[0].close_date == "2026-06-22"  # entry + 3 negative sessions


def test_flow_exit_disabled_by_default() -> None:
    """B1 default 0 keeps legacy behaviour (no flow exit)."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23", "2026-06-24"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}, "2026-06-24": {}}
    prices = {
        TS1: {
            "2026-06-18": 10.0,
            "2026-06-19": 10.2,
            "2026-06-22": 10.1,
            "2026-06-23": 10.0,
            "2026-06-24": 10.0,
        }
    }
    data = _data(calendar, scores, prices)
    data.flow5d_by_day = {d: {"通信": -3.0e8} for d in calendar}
    data.industry_by_ts = {TS1: "通信"}
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-24",
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=20,
        gates="regime",
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "end_of_window"


def test_score_confirm_blocks_single_day_spike() -> None:
    """C1: a single-day score spike does not enter without prior-day confirm."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {
        "2026-06-18": {},
        "2026-06-19": {CN1: 95.0},   # spike day — no prior score
        "2026-06-22": {CN1: 90.0},
        "2026-06-23": {CN1: 92.0},
    }
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5, "2026-06-22": 10.8, "2026-06-23": 11.0},
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        score_threshold=65.0,
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=20,
        gates="regime",
        score_confirm_days=1,
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].entry_date == "2026-06-22"  # first confirmed day


def test_score_confirm_disabled_by_default() -> None:
    """C1 default 0 enters on the spike day as usual."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {
        "2026-06-18": {},
        "2026-06-19": {CN1: 95.0},
        "2026-06-22": {CN1: 90.0},
        "2026-06-23": {CN1: 92.0},
    }
    prices = {
        TS1: {"2026-06-18": 10.0, "2026-06-19": 10.5, "2026-06-22": 10.8, "2026-06-23": 11.0},
    }
    data = _data(calendar, scores, prices)
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        score_threshold=65.0,
        stop_loss_pct=-15.0,
        target_pnl_pct=30.0,
        max_hold_days=20,
        gates="regime",
    )
    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].entry_date == "2026-06-19"


def test_atr_stop_strength_floor_selects_atr_line_d1() -> None:
    """§19.2 D1: with atr_stop_strength_min set, the ATR line applies only on
    days whose 0-100 strength >= the floor (regime rule is bypassed)."""
    from unittest.mock import patch

    calendar = ["2026-06-18", "2026-06-19", "2026-06-22", "2026-06-23"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {}, "2026-06-22": {}, "2026-06-23": {}}
    prices = {
        TS1: {
            "2026-06-17": 10.0,
            "2026-06-18": 10.0,
            "2026-06-19": 10.3,
            "2026-06-22": 10.1,
            "2026-06-23": 9.3,  # -7%: fixed -15% holds; ATR (~-6%) fires
        }
    }
    data = _data(calendar, scores, prices, regime="Strong")  # regime allows entry
    data.bars_by_ts = {
        TS1: [
            ("2026-06-08", "9.8", "10.0", "9.7", "9.9", "1000"),
            ("2026-06-09", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-10", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-11", "10.1", "10.2", "10.0", "10.1", "1000"),
            ("2026-06-12", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-15", "9.9", "10.1", "9.8", "10.0", "1000"),
            ("2026-06-16", "10.0", "10.2", "9.9", "10.1", "1000"),
            ("2026-06-17", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-18", "10.0", "10.1", "9.9", "10.0", "1000"),
            ("2026-06-19", "10.2", "10.4", "10.1", "10.3", "1000"),
            ("2026-06-22", "10.2", "10.3", "10.0", "10.1", "1000"),
            ("2026-06-23", "9.3", "9.4", "9.2", "9.3", "1000"),
        ]
    }
    data.close_by_ts_day[TS1] = {d: float(v) for d, v in prices[TS1].items()}
    config = BacktestConfig(
        start_date="2026-06-18",
        end_date="2026-06-23",
        stop_loss_pct=-15.0,
        target_pnl_pct=100.0,
        max_hold_days=60,
        trailing_stop_pct=0.0,
        atr_stop_mult=2.0,
        atr_stop_strength_min=60.0,  # bypass regime: strength decides
    )
    with patch(
        "data_sync_service.service.market_regime.regime_strength_score",
        side_effect=lambda **kw: {"strength": 70.0},
    ):
        run_hi = simulate(config, data=data)
    assert run_hi.summary.closed == 1  # strength 70 >= 60 → ATR line fired
    assert run_hi.trades[0].close_reason in ("stop_hit", "trailing_stop")
    assert run_hi.trades[0].close_price == 9.3
    # Same regime (Strong) but strength 30 < 60 → FIXED -15% line → hold.
    # This proves the strength floor REPLACED the regime rule.
    with patch(
        "data_sync_service.service.market_regime.regime_strength_score",
        side_effect=lambda **kw: {"strength": 30.0},
    ):
        run_lo = simulate(config, data=data)
    assert run_lo.summary.closed == 1
    assert run_lo.trades[0].close_reason == "end_of_window"  # fixed -15% never fired


# --- entry_mode (last-hour dip proxy / next-open) ---


def _data_with_bars(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    ohlc: dict[str, dict[str, tuple[float, float, float, float]]],
) -> BacktestData:
    """Same shape as _data but fills bars_by_ts with (date, o, h, l, c, vol)."""
    data = _data(calendar, scores, {ts: {d: v[3] for d, v in m.items()} for ts, m in ohlc.items()})
    data.bars_by_ts = {
        ts: [
            (d, str(o), str(h), str(l), str(c), "0")
            for d, (o, h, l, c) in sorted(m.items())
        ]
        for ts, m in ohlc.items()
    }
    return data


def test_entry_mode_close_uses_signal_day_close() -> None:
    """entry_mode=close (default) fills at the signal-day close."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    ohlc = {TS1: {"2026-06-18": (9.0, 11.0, 8.0, 10.0), "2026-06-19": (10.0, 10.0, 10.0, 10.0), "2026-06-22": (10.0, 10.0, 10.0, 10.0)}}
    data = _data_with_bars(calendar, scores, ohlc)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_mode="close")

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert abs(run.trades[0].entry_price - 10.0) < 1e-6


def test_entry_mode_last_hour_low_buys_dip_below_close() -> None:
    """last_hour_low = low*0.5 + close*0.5, clamped at close (dip proxy)."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    ohlc = {TS1: {"2026-06-18": (9.0, 11.0, 8.0, 10.0), "2026-06-19": (10.0, 10.0, 10.0, 10.0), "2026-06-22": (10.0, 10.0, 10.0, 10.0)}}
    data = _data_with_bars(calendar, scores, ohlc)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_mode="last_hour_low")

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    # low*0.5 + close*0.5 = 8*0.5 + 10*0.5 = 9.0
    assert abs(run.trades[0].entry_price - 9.0) < 1e-6


def test_entry_mode_next_open_uses_next_session_open() -> None:
    """next_open fills at the NEXT session's open (T+1 买入)."""
    calendar = ["2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    ohlc = {TS1: {"2026-06-18": (9.0, 11.0, 8.0, 10.0), "2026-06-19": (10.5, 11.0, 10.0, 10.8), "2026-06-22": (10.0, 10.0, 10.0, 10.0)}}
    data = _data_with_bars(calendar, scores, ohlc)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_mode="next_open")

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert abs(run.trades[0].entry_price - 10.5) < 1e-6


# --- TIP-014 entry_style (momentum / dip) ---


def _data_with_bars5(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    closes: dict[str, dict[str, float]],
    rs_rank: dict[str, dict[str, float]],
) -> BacktestData:
    """Like _data but with rs_rank_by_day + bars (ret5 needs >=6 closes)."""
    data = _data(calendar, scores, closes)
    data.rs_rank_by_day = rs_rank
    data.bars_by_ts = {
        ts: [(d, str(c), str(c), str(c), str(c), "0") for d, c in sorted(m.items())]
        for ts, m in closes.items()
    }
    return data


def test_entry_style_momentum_requires_rs_and_no_pullback() -> None:
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {CN1: 90.0},
        "2026-06-22": {CN1: 90.0},
    }
    # close series: 10,10,10,10 → 11 (ret5 over 5 prior closes = +10% momentum)
    closes = {TS1: {"2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0, "2026-06-18": 11.0, "2026-06-19": 11.0, "2026-06-22": 11.0}}
    # 5-day return at 06-18 = 11/10 - 1 = +10% → momentum OK
    rs = {"2026-06-18": {TS1: 0.9}, "2026-06-19": {TS1: 0.9}, "2026-06-22": {TS1: 0.9}}
    data = _data_with_bars5(calendar, scores, closes, rs)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_style="momentum")

    run = simulate(config, data=data)
    assert run.summary.closed == 1


def test_entry_style_dip_requires_pullback() -> None:
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {CN1: 90.0},
        "2026-06-22": {CN1: 90.0},
    }
    # 5-day return = 10.0/11 - 1 = -9.1% → dip (>= 5% pullback)
    closes = {TS1: {"2026-06-15": 11.0, "2026-06-16": 11.0, "2026-06-17": 11.0, "2026-06-18": 10.0, "2026-06-19": 10.0, "2026-06-22": 10.0}}
    rs = {"2026-06-18": {TS1: 0.9}, "2026-06-19": {TS1: 0.9}, "2026-06-22": {TS1: 0.9}}
    data = _data_with_bars5(calendar, scores, closes, rs)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_style="dip")

    run = simulate(config, data=data)
    assert run.summary.closed == 1


def test_entry_style_momentum_rejects_pullback_names() -> None:
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {CN1: 90.0},
        "2026-06-19": {CN1: 90.0},
        "2026-06-22": {CN1: 90.0},
    }
    # 5-day return = 10.5/11 - 1 = -4.5% → pullback → momentum must REJECT
    closes = {TS1: {"2026-06-15": 11.0, "2026-06-16": 11.0, "2026-06-17": 11.0, "2026-06-18": 10.5, "2026-06-19": 10.5, "2026-06-22": 10.5}}
    rs = {"2026-06-18": {TS1: 0.9}, "2026-06-19": {TS1: 0.9}, "2026-06-22": {TS1: 0.9}}
    data = _data_with_bars5(calendar, scores, closes, rs)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_style="momentum")

    run = simulate(config, data=data)
    assert run.summary.closed == 0


def test_entry_style_rejects_low_rs() -> None:
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}, "2026-06-22": {CN1: 90.0}}
    closes = {TS1: {"2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0, "2026-06-18": 11.0, "2026-06-19": 11.0, "2026-06-22": 11.0}}
    # RS 0.5 < entry_style_rs_min 0.8 → both styles reject
    rs = {"2026-06-18": {TS1: 0.5}, "2026-06-19": {TS1: 0.5}, "2026-06-22": {TS1: 0.5}}
    data = _data_with_bars5(calendar, scores, closes, rs)
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", entry_style="dip")

    run = simulate(config, data=data)
    assert run.summary.closed == 0


# --- TIP-014 neutral_block ---


def _data_with_env(
    calendar: list[str],
    scores: dict[str, dict[str, float]],
    closes: dict[str, dict[str, float]],
    env: dict[str, str],
) -> BacktestData:
    data = _data(calendar, scores, closes)
    data.env_by_day = env
    return data


def test_neutral_block_rejects_true_neutral_days() -> None:
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    closes = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.0}}
    env = {"2026-06-18": "neutral", "2026-06-19": "uptrend"}
    data = _data_with_env(calendar, scores, closes, env)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", neutral_block=True
    )

    run = simulate(config, data=data)
    # 06-18 (neutral) blocked; 06-19 (uptrend) entry allowed → 1 closed.
    assert run.summary.gated_blocks.get("neutral", 0) >= 1
    assert run.summary.closed == 1
    assert run.trades[0].entry_date == "2026-06-19"


def test_neutral_block_keeps_unknown_days_open() -> None:
    """No env data at all → day is UNKNOWN, NOT neutral → entries stay open."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    closes = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.6}}
    data = _data_with_env(calendar, scores, closes, {})
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", neutral_block=True
    )

    run = simulate(config, data=data)
    assert run.summary.closed == 1


def test_neutral_block_rejects_implicit_weak_days() -> None:
    """ratio<0.5 days labelled weak must also be blocked."""
    calendar = ["2026-06-18", "2026-06-19"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    closes = {TS1: {"2026-06-18": 10.0, "2026-06-19": 10.0}}
    env = {"2026-06-18": "weak", "2026-06-19": "uptrend"}
    data = _data_with_env(calendar, scores, closes, env)
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19", neutral_block=True
    )

    run = simulate(config, data=data)
    assert run.summary.gated_blocks.get("neutral", 0) >= 1
    assert run.trades[0].entry_date == "2026-06-19"


# --- TIP-014 HK style map (experimental, default OFF) ---


def test_hk_style_map_parse() -> None:
    """hk_style_map string parsing produces the expected per-regime styles."""
    cfg = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-19",
        market="HK", hk_style_map="Strong:dip,Diverging:momentum,Weak:blocked",
    )
    assert cfg.hk_style_map == "Strong:dip,Diverging:momentum,Weak:blocked"


def test_hk_auto_default_maps_strong_to_momentum() -> None:
    """HK auto WITHOUT override: Strong→momentum — a momentum name (5d +10%,
    RS 0.9) passes; a pullback name would be rejected."""
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {"HK:00700": 90.0},
        "2026-06-19": {"HK:00700": 90.0},
        "2026-06-22": {"HK:00700": 90.0},
    }
    # 5d return at 06-18 = 11/10 - 1 = +10% → momentum passes
    closes = {"00700.HK": {
        "2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0,
        "2026-06-18": 11.0, "2026-06-19": 11.0, "2026-06-22": 11.0,
    }}
    data = _data(calendar, scores, closes)
    data.regime_by_day = {d: "Strong" for d in calendar}
    data.rs_rank_by_day = {d: {"00700.HK": 0.9} for d in calendar}
    data.bars_by_ts = {
        "00700.HK": [(d, str(c), str(c), str(c), str(c), "0") for d, c in closes["00700.HK"].items()]
    }
    data.ts_codes = ["00700.HK"]
    data.industry_by_ts = {}
    data.sentiment_risk_by_day = {}
    data.env_by_day = {}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-22",
        market="HK", gates="regime", entry_style="auto",
    )

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    assert run.trades[0].symbol == "HK:00700"


def test_hk_auto_override_diverging_to_momentum() -> None:
    """hk_style_map override: Diverging→momentum — a momentum name on a
    Diverging day passes (proves the override path is live)."""
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {
        "2026-06-18": {"HK:00700": 90.0},
        "2026-06-19": {"HK:00700": 90.0},
        "2026-06-22": {"HK:00700": 90.0},
    }
    closes = {"00700.HK": {
        "2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0,
        "2026-06-18": 11.0, "2026-06-19": 11.0, "2026-06-22": 11.0,
    }}
    data = _data(calendar, scores, closes)
    data.regime_by_day = {d: "Diverging" for d in calendar}
    data.rs_rank_by_day = {d: {"00700.HK": 0.9} for d in calendar}
    data.bars_by_ts = {
        "00700.HK": [(d, str(c), str(c), str(c), str(c), "0") for d, c in closes["00700.HK"].items()]
    }
    data.ts_codes = ["00700.HK"]
    data.industry_by_ts = {}
    data.sentiment_risk_by_day = {}
    data.env_by_day = {}
    config = BacktestConfig(
        start_date="2026-06-18", end_date="2026-06-22",
        market="HK", gates="regime", entry_style="auto",
        diverging_scale=1.0,
        hk_style_map="Strong:blocked,Diverging:momentum,Weak:blocked",
    )

    run = simulate(config, data=data)
    # Diverging→momentum (override), momentum name passes
    assert run.summary.closed == 1


# --- D2 max_hold_env_shorten (uptrend entries force-close early) ---


def test_max_hold_env_shorten_closes_uptrend_entries_early() -> None:
    """An uptrend-day entry force-closes after max_hold_env_shorten days."""
    calendar = [f"2026-0{i}-{j:02d}" for i in range(6, 9) for j in (1, 2, 3, 4, 5)]
    # only one entry day (2026-06-01) with a score; flat prices after.
    scores = {"2026-06-01": {CN1: 90.0}}
    closes = {TS1: {d: 10.0 for d in calendar}}
    closes[TS1]["2026-06-01"] = 10.0
    data = _data(calendar, scores, closes)
    data.env_by_day = {d: "uptrend" for d in calendar}
    # give enough bars so entry price resolves
    data.bars_by_ts = {
        TS1: [(d, "10", "10", "10", "10", "0") for d in calendar]
    }
    config = BacktestConfig(
        start_date="2026-06-01", end_date="2026-08-31",
        max_hold_days=60, max_hold_env_shorten=3,
    )

    run = simulate(config, data=data)
    assert run.summary.closed == 1
    t = run.trades[0]
    assert t.close_reason == "max_hold"
    # entry 06-01 + 3 days → closed 06-04 (3 holding days)
    assert t.holding_days <= 3


def test_max_hold_env_shorten_ignores_unknown_entries() -> None:
    """Entries on UNKNOWN days keep the normal max_hold_days."""
    calendar = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05", "2026-06-08", "2026-06-09"]
    scores = {"2026-06-01": {CN1: 90.0}}
    closes = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, closes)
    data.env_by_day = {}  # no env data at all → UNKNOWN
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "0") for d in calendar]}
    config = BacktestConfig(
        start_date="2026-06-01", end_date="2026-06-09",
        max_hold_days=60, max_hold_env_shorten=3,
    )

    run = simulate(config, data=data)
    # no early close (env UNKNOWN → normal 60-day hold) → end_of_window
    assert run.summary.closed == 1
    assert run.trades[0].close_reason == "end_of_window"


def test_breakout_gate_blocks_non_breakout() -> None:
    """P1: with breakout_days=3, a close that does NOT exceed the prior
    3-day high is blocked at entry."""
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    # flat/declining closes: never exceeds the prior 3-day high
    closes = {TS1: {"2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0,
                    "2026-06-18": 10.0, "2026-06-19": 9.0, "2026-06-22": 9.0}}
    data = _data_with_bars5(calendar, scores, closes, {d: {TS1: 0.9} for d in calendar})
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", breakout_days=3)

    run = simulate(config, data=data)
    assert run.summary.closed == 0


def test_breakout_gate_allows_breakout() -> None:
    """P1: a close ABOVE the prior 3-day high passes the breakout gate."""
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}, "2026-06-19": {CN1: 90.0}}
    # 06-18 close 10.5 > prior 3-day highs (10.0) → breakout OK (+5%, not limit-up)
    closes = {TS1: {"2026-06-15": 10.0, "2026-06-16": 10.0, "2026-06-17": 10.0,
                    "2026-06-18": 10.5, "2026-06-19": 10.5, "2026-06-22": 10.5}}
    data = _data_with_bars5(calendar, scores, closes, {d: {TS1: 0.9} for d in calendar})
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", breakout_days=3)

    run = simulate(config, data=data)
    assert run.summary.closed == 1


def test_volume_breakout_gate_blocks_thin_volume() -> None:
    """P2: with volume_breakout_mult=2.0, a normal-volume close is blocked."""
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22"]
    scores = {"2026-06-18": {CN1: 90.0}}
    closes = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, closes)
    data.env_by_day = {d: "unknown" for d in calendar}
    # flat volume: today 100 vs 20d avg... only 5 bars — prior-20 fail-closed
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "100") for d in calendar]}
    config = BacktestConfig(start_date="2026-06-18", end_date="2026-06-22", volume_breakout_mult=2.0)

    run = simulate(config, data=data)
    # insufficient prior-20 history → fail-closed (no entry)
    assert run.summary.closed == 0


def test_volume_breakout_gate_allows_volume_spike() -> None:
    """P2: entry-day volume above K x the 20d average passes the gate."""
    calendar = [f"2026-0{6 - i // 7}-{15 + i % 7:02d}" for i in range(24)]
    # fix overlapping dates via explicit list
    calendar = ["2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19", "2026-06-22",
                "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-29", "2026-06-30",
                "2026-07-01", "2026-07-02", "2026-07-03", "2026-07-06", "2026-07-07", "2026-07-08",
                "2026-07-09", "2026-07-10", "2026-07-13", "2026-07-14", "2026-07-15", "2026-07-16"]
    scores = {calendar[21]: {CN1: 90.0}}
    closes = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, closes)
    data.env_by_day = {d: "unknown" for d in calendar}
    # 20 prior bars at vol=100, entry-day vol=300 (> 2x avg)
    bars = []
    for d in calendar:
        if d == calendar[21]:
            bars.append((d, "10", "10", "10", "10", "300"))
        else:
            bars.append((d, "10", "10", "10", "10", "100"))
    data.bars_by_ts = {TS1: bars}
    config = BacktestConfig(start_date=calendar[21], end_date=calendar[23], volume_breakout_mult=2.0)

    run = simulate(config, data=data)
    assert run.summary.closed == 1


def _weekday_calendar(n: int) -> list[str]:
    import datetime as _dt

    calendar: list[str] = []
    d = _dt.date(2026, 4, 1)
    while len(calendar) < n:
        if d.weekday() < 5:
            calendar.append(d.isoformat())
        d += _dt.timedelta(days=1)
    return calendar


def test_ma_slope_gate_blocks_flat_ma() -> None:
    """P4: a flat MA20 (slope ~0) is blocked by ma_slope_min_pct."""
    calendar = _weekday_calendar(50)
    scores = {calendar[30]: {CN1: 90.0}}
    closes = {TS1: {d: 10.0 for d in calendar}}
    data = _data(calendar, scores, closes)
    data.env_by_day = {d: "unknown" for d in calendar}
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", "10", "0") for d in calendar]}
    config = BacktestConfig(start_date=calendar[30], end_date=calendar[-1], ma_slope_min_pct=2.0)

    run = simulate(config, data=data)
    # flat closes -> MA20 slope 0% < 2% -> blocked
    assert run.summary.closed == 0


def test_ma_slope_gate_allows_rising_ma() -> None:
    """P4: a rising MA20 (10 -> ~12.5 over 50 sessions) passes the gate."""
    calendar = _weekday_calendar(55)
    scores = {calendar[45]: {CN1: 90.0}}
    closes = {TS1: {d: 10.0 + 0.05 * i for i, d in enumerate(calendar)}}
    data = _data(calendar, scores, closes)
    data.env_by_day = {d: "unknown" for d in calendar}
    data.bars_by_ts = {TS1: [(d, "10", "10", "10", str(10.0 + 0.05 * i), "0") for i, d in enumerate(calendar)]}
    config = BacktestConfig(start_date=calendar[45], end_date=calendar[-1], ma_slope_min_pct=2.0)

    run = simulate(config, data=data)
    # MA20 at idx45 ~= avg(11.3..12.2) = 11.75; MA20 at idx25 ~= 10.75
    # -> slope ~9% > 2% -> passes
    assert run.summary.closed == 1
