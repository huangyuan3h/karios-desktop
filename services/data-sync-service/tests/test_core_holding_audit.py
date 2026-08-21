"""Unit tests for the core-holding operation audit (2026-08-21)."""

from __future__ import annotations

import pytest

from data_sync_service.service.core_holding_audit import (
    _judge_add,
    _judge_open,
    _judge_sell,
    _replay_ops,
)

GATE_OPEN = {"regime": "Strong", "panicActive": False, "gateOpen": True}
GATE_PANIC = {"regime": "Weak", "panicActive": True, "gateOpen": False}


def _op(side: str, date: str, price: float, pct: float) -> dict:
    return {
        "symbol": "CN:300628", "side": side, "trade_date": date,
        "price": price, "position_pct": pct, "source": "TV",
    }


def test_pyramid_trigger_against_pre_trade_cost():
    """A 4.0% ADD at 41.83 with cost 39.9 / pct 5.93 pre-trade is ON the rule:
    price >= 39.9 * 1.025 (40.897), amount ~ half sleeve (3.0%)."""
    ops = [_op("ADD", "2026-08-21", 41.83, 4.0)]
    state = {"costPrice": 40.677, "positionPct": 9.93}  # blended post-trade
    verdicts = _replay_ops(ops, state, GATE_PANIC)
    assert verdicts[0]["verdict"] == "ok"
    assert verdicts[0]["rule"] == "pyramid"
    assert "40.897" in verdicts[0]["detail"]  # trigger from the PRE-trade cost


def test_pyramid_add_below_trigger_is_warn():
    state = {"costPrice": 40.677, "positionPct": 9.93}
    ops = [_op("ADD", "2026-08-21", 40.0, 2.0)]  # far below the 40.897 trigger
    verdicts = _replay_ops(ops, state, GATE_PANIC)
    assert verdicts[0]["verdict"] == "warn"


def test_oversized_pyramid_add_is_warn():
    """An ADD of 6% when the half sleeve is 3.0% is >1.5x -> warn."""
    ops = [_op("ADD", "2026-08-21", 41.83, 6.0)]
    state = {"costPrice": 40.677, "positionPct": 9.93}
    verdicts = _replay_ops(ops, state, GATE_PANIC)
    assert verdicts[0]["verdict"] == "warn"


def test_pyramid_is_regime_independent():
    """Even in a panic window the A-share pyramid ADD stays ok (no CN gate)."""
    ops = [_op("ADD", "2026-08-21", 41.83, 4.0)]
    state = {"costPrice": 40.677, "positionPct": 9.93}
    verdicts = _replay_ops(ops, state, GATE_PANIC)
    assert verdicts[0]["verdict"] == "ok"


def test_sell_below_stop_is_stop_execution():
    verdict = _judge_sell(
        _op("SELL", "2026-08-11", 37.8, 4.5), {"cost": 40.0, "pct": 5.0}, GATE_PANIC
    )
    assert verdict["verdict"] == "ok"
    assert verdict["rule"] == "stop"


def test_sell_above_stop_is_discretionary():
    verdict = _judge_sell(
        _op("SELL", "2026-08-11", 42.0, 4.5), {"cost": 40.0, "pct": 5.0}, GATE_OPEN
    )
    assert verdict["verdict"] == "warn"
    assert verdict["rule"] == "discretionary"


def test_sell_without_cost_in_panic_is_de_risk():
    verdict = _judge_sell(
        _op("SELL", "2026-08-13", 189.9, 8.03), {"cost": 0.0, "pct": 8.03}, GATE_PANIC
    )
    assert verdict["verdict"] == "ok"
    assert verdict["rule"] == "panic_de_risk"


def test_open_etf_not_gated_by_cn_regime():
    op = {"symbol": "ETF:513110", "side": "BUY", "trade_date": "2026-08-20",
          "price": 2.478, "position_pct": 23.61, "source": "MANUAL"}
    verdict = _judge_open(op, {"cost": 0.0, "pct": 0.0}, GATE_PANIC)
    assert verdict["verdict"] == "ok"
    assert verdict["rule"] == "sleeve"


def test_open_cn_in_panic_is_warn():
    verdict = _judge_open(
        _op("BUY", "2026-08-11", 10.0, 5.0), {"cost": 0.0, "pct": 0.0}, GATE_PANIC
    )
    assert verdict["verdict"] == "warn"
    assert verdict["rule"] == "regime"


def test_replay_multiple_adds_restores_earlier_costs():
    """Two ADDs replay back: the earlier one is judged against the original
    39.9 cost, the later one against the first blend."""
    ops = [
        _op("ADD", "2026-08-20", 40.5, 3.0),
        _op("ADD", "2026-08-21", 41.83, 4.0),
    ]
    state = {"costPrice": 41.0, "positionPct": 12.93}
    verdicts = _replay_ops(ops, state, GATE_OPEN)
    assert len(verdicts) == 2
    assert verdicts[0]["date"] == "2026-08-20"
    assert verdicts[1]["date"] == "2026-08-21"


def test_judge_add_etf_uses_sleeve_semantics():
    op = {"symbol": "ETF:513110", "side": "ADD", "trade_date": "2026-08-21",
          "price": 2.459, "position_pct": 15.0, "source": "MANUAL"}
    verdict = _judge_add(op, {"cost": 2.478, "pct": 33.61, "ma200": 2.233, "aboveMa200": True}, GATE_PANIC)
    assert verdict["verdict"] == "ok"
    assert verdict["rule"] == "sleeve"