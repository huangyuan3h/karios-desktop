"""core_holding_audit S-3 judges — pure unit tests, no DB."""

from __future__ import annotations

from data_sync_service.service import core_holding_audit as audit


def _op(symbol="CN:688525", side="BUY", date="2026-09-07", price=221.28, pct=12.5, leg="s3"):
    return {
        "id": "x",
        "symbol": symbol,
        "side": side,
        "trade_date": date,
        "price": price,
        "position_pct": pct,
        "cost_basis": None,
        "entry_date": None,
        "pnl_pct": None,
        "holding_days": None,
        "source": "RESEARCH",
        "market": "CN",
        "note": None,
        "leg": leg,
        "created_at": None,
    }


def test_judge_add_pyramid_trigger_ok() -> None:
    v = audit._judge_add(
        _op(side="ADD", price=102.5, pct=5),
        {"cost": 100.0, "pct": 10.0},
        {"regime": "Strong", "panicActive": False, "gateOpen": True},
    )
    assert v["verdict"] == "ok"
    assert v["rule"] == "pyramid"


def test_judge_add_early_or_oversized_warns() -> None:
    early = audit._judge_add(
        _op(side="ADD", price=101.0, pct=5),
        {"cost": 100.0, "pct": 10.0},
        {"regime": "Strong", "panicActive": False, "gateOpen": True},
    )
    assert early["verdict"] == "warn"
    oversized = audit._judge_add(
        _op(side="ADD", price=103.0, pct=20),
        {"cost": 100.0, "pct": 10.0},
        {"regime": "Strong", "panicActive": False, "gateOpen": True},
    )
    assert oversized["verdict"] == "warn"


def test_judge_add_etf_uses_ma200() -> None:
    above = audit._judge_add(
        _op(symbol="ETF:513100", side="ADD", price=2.2, pct=5),
        {"cost": 2.0, "pct": 10.0, "ma200": 1.9, "aboveMa200": True},
        {"regime": "Weak", "panicActive": True, "gateOpen": False},
    )
    assert above["verdict"] == "ok"
    assert above["rule"] == "sleeve"
    below = audit._judge_add(
        _op(symbol="ETF:513100", side="ADD", price=2.2, pct=5),
        {"cost": 2.0, "pct": 10.0, "ma200": 2.5, "aboveMa200": False},
        {"regime": "Weak", "panicActive": True, "gateOpen": False},
    )
    assert below["verdict"] == "warn"


def test_judge_sell_stop_execution_vs_discretionary() -> None:
    gate = {"regime": "Strong", "panicActive": False, "gateOpen": True}
    stop = audit._judge_sell(
        _op(side="SELL", price=95.0, pct=10), {"cost": 100.0, "pct": 10.0}, gate
    )
    assert stop["verdict"] == "ok" and stop["rule"] == "stop"
    disc = audit._judge_sell(
        _op(side="SELL", price=99.0, pct=10), {"cost": 100.0, "pct": 10.0}, gate
    )
    assert disc["verdict"] == "warn" and disc["rule"] == "discretionary"


def test_judge_open_regime_gate() -> None:
    open_v = audit._judge_open(
        _op(price=10.0, pct=10),
        {"cost": 0.0, "pct": 0.0},
        {"regime": "Diverging", "panicActive": False, "gateOpen": True},
    )
    assert open_v["verdict"] == "ok"
    closed_v = audit._judge_open(
        _op(price=10.0, pct=10),
        {"cost": 0.0, "pct": 0.0},
        {"regime": "Weak", "panicActive": True, "gateOpen": False},
    )
    assert closed_v["verdict"] == "warn"
    etf_v = audit._judge_open(
        _op(symbol="ETF:513100", price=2.0, pct=10),
        {"cost": 0.0, "pct": 0.0},
        {"regime": "Weak", "panicActive": True, "gateOpen": False},
    )
    assert etf_v["verdict"] == "ok"


def test_replay_verdicts_carry_row_ids() -> None:
    """OPT-150: the UI needs op ids to patch a misfiled leg."""
    ops = [_op(side="BUY"), _op(side="SELL", date="2026-09-09", price=230.0)]
    out = audit._replay_ops(
        ops,
        {"costPrice": 221.28, "positionPct": 12.5},
        {"regime": "", "panicActive": False, "gateOpen": False},
    )
    assert [v.get("id") for v in out] == ["x", "x"]


def test_replay_judges_s3_and_skips_parking_legs() -> None:
    ops = [
        _op(side="BUY", leg="parking"),
        _op(side="ADD", date="2026-09-08", price=230.0, pct=5, leg="s3"),
    ]
    gate = {"regime": "Diverging", "panicActive": False, "gateOpen": True}
    out = audit._replay_ops(ops, {"costPrice": 221.28, "positionPct": 12.5}, gate)
    assert len(out) == 1
    assert out[0]["side"] == "ADD"
    assert "parking" not in [op.get("leg") for op in out]
