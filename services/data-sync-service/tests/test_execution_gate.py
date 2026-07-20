"""Tests for Execution Gate (ATTACK / HOLD_ONLY / DEFEND)."""

from __future__ import annotations

from data_sync_service.service.execution_gate import (
    MODE_ATTACK,
    MODE_DEFEND,
    MODE_HOLD_ONLY,
    REGIME_DIVERGING,
    REGIME_STRONG,
    REGIME_WEAK,
    compute_execution_gate,
)
from data_sync_service.service.sector_rotation_index import (
    SRV_LEVEL_ELEVATED,
    SRV_LEVEL_EXTREME_HIGH,
    SRV_LEVEL_STABLE,
)


def _signals(sse: str, cyb: str) -> list[dict]:
    return [
        {"name": "上证指数", "signal": sse, "positionRange": "50%-60%"},
        {"name": "创业板指", "signal": cyb, "positionRange": "50%-60%"},
    ]


def _srv(level: str | None, overlap: int | None = 3) -> dict:
    return {"level": level, "overlapCount": overlap, "overlapSectors": []}


def test_attack_when_strong_and_srv_stable() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=1200,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["allowNewEntries"] is True
    assert out["marketRegime"] == REGIME_STRONG
    assert "REGIME_STRONG" in out["reasons"]
    assert "SRV_STABLE" in out["reasons"]


def test_attack_when_strong_and_srv_unknown() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=800,
        risk_mode="hot",
        srv_index=_srv(None, None),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["allowNewEntries"] is True
    assert "SRV_UNKNOWN" in out["reasons"]


def test_defend_when_srv_extreme_high() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 1),
    )
    assert out["mode"] == MODE_DEFEND
    assert out["allowNewEntries"] is False
    assert "SRV_EXTREME_HIGH" in out["reasons"]


def test_defend_when_breadth_panic() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=3200,
        risk_mode="hot",
        srv_index=_srv(SRV_LEVEL_STABLE, 4),
    )
    assert out["mode"] == MODE_DEFEND
    assert out["allowNewEntries"] is False
    assert "BREADTH_PANIC" in out["reasons"]


def test_hold_only_when_diverging() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "red"),
        down_count=1500,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_HOLD_ONLY
    assert out["allowNewEntries"] is False
    assert out["marketRegime"] == REGIME_DIVERGING
    assert "REGIME_DIVERGING" in out["reasons"]


def test_hold_only_when_srv_elevated_even_if_strong() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=900,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_ELEVATED, 2),
    )
    assert out["mode"] == MODE_HOLD_ONLY
    assert out["allowNewEntries"] is False
    assert "SRV_ELEVATED" in out["reasons"]


def test_defend_when_weak_regime() -> None:
    out = compute_execution_gate(
        index_signals=_signals("red", "yellow"),
        down_count=500,
        risk_mode="caution",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_DEFEND
    assert out["marketRegime"] == REGIME_WEAK
    assert "REGIME_WEAK" in out["reasons"]


def test_defend_when_risk_no_new_positions() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=400,
        risk_mode="no_new_positions",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_DEFEND
    assert "RISK_NO_NEW" in out["reasons"]


def test_index_light_is_tighter_of_pair() -> None:
    out = compute_execution_gate(
        index_signals=[
            {"name": "上证指数", "signal": "deep_green", "positionRange": "80%-100%"},
            {"name": "创业板指", "signal": "yellow", "positionRange": "30%"},
        ],
        down_count=100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["indexLight"] == "yellow"
    assert out["positionRangeHint"] == "30%"
