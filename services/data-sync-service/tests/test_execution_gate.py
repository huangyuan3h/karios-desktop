"""Tests for Execution Gate (ATTACK / WEAK_ATTACK / HOLD_ONLY / DEFEND)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from data_sync_service.service.execution_gate import (
    MODE_ATTACK,
    MODE_DEFEND,
    MODE_HOLD_ONLY,
    MODE_WEAK_ATTACK,
    OVERFLOW_INFLOW_THRESHOLD_CNY,
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


def _sh(hour: int, minute: int) -> datetime:
    return datetime(2026, 7, 27, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai"))


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


def test_diverging_allows_entries() -> None:
    # S-3 定案：Diverging = 进攻（diverging_scale=1.0 满仓），与回测/paper_s3 同口径
    out = compute_execution_gate(
        index_signals=_signals("green", "red"),
        down_count=1500,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["allowNewEntries"] is True
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


def test_v63_overflow_upgrades_srv_extreme_to_weak_attack() -> None:
    """726亿 electronics + upCount 4100 at 14:31 → WEAK_ATTACK despite SRV Extreme_High."""
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 0),
        max_sector_inflow_cny=726e8,
        overflow_sector="电子",
        now=_sh(14, 31),
    )
    assert out["mode"] == MODE_WEAK_ATTACK
    assert out["allowNewEntries"] is True
    assert "INTRADAY_OVERFLOW_OVERRIDE" in out["reasons"]
    assert "SRV_EXTREME_HIGH" in out["reasons"]
    assert out["overflowSector"] == "电子"
    assert out["overflowInflowYi"] == 726.0


def test_v63_overflow_before_1430_stays_defend() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 0),
        max_sector_inflow_cny=726e8,
        overflow_sector="电子",
        now=_sh(14, 0),
    )
    assert out["mode"] == MODE_DEFEND
    assert out["allowNewEntries"] is False
    assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


def test_v63_overflow_does_not_override_breadth_panic() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=3200,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 0),
        max_sector_inflow_cny=OVERFLOW_INFLOW_THRESHOLD_CNY + 1e8,
        overflow_sector="电子",
        now=_sh(14, 31),
    )
    assert out["mode"] == MODE_DEFEND
    assert "BREADTH_PANIC" in out["reasons"]
    assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


def test_v63_overflow_irrelevant_when_diverging_attacks() -> None:
    # Diverging 现在本身就是 ATTACK（回测口径），overflow override 不再需要
    out = compute_execution_gate(
        index_signals=_signals("green", "red"),
        down_count=1000,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        max_sector_inflow_cny=550e8,
        overflow_sector="半导体",
        now=_sh(14, 30),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["allowNewEntries"] is True
    assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


def test_v63_overflow_does_not_downgrade_attack() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=800,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        max_sector_inflow_cny=726e8,
        overflow_sector="电子",
        now=_sh(14, 31),
    )
    assert out["mode"] == MODE_ATTACK
    assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


def test_v63_overflow_requires_up_count() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        up_count=3999,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 0),
        max_sector_inflow_cny=726e8,
        now=_sh(14, 31),
    )
    assert out["mode"] == MODE_DEFEND
    assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


def _cn_hk_signals(sse, cyb, zz500, hsi, hstech) -> list[dict]:
    return [
        {"name": "上证指数", "signal": sse, "positionRange": "50%-60%"},
        {"name": "创业板指", "signal": cyb, "positionRange": "50%-60%"},
        {"name": "中证500", "signal": zz500, "positionRange": "50%-60%"},
        {"name": "恒生指数", "signal": hsi, "positionRange": "50%-60%"},
        {"name": "恒生科技指数", "signal": hstech, "positionRange": "50%-60%"},
    ]


def test_cn_regime_requires_all_three_cn_lights_green() -> None:
    # Two of three green → Diverging, not Strong.
    out = compute_execution_gate(
        index_signals=_cn_hk_signals("green", "red", "green", "green", "green"),
        down_count=900,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["marketRegime"] == REGIME_DIVERGING
    assert out["mode"] == MODE_ATTACK


def test_hk_gate_independent_of_cn_when_hk_strong() -> None:
    # CN diverging (one red) → CN gate ATTACK (S-3: Diverging 允许开仓), HK both green → ATTACK.
    out = compute_execution_gate(
        index_signals=_cn_hk_signals("green", "red", "yellow", "green", "green"),
        down_count=900,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["hkGate"]["marketRegime"] == REGIME_STRONG
    assert out["hkGate"]["mode"] == MODE_ATTACK
    assert out["hkGate"]["allowNewEntries"] is True
    assert out["cnGate"]["mode"] == out["mode"]


def test_hk_gate_defends_when_hk_weak_even_if_cn_strong() -> None:
    out = compute_execution_gate(
        index_signals=_cn_hk_signals("green", "green", "green", "red", "yellow"),
        down_count=500,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_ATTACK
    assert out["hkGate"]["marketRegime"] == REGIME_WEAK
    assert out["hkGate"]["mode"] == MODE_DEFEND
    assert out["hkGate"]["allowNewEntries"] is False


def test_hk_gate_defends_on_global_risk_even_when_hk_strong() -> None:
    out = compute_execution_gate(
        index_signals=_cn_hk_signals("green", "green", "green", "green", "green"),
        down_count=500,
        risk_mode="extreme_caution",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["hkGate"]["mode"] == MODE_DEFEND
    assert "RISK_EXTREME_CAUTION" in out["hkGate"]["reasons"]


def test_hk_position_range_hint_removed_by_backtest() -> None:
    # 2026-08-12 (OPT-093): replaying 2024-08~2026-08 shows HK index lights
    # carry no separation for S-3 entries (medians red -5.0% / yellow -2.0% /
    # green -5.1%; the red mean is right-tail driven). The heuristic
    # "red → 0-10%" was deleted — hint is always None for HK now.
    sigs = [
        {"name": "上证指数", "signal": "green", "positionRange": "50%-60%"},
        {"name": "创业板指", "signal": "green", "positionRange": "50%-60%"},
        {"name": "中证500", "signal": "green", "positionRange": "50%-60%"},
        {"name": "恒生指数", "signal": "deep_green", "positionRange": "80%-100%"},
        {"name": "恒生科技指数", "signal": "yellow", "positionRange": "30%"},
    ]
    out = compute_execution_gate(
        index_signals=sigs,
        down_count=100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["hkGate"]["indexLight"] == "yellow"
    assert out["hkGate"]["positionRangeHint"] is None


# ---------- V6.4 ETF flow confirmation layer ----------

def _etf(verdict: str, *, incomplete: bool = False) -> dict:
    return {
        "verdict": verdict,
        "broadDirection": "buy" if verdict == "confirm" else "outflow",
        "sectorDirection": "neutral",
        "incomplete": incomplete,
    }


def test_etf_confirm_keeps_attack_with_reason() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        etf_flow_signal=_etf("confirm"),
    )
    assert out["mode"] == MODE_ATTACK
    assert "ETF_FLOW_CONFIRM" in out["reasons"]
    assert "ETF_FLOW_CONTRADICT" not in out["reasons"]


def test_etf_contradict_downgrades_attack_to_hold_only() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        etf_flow_signal=_etf("contradict"),
    )
    assert out["mode"] == MODE_HOLD_ONLY
    assert out["allowNewEntries"] is False
    assert "ETF_FLOW_CONTRADICT" in out["reasons"]


def test_etf_contradict_never_upgrades_or_moves_hard_defend() -> None:
    # DEFEND base (SRV extreme high) stays DEFEND; contradict is informational.
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 1),
        etf_flow_signal=_etf("contradict"),
    )
    assert out["mode"] == MODE_DEFEND
    assert "ETF_FLOW_CONTRADICT" in out["reasons"]


def test_etf_contradict_does_not_downgrade_weak_attack() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "green"),
        down_count=1000,
        up_count=4100,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_EXTREME_HIGH, 0),
        max_sector_inflow_cny=726e8,
        overflow_sector="电子",
        now=_sh(14, 31),
        etf_flow_signal=_etf("contradict"),
    )
    assert out["mode"] == MODE_WEAK_ATTACK
    assert "ETF_FLOW_CONTRADICT" in out["reasons"]


def test_etf_incomplete_signal_is_ignored() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        etf_flow_signal=_etf("contradict", incomplete=True),
    )
    assert out["mode"] == MODE_ATTACK
    assert "ETF_FLOW_CONTRADICT" not in out["reasons"]
    assert "ETF_FLOW_CONFIRM" not in out["reasons"]
    assert out["etfFlowSignal"]["incomplete"] is True


def test_etf_neutral_adds_no_reason() -> None:
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        etf_flow_signal=_etf("neutral"),
    )
    assert out["mode"] == MODE_ATTACK
    assert "ETF_FLOW_CONFIRM" not in out["reasons"]
    assert "ETF_FLOW_CONTRADICT" not in out["reasons"]
    assert out["etfFlowSignal"]["verdict"] == "neutral"


def test_etf_signal_in_cn_gate_not_hk_gate() -> None:
    out = compute_execution_gate(
        index_signals=_cn_hk_signals("green", "green", "green", "green", "green"),
        down_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
        etf_flow_signal=_etf("confirm"),
    )
    assert out["etfFlowSignal"]["verdict"] == "confirm"
    assert out["cnGate"]["etfFlowSignal"]["verdict"] == "confirm"
    assert "etfFlowSignal" not in out["hkGate"]


def test_defend_when_implicit_weak_breadth_ratio() -> None:
    """TIP-014: up/down < 0.5 with normal risk_mode still defends."""
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=3200,
        up_count=800,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_DEFEND
    assert out["allowNewEntries"] is False
    assert "BREADTH_IMPLICIT_WEAK" in out["reasons"]


def test_attack_when_ratio_above_weak_threshold() -> None:
    """up/down = 1.0 (balanced) with normal risk_mode stays ATTACK."""
    out = compute_execution_gate(
        index_signals=_signals("green", "deep_green"),
        down_count=2000,
        up_count=2000,
        risk_mode="normal",
        srv_index=_srv(SRV_LEVEL_STABLE, 3),
    )
    assert out["mode"] == MODE_ATTACK
