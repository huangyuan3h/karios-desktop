"""execution_gate.py remaining branches (non-dict signals / fallbacks / error guards)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from data_sync_service.service.execution_gate import (
    MODE_ATTACK,
    MODE_DEFEND,
    MODE_HOLD_ONLY,
    MODE_WEAK_ATTACK,
    REGIME_DIVERGING,
    REGIME_WEAK,
    _classify_regime,
    _cn_index_signals,
    _fallback_signals,
    _hk_index_signals,
    _hk_satellite_note,
    _overflow_inflow_yi,
    _position_range_hint,
    _position_range_hint_from,
    _shanghai_minutes,
    _tighter_light,
    classify_market_regime,
    compute_execution_gate,
    compute_hk_gate,
    tighter_index_light,
)
from data_sync_service.service.sector_rotation_index import (
    SRV_LEVEL_STABLE,
)

CN = {"name": "上证指数", "signal": "green", "positionRange": "50%-60%"}
CN2 = {"name": "创业板指", "signal": "green", "positionRange": "50%-60%"}
HK = {"name": "恒生指数", "signal": "yellow", "positionRange": "30%"}
HK2 = {"name": "恒生科技指数", "signal": "green", "positionRange": "50%-60%"}


def test_cn_index_signals_skips_non_dict() -> None:
    out = _cn_index_signals([None, "x", CN, CN2])
    assert out == [CN, CN2]


def test_hk_index_signals_skips_non_dict() -> None:
    out = _hk_index_signals([None, CN, HK, HK2])
    assert out == [HK, HK2]


def test_fallback_signals_takes_first_two_dicts() -> None:
    assert _fallback_signals([None, "x", CN, CN2, HK]) == [CN, CN2]


def test_classify_regime_too_few_is_weak() -> None:
    assert _classify_regime([CN]) == REGIME_WEAK
    assert _classify_regime([]) == REGIME_WEAK


def test_classify_market_regime_falls_back_when_less_than_two_cn() -> None:
    assert classify_market_regime([CN, HK]) == REGIME_DIVERGING


def test_tighter_light_empty_is_red() -> None:
    assert _tighter_light([]) == "red"


def test_tighter_index_light_falls_back_when_less_than_two_cn() -> None:
    assert tighter_index_light([CN, HK]) == "yellow"


class TestPositionRangeHint:
    def test_second_pass_takes_any_signal_with_range(self) -> None:
        signals = [
            {"name": "上证指数", "signal": "green", "positionRange": ""},
            {"name": "创业板指", "signal": "red", "positionRange": "0%-10%"},
        ]
        assert _position_range_hint_from(signals, "yellow") == "0%-10%"

    def test_defaults_for_unknown_light(self) -> None:
        assert _position_range_hint_from([], "orange") == "—"
        assert _position_range_hint_from([], "deep_green") == "80%-100%"
        assert _position_range_hint_from([], "red") == "0%-10%"

    def test_falls_back_when_less_than_two_cn(self) -> None:
        signals = [{"name": "上证指数", "signal": "green", "positionRange": "50%-60%"}]
        assert _position_range_hint(signals, "green") == "50%-60%"

    def test_matching_signal_first(self) -> None:
        signals = [
            {"name": "上证指数", "signal": "red", "positionRange": "0%-10%"},
            {"name": "创业板指", "signal": "green", "positionRange": "50%-60%"},
        ]
        assert _position_range_hint_from(signals, "red") == "0%-10%"


def test_hk_satellite_note_weak_attack() -> None:
    assert "5% 先锋仓" in _hk_satellite_note(MODE_WEAK_ATTACK)


class TestShanghaiMinutes:
    def test_none_uses_now(self) -> None:
        m = _shanghai_minutes(None)
        assert 0 <= m <= 1439

    def test_naive_datetime_assumes_shanghai(self) -> None:
        m = _shanghai_minutes(datetime(2026, 7, 27, 14, 30))
        assert m == 870

    def test_aware_datetime_converted(self) -> None:
        m = _shanghai_minutes(datetime(2026, 7, 27, 6, 30, tzinfo=ZoneInfo("UTC")))
        assert m == 870


class TestOverflowInflowYi:
    def test_none(self) -> None:
        assert _overflow_inflow_yi(None) is None

    def test_bad_value(self) -> None:
        assert _overflow_inflow_yi("abc") is None

    def test_ok(self) -> None:
        assert _overflow_inflow_yi(5_000_000_000) == 50.0


class TestDefendPaths:
    def test_breadth_panic_defends(self) -> None:
        out = compute_execution_gate(index_signals=[CN, CN2], down_count=9999)
        assert out["mode"] == MODE_DEFEND
        assert "BREADTH_PANIC" in out["reasons"]

    def test_risk_extreme_caution_defends(self) -> None:
        out = compute_execution_gate(index_signals=[CN, CN2], risk_mode="extreme_caution")
        assert out["mode"] == MODE_DEFEND
        assert "RISK_EXTREME_CAUTION" in out["reasons"]

    def test_srv_extreme_high_defends(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            srv_index={"level": "Extreme_High", "overlapCount": 3},
        )
        assert out["mode"] == MODE_DEFEND
        assert "SRV_EXTREME_HIGH" in out["reasons"]

    def test_elevated_srv_hold_only(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            srv_index={"level": "Elevated", "overlapCount": 3},
        )
        assert out["mode"] == MODE_HOLD_ONLY
        assert "SRV_ELEVATED" in out["reasons"]

    def test_weak_regime_defends(self) -> None:
        out = compute_execution_gate(
            index_signals=[
                {"name": "上证指数", "signal": "red", "positionRange": "0%-10%"},
                {"name": "创业板指", "signal": "red", "positionRange": "0%-10%"},
            ]
        )
        assert out["mode"] == MODE_DEFEND
        assert "REGIME_WEAK" in out["reasons"]

    def test_diverging_attacks(self) -> None:
        # S-3 定案：Diverging = 进攻（diverging_scale=1.0 满仓），与回测/paper_s3 同口径
        out = compute_execution_gate(index_signals=[CN, {"name": "创业板指", "signal": "red"}])
        assert out["mode"] == MODE_ATTACK
        assert "REGIME_DIVERGING" in out["reasons"]

    def test_diverging_with_elevated_srv_hold_only(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, {"name": "创业板指", "signal": "red"}],
            srv_index={"level": "Elevated", "overlapCount": 3},
        )
        assert out["mode"] == MODE_HOLD_ONLY
        assert "SRV_ELEVATED" in out["reasons"]


class TestOverflowOverride:
    def test_overflow_irrelevant_when_diverging_attacks(self) -> None:
        # Diverging 本身 ATTACK（回测口径），overflow 不再升级为 WEAK_ATTACK
        out = compute_execution_gate(
            index_signals=[CN, {"name": "创业板指", "signal": "red"}],
            up_count=5000,
            max_sector_inflow_cny=600e8,
            overflow_sector="半导体",
            now=datetime(2026, 7, 27, 14, 45, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        assert out["mode"] == MODE_ATTACK
        assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]
        assert out["overflowSector"] == "半导体"
        assert out["overflowInflowYi"] == 600.0

    def test_overflow_requires_unlock_time(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, {"name": "创业板指", "signal": "red"}],
            up_count=5000,
            max_sector_inflow_cny=600e8,
            now=datetime(2026, 7, 27, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        assert out["mode"] != MODE_WEAK_ATTACK

    def test_overflow_not_applied_on_attack(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            up_count=5000,
            max_sector_inflow_cny=600e8,
            now=datetime(2026, 7, 27, 14, 45, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
        assert out["mode"] == MODE_ATTACK
        assert "INTRADAY_OVERFLOW_OVERRIDE" not in out["reasons"]


class TestEtfFlowLayer:
    def test_confirm_adds_reason(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            etf_flow_signal={"verdict": "confirm", "broadDirection": "up"},
        )
        assert "ETF_FLOW_CONFIRM" in out["reasons"]

    def test_contradict_downgrades_attack(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            etf_flow_signal={"verdict": "contradict", "broadDirection": "down"},
        )
        assert out["mode"] == MODE_HOLD_ONLY
        assert "ETF_FLOW_CONTRADICT" in out["reasons"]

    def test_contradict_no_effect_on_hold_only(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, {"name": "创业板指", "signal": "red"}],
            etf_flow_signal={"verdict": "contradict"},
        )
        assert out["mode"] == MODE_HOLD_ONLY

    def test_incomplete_etf_flow_ignored(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            etf_flow_signal={"verdict": "contradict", "incomplete": True},
        )
        assert out["mode"] == MODE_ATTACK
        assert "ETF_FLOW_CONTRADICT" not in out["reasons"]


class TestComputeExecutionGate:
    def test_srv_not_dict_treated_empty(self) -> None:
        out = compute_execution_gate(srv_index="nope")
        assert out["srvLevel"] is None

    def test_srv_level_empty_string_normalized(self) -> None:
        out = compute_execution_gate(
            srv_index={"level": "", "overlapCount": 3},
            index_signals=[CN, CN2],
            down_count=100,
        )
        assert out["srvLevel"] is None

    def test_overlap_bad_value_guarded(self) -> None:
        out = compute_execution_gate(
            srv_index={"level": SRV_LEVEL_STABLE, "overlapCount": "abc"},
            index_signals=[CN, CN2],
        )
        assert out["srvOverlapCount"] is None

    def test_inflow_bad_value_guarded(self) -> None:
        out = compute_execution_gate(max_sector_inflow_cny="abc")
        assert out["overflowInflowYi"] is None

    def test_strong_with_unknown_srv_level_hold_only(self) -> None:
        out = compute_execution_gate(
            index_signals=[CN, CN2],
            srv_index={"level": "Mild", "overlapCount": 2},
        )
        assert out["mode"] == MODE_HOLD_ONLY
        assert "REGIME_STRONG" in out["reasons"]
        assert "SRV_MILD" in out["reasons"]

    def test_strong_without_srv_attacks(self) -> None:
        out = compute_execution_gate(index_signals=[CN, CN2])
        assert out["mode"] == MODE_ATTACK
        assert "SRV_UNKNOWN" in out["reasons"]


class TestComputeHkGate:
    def test_attack_with_strong_hk(self) -> None:
        out = compute_hk_gate(
            index_signals=[
                {"name": "恒生指数", "signal": "green", "positionRange": "50%-60%"},
                {"name": "恒生科技指数", "signal": "green", "positionRange": "50%-60%"},
            ]
        )
        assert out["mode"] == MODE_ATTACK
        # OPT-093: HK position hints removed (backtest shows no separation).
        assert out["positionRangeHint"] is None

    def test_diverging_allows_entries(self) -> None:
        # S-3 HK 定案（gates=regime）：Diverging 允许开仓，与回测一致
        out = compute_hk_gate(
            index_signals=[HK, {"name": "恒生科技指数", "signal": "green"}]
        )
        assert out["mode"] == MODE_ATTACK
        assert out["allowNewEntries"] is True
        assert "REGIME_DIVERGING" in out["reasons"]

    def test_weak_hk_defends(self) -> None:
        out = compute_hk_gate(
            index_signals=[
                {"name": "恒生指数", "signal": "red", "positionRange": "0%-10%"},
                {"name": "恒生科技指数", "signal": "red"},
            ]
        )
        assert out["mode"] == MODE_DEFEND
        assert "REGIME_WEAK" in out["reasons"]

    def test_falls_back_when_no_hk_signals(self) -> None:
        out = compute_hk_gate(index_signals=[CN, CN2])
        assert out["mode"] == MODE_ATTACK

    def test_risk_defend(self) -> None:
        out = compute_hk_gate(index_signals=[HK, HK2], risk_mode="no_new_positions")
        assert out["mode"] == MODE_DEFEND
        assert "RISK_NO_NEW" in out["reasons"]

    def test_hk_gate_embedded_in_cn_gate(self) -> None:
        out = compute_execution_gate(
            index_signals=[
                CN,
                CN2,
                {"name": "恒生指数", "signal": "green", "positionRange": "50%-60%"},
                {"name": "恒生科技指数", "signal": "green", "positionRange": "50%-60%"},
            ]
        )
        assert out["hkGate"]["mode"] == MODE_ATTACK
        assert out["cnGate"]["mode"] == MODE_ATTACK
