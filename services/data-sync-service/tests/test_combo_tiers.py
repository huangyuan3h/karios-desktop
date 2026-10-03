"""Unit tests for the RESTRUCTURE PR1 combo-tier layer (pure, no DB/network).

Covers the PROMPT Sec 4 contract:
- threshold boundaries + buffer bands, no flip-flop
- 60d -15% fuse downgrade (A->B->C->CASH)
- single-day -8% pause + single-week -5% halve flags
- starship front-gate unmet => 0% + proportional refill
- every tier weights sum to 100% (pre- and post-gate)
- 20% starship cap
- amounts == weights * total_assets; invalid inputs raise
"""

from __future__ import annotations

import pytest

from data_sync_service.service import combo_tiers as ct
from data_sync_service.service import combo_tiers_config as cfg


def _flat_nav(n: int = 60, level: float = 1.0) -> list[float]:
    return [level] * n


# --- threshold boundaries + buffer bands (H2j Sec 3 / RST Sec 2.3) ---


def test_a_to_b_boundary():
    assert ct.resolve_asset_tier(1_550_000, "A") == "B"
    assert ct.resolve_asset_tier(1_549_999, "A") == "A"


def test_b_to_a_boundary():
    assert ct.resolve_asset_tier(1_450_000, "B") == "A"
    assert ct.resolve_asset_tier(1_450_001, "B") == "B"


def test_b_to_c_boundary():
    assert ct.resolve_asset_tier(2_100_000, "B") == "C"
    assert ct.resolve_asset_tier(2_099_999, "B") == "B"


def test_c_to_b_boundary():
    assert ct.resolve_asset_tier(1_800_000, "C") == "B"
    assert ct.resolve_asset_tier(1_800_001, "C") == "C"


def test_buffer_no_flip_flop():
    # 145-155w band holds current tier.
    assert ct.resolve_asset_tier(1_500_000, "A") == "A"
    assert ct.resolve_asset_tier(1_500_000, "B") == "B"
    # 180-210w band holds current tier.
    assert ct.resolve_asset_tier(1_950_000, "B") == "B"
    assert ct.resolve_asset_tier(1_950_000, "C") == "C"
    # One step max: A never jumps straight to C even at 220w.
    assert ct.resolve_asset_tier(2_200_000, "A") == "B"


def test_initial_tier_inference():
    assert ct.resolve_asset_tier(1_200_000, None) == "A"
    assert ct.resolve_asset_tier(1_600_000, None) == "B"
    assert ct.resolve_asset_tier(2_200_000, None) == "C"


# --- fuse downgrade (rolling-60d -15% -> one tier down) ---


def test_fuse_downgrade_a_to_b():
    nav = [1.0] * 59 + [0.849]  # -15.1% vs peak
    fuse = ct.check_fuse(nav)
    assert fuse["downgrade"] is True
    plan = ct.plan_tier(1_200_000, "A", nav, True)
    assert plan["asset_tier"] == "A"
    assert plan["target_tier"] == "B"
    assert plan["fuse_downgraded"] is True


def test_fuse_downgrade_b_to_c_and_c_to_cash():
    nav = [1.0] * 59 + [0.84]
    assert ct.plan_tier(1_600_000, "B", nav, True)["target_tier"] == "C"
    assert ct.plan_tier(2_200_000, "C", nav, True)["target_tier"] == "CASH"


def test_no_fuse_above_threshold():
    nav = [1.0] * 59 + [0.851]  # -14.9%, just above the line
    fuse = ct.check_fuse(nav)
    assert fuse["downgrade"] is False
    plan = ct.plan_tier(1_200_000, "A", nav, True)
    assert plan["target_tier"] == "A"


def test_single_day_pause_and_weekly_halve_flags():
    # Single-day -8%: last/prev -1 <= -8%.
    nav = [1.0] * 59 + [0.92]
    fuse = ct.check_fuse(nav)
    assert fuse["pause_new"] is True
    # Single-week -5%: last/nav[-6] - 1 <= -5% without tripping 60d.
    nav2 = [1.0] * 54 + [1.0, 1.0, 1.0, 1.0, 1.0, 0.949]
    fuse2 = ct.check_fuse(nav2)
    assert fuse2["halve"] is True
    assert fuse2["downgrade"] is False


def test_empty_nav_no_fuse():
    assert ct.check_fuse(None)["triggered"] is False
    assert ct.check_fuse([])["triggered"] is False
    plan = ct.plan_tier(1_200_000, "A", None, True)
    assert plan["target_tier"] == "A"


# --- starship front gate (old three until H2k) ---


def test_starship_ready_keeps_weight():
    plan = ct.plan_tier(1_200_000, "A", _flat_nav(), True)
    assert plan["weights"]["starship"] == pytest.approx(0.20)
    assert plan["starship"]["refilled"] is False


def test_starship_unmet_zero_and_refill_a():
    plan = ct.plan_tier(1_200_000, "A", _flat_nav(), False)
    assert plan["weights"]["starship"] == 0.0
    assert plan["starship"]["refilled"] is True
    # 40/20/20 over 80 refilled to 50/25/25.
    assert plan["weights"]["harbor"] == pytest.approx(0.50)
    assert plan["weights"]["m30"] == pytest.approx(0.25)
    assert plan["weights"]["b3"] == pytest.approx(0.25)


def test_starship_unmet_refill_c():
    plan = ct.plan_tier(2_200_000, "C", _flat_nav(), False)
    assert plan["weights"]["starship"] == 0.0
    # 20/60/10 over 90 refilled proportionally (weights rounded to 6dp).
    assert plan["weights"]["harbor"] == pytest.approx(0.20 / 0.90, abs=1e-6)
    assert plan["weights"]["m30"] == pytest.approx(0.60 / 0.90, abs=1e-6)
    assert plan["weights"]["b3"] == pytest.approx(0.10 / 0.90, abs=1e-6)


def test_starship_three_conditions():
    ready = {
        "paper20_pass": True,
        "holdout_recovered": True,
        "filtered_valid_positive": True,
    }
    assert ct.plan_tier(1_200_000, "A", None, ready)["weights"]["starship"] == pytest.approx(0.20)
    partial = dict(ready, holdout_recovered=False)
    assert ct.plan_tier(1_200_000, "A", None, partial)["weights"]["starship"] == 0.0


def test_b_tier_unaffected_by_gate():
    plan = ct.plan_tier(1_600_000, "B", _flat_nav(), False)
    assert plan["weights"]["starship"] == 0.0
    assert plan["starship"]["refilled"] is False


# --- weights sum 100% + 20% cap + amounts ---


@pytest.mark.parametrize("tier", ["A", "B", "C", "CASH"])
def test_each_tier_weights_sum_100(tier):
    for ready in (True, False):
        plan = ct.plan_tier(1_200_000, tier, _flat_nav(), ready)
        total = sum(plan["weights"].values())
        assert total == pytest.approx(1.0, abs=1e-9)
        req_total = sum(plan["weights_requested"].values())
        assert req_total == pytest.approx(1.0, abs=1e-9)


@pytest.mark.parametrize("tier", ["A", "B", "C", "CASH"])
def test_starship_cap_20(tier):
    for ready in (True, False):
        plan = ct.plan_tier(2_000_000, tier, _flat_nav(), ready)
        assert plan["weights"]["starship"] <= cfg.STARSHIP_CAP + 1e-12


def test_amounts_equal_weights_times_assets():
    for ta, tier in ((1_200_000, "A"), (1_600_000, "B"), (2_200_000, "C")):
        plan = ct.plan_tier(ta, tier, _flat_nav(), False)
        for leg, w in plan["weights"].items():
            # amounts use full precision, weights are 6dp-rounded for display.
            assert plan["amounts"][leg] == pytest.approx(ta * w, abs=1.0)
        assert sum(plan["amounts"].values()) == pytest.approx(ta, abs=1.0)


def test_rebalance_orders_cover_all_legs():
    plan = ct.plan_tier(1_200_000, "A", _flat_nav(), True)
    legs = {o["leg"] for o in plan["rebalance_orders"]}
    assert legs == {"harbor", "m30", "b3", "starship", "cash"}
    assert plan["next_rebalance_rule"]
    assert plan["single_ticket_cap"] == pytest.approx(0.025)


def test_invalid_inputs_raise():
    with pytest.raises(ValueError):
        ct.plan_tier(0, "A", None, True)
    with pytest.raises(ValueError):
        ct.plan_tier(-5, "A", None, True)
    with pytest.raises(ValueError):
        ct.plan_tier(1_200_000, "Z", None, True)
