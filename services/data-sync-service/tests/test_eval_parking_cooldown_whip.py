"""Unit tests for the H-PARK-C rerun whip-saw metrics (2026-11-05).

``whip_stats`` is a pure function over ``parking_replay`` records; no DB/network.
See docs/backtests/stable/parking-cooldown-rerun-2026-11-05.md
"""

from __future__ import annotations

from scripts.eval_parking_cooldown import whip_stats


def _rec(key: str, *, trail: bool = False) -> dict:
    return {"date": "x", "prev": "y", "pick_key": key, "trail_exit": trail}


def test_whip_counts_by_horizon() -> None:
    records = [
        _rec("GOLD"),
        _rec("GOLD"),
        _rec("REPO", trail=True),  # exits GOLD -> rebought next session
        _rec("GOLD"),
        _rec("GOLD"),
        _rec("GOLD"),
        _rec("GOLD"),
        _rec("REPO", trail=True),  # exits GOLD -> rebought 4 sessions later
        _rec("REPO"),
        _rec("REPO"),
        _rec("REPO"),
        _rec("GOLD"),
    ]
    stats = whip_stats(records)
    assert stats["trail_exits"] == 2
    assert stats["whip_rebuy_within_1"] == 1
    assert stats["whip_rebuy_within_3"] == 1
    assert stats["whip_rebuy_within_5"] == 2
    assert stats["days_by_key"] == {"GOLD": 7, "REPO": 5}
    assert stats["time_in_repo"] == round(5 / 12, 3)
    assert stats["time_in_oil"] == 0.0


def test_trail_exit_without_prior_holding_is_not_a_whip() -> None:
    stats = whip_stats([_rec("REPO", trail=True), _rec("GOLD")])
    assert stats["trail_exits"] == 1
    assert stats["whip_rebuy_within_5"] == 0


def test_no_trail_exits_is_all_zero() -> None:
    stats = whip_stats([_rec("OIL"), _rec("OIL"), _rec("REPO")])
    assert stats["trail_exits"] == 0
    assert stats["whip_rebuy_within_1"] == 0
    assert stats["time_in_oil"] == round(2 / 3, 3)
