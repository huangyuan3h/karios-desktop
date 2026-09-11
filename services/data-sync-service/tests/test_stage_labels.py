"""state_bucket_track stage labels — H-SAT-RANK single source of truth."""

from __future__ import annotations

from data_sync_service.service.state_bucket_track import (
    stage_labels,
    stage_rank_key,
    stage_tier,
)


def _ramp(n: int, start: float, step: float) -> list[float]:
    return [start + i * step for i in range(n)]


def test_s2_climax_tier0():
    # steady advance: price > MA20 > MA60, rising, fresh 5d runup
    closes = _ramp(70, 10.0, 0.15)
    base = closes[-6]
    closes[-5:] = [base * (1 + 0.025 * i) for i in range(1, 6)]  # runup5 = 12.5%
    lab = stage_labels(closes)
    assert lab is not None
    assert lab["wein"] == "S2-advance"
    assert lab["runup5"] == "climax"
    assert stage_tier(lab) == 0


def test_s1_cool_tier2():
    # flat base: price oscillates around MAs, no runup
    closes = [10.0 + (0.05 if i % 2 else -0.05) for i in range(70)]
    lab = stage_labels(closes)
    assert lab is not None
    assert stage_tier(lab) == 2


def test_short_history_unlabeled_last():
    assert stage_labels([1.0] * 60) is None
    assert stage_tier(None) == 3
    assert stage_rank_key("a", None, 1.0) > stage_rank_key(
        "b", {"wein": "S1-base", "runup5": "cool"}, 99.0)


def test_rank_key_amp_tiebreak():
    l0 = {"wein": "S2-advance", "runup5": "climax"}
    assert stage_rank_key("a", l0, 5.0) < stage_rank_key("b", l0, 6.0)
