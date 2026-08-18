"""env_label tests (TIP-014 Phase 1a) — pure unit (no DB)."""

from __future__ import annotations

from data_sync_service.service import env_label as el


def test_bucket_constants() -> None:
    assert el.ENV_UPTREND == "uptrend"
    assert el.ENV_FAN == "fan"
    assert el.ENV_WEAK == "weak"
    assert el.ENV_NEUTRAL == "neutral"
    assert el.ENV_UNKNOWN == "unknown"


def test_weak_ratio_max_matches_execution_gate() -> None:
    """Implicit-weak definition is shared with the live gate (TIP-014)."""
    assert el.WEAK_RATIO_MAX == 0.5


def test_churn_ratio() -> None:
    assert el._churn_ratio(["a", "b", "c"], {"a", "b", "c"}) == 0.0
    assert el._churn_ratio(["a", "b", "d"], {"a", "b", "c"}) == 1 / 3
    assert el._churn_ratio(["a", "b"], {"c", "d"}) == 1.0
    assert el._churn_ratio(None, {"a"}) is None
    assert el._churn_ratio(["a"], None) is None
    assert el._churn_ratio(["a", "b", "c"], set()) == 0.0


def test_fan_ratio_bounds() -> None:
    assert el.FAN_RATIO_LOW == 0.5
    assert el.FAN_RATIO_HIGH == 1.5
    assert el.UPTREND_RATIO_MIN == 2.0
