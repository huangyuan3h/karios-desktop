from __future__ import annotations

from data_sync_service.service.trendok import (
    MACRO_LOCK_DOWN_THRESHOLD,
    apply_macro_override_lock,
    macro_override_lock_active,
)


def _sample_result(*, buy_action: str = "buy", buy_mode: str = "B_momentum") -> dict:
    return {
        "symbol": "CN:600000",
        "buyAction": buy_action,
        "buyMode": buy_mode,
        "buyWhy": "test",
        "buyChecks": {},
        "riskAlerts": [],
    }


def test_macro_lock_active_on_extreme_caution() -> None:
    assert macro_override_lock_active("extreme_caution", 1000) is True


def test_macro_lock_active_on_down_threshold() -> None:
    assert macro_override_lock_active("normal", MACRO_LOCK_DOWN_THRESHOLD) is True
    assert macro_override_lock_active("normal", MACRO_LOCK_DOWN_THRESHOLD - 1) is False


def test_macro_lock_inactive_on_capitulation() -> None:
    assert macro_override_lock_active("capitulation_v_bottom", 4600) is False


def test_macro_lock_inactive_on_confirmed_uptrend() -> None:
    assert macro_override_lock_active("confirmed_uptrend", 800) is False


def test_apply_macro_lock_forces_all_buy_actions_to_avoid() -> None:
    rows = [
        _sample_result(buy_action="buy", buy_mode="B_momentum"),
        _sample_result(buy_action="B_pullback", buy_mode="B_momentum"),
        _sample_result(buy_action="wait", buy_mode="A_pullback"),
    ]
    out = apply_macro_override_lock(rows, "extreme_caution", 4600)
    for row in out:
        assert row["buyAction"] == "avoid"
        assert row["buyMode"] == "none"
        assert row["macroLock"]["active"] is True
        assert row["buyChecks"]["blocked_macro_lock"] is True
        assert any(a.get("code") == "macro_override_lock" for a in row["riskAlerts"])


def test_apply_macro_lock_skips_when_inactive() -> None:
    row = _sample_result()
    out = apply_macro_override_lock([row], "normal", 1000)
    assert out[0]["buyAction"] == "buy"
    assert "macroLock" not in out[0]
