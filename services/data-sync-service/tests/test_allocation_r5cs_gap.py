"""Coverage for allocation R5CS + sleeve fallback + weekly persistence (M30 gap).

All branches are pure or use injected fakes; no DB required.
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import allocation as al


def test_week_start_for_monday() -> None:
    # 2026-06-22 is a Monday; Sunday maps back to same Monday
    assert al.week_start_for("2026-06-22") == "2026-06-22"
    assert al.week_start_for("2026-06-24") == "2026-06-22"
    assert al.week_start_for("2026-06-21") == "2026-06-15"


def test_weights_with_sleeve_from_regimes_pick_none_means_cash() -> None:
    with patch("data_sync_service.service.multi_asset_sleeve._pick", return_value=None):
        assert al.weights_with_sleeve_from_regimes("Weak", "Weak") == (0.0, 0.0, 0.0)


def test_weights_with_sleeve_from_regimes_pick_below_ma_means_cash() -> None:
    with patch(
        "data_sync_service.service.multi_asset_sleeve._pick",
        return_value={"above_ma200": False},
    ):
        assert al.weights_with_sleeve_from_regimes("Weak", "Weak") == (0.0, 0.0, 0.0)


def test_weights_with_sleeve_from_regimes_pick_above_ma_means_sleeve() -> None:
    with patch(
        "data_sync_service.service.multi_asset_sleeve._pick",
        return_value={"above_ma200": True},
    ):
        assert al.weights_with_sleeve_from_regimes("Weak", "Weak") == (0.0, 0.0, 1.0)


def test_weights_with_sleeve_from_regimes_fallback_to_nasdaq_leg() -> None:
    with patch(
        "data_sync_service.service.multi_asset_sleeve._pick",
        side_effect=RuntimeError("no bars"),
    ):
        with patch(
            "data_sync_service.service.multi_asset_sleeve._etf_market_data",
            return_value={"ok": True, "above": True},
        ):
            assert al.weights_with_sleeve_from_regimes("Weak", "Weak") == (0.0, 0.0, 1.0)
        with patch(
            "data_sync_service.service.multi_asset_sleeve._etf_market_data",
            return_value={"ok": False, "above": False},
        ):
            assert al.weights_with_sleeve_from_regimes("Weak", "Weak") == (0.0, 0.0, 0.0)


def test_live_regimes_shape() -> None:
    with (
        patch(
            "data_sync_service.service.market_regime.get_index_signals",
            return_value=[{"name": "x"}],
        ),
        patch(
            "data_sync_service.service.allocation.live_regimes.__wrapped__"
            if hasattr(al.live_regimes, "__wrapped__")
            else "data_sync_service.service.execution_gate.classify_market_regime",
            return_value="Strong",
        )
        if False
        else patch(
            "data_sync_service.service.execution_gate.classify_market_regime",
            return_value="Strong",
        ),
        patch(
            "data_sync_service.service.market_regime.get_hk_regime",
            return_value={"regime": "Weak"},
        ),
    ):
        out = al.live_regimes(as_of_date="2026-06-22")
    assert out == {"CN": "Strong", "HK": "Weak"}


def test_decide_week_persists_first_wins() -> None:
    seen: dict = {}

    def fake_insert(*, week_start, cn_regime, hk_regime, w_cn, w_hk):
        seen.update(
            week_start=week_start,
            cn_regime=cn_regime,
            hk_regime=hk_regime,
            w_cn=w_cn,
            w_hk=w_hk,
        )
        return dict(seen)

    with (
        patch(
            "data_sync_service.service.allocation.live_regimes",
            return_value={"CN": "Strong", "HK": "Weak"},
        ),
        patch(
            "data_sync_service.db.allocation.insert_week_decision",
            side_effect=fake_insert,
        ),
    ):
        out = al.decide_week(week_start="2026-06-22", as_of_date="2026-06-22")
    assert out["weekStart"] == "2026-06-22"
    assert out["decision"]["w_cn"] == 1.0
    assert out["decision"]["cn_regime"] == "Strong"


def test_week_weights_uses_persisted_row() -> None:
    with patch(
        "data_sync_service.db.allocation.get_week_decision",
        return_value={"w_cn": 0.0, "w_hk": 1.0},
    ):
        out = al.week_weights("2026-06-24")
    assert out["weekStart"] == "2026-06-22"
    assert out["decision"] == {"w_cn": 0.0, "w_hk": 1.0}


def test_week_weights_falls_back_to_spot_decision() -> None:
    with (
        patch("data_sync_service.db.allocation.get_week_decision", return_value=None),
        patch(
            "data_sync_service.service.allocation.live_regimes",
            return_value={"CN": "Weak", "HK": "Weak"},
        ),
        patch(
            "data_sync_service.db.allocation.insert_week_decision",
            return_value={"w_cn": 0.0, "w_hk": 0.0},
        ),
    ):
        out = al.week_weights("2026-06-24")
    assert out["weekStart"] == "2026-06-22"
    assert out["decision"] == {"w_cn": 0.0, "w_hk": 0.0}


def test_weights_r5cs_both_weak_delegates() -> None:
    assert al.weights_r5cs("Weak", "Weak", etf_above_ma200=True) == (0.0, 0.0, 1.0)
    assert al.weights_r5cs("Weak", "Weak", etf_above_ma200=False) == (0.0, 0.0, 0.0)


def test_weights_r5cs_cn_idle_to_sleeve() -> None:
    # 5 of 10 holdings used -> half stays CN, half idle to sleeve
    assert al.weights_r5cs("Strong", "Weak", cn_holdings=5, etf_above_ma200=True) == (
        0.5,
        0.0,
        0.5,
    )
    # No sleeve when ETF below MA
    assert al.weights_r5cs("Strong", "Weak", cn_holdings=5, etf_above_ma200=False) == (
        1.0,
        0.0,
        0.0,
    )
    # Full book -> no idle
    assert al.weights_r5cs("Strong", "Weak", cn_holdings=10, etf_above_ma200=True) == (
        1.0,
        0.0,
        0.0,
    )
    # Empty book -> all idle to sleeve
    assert al.weights_r5cs("Strong", "Weak", cn_holdings=0, etf_above_ma200=True) == (
        0.0,
        0.0,
        1.0,
    )


def test_weights_r5cs_hk_idle_to_sleeve() -> None:
    assert al.weights_r5cs("Weak", "Strong", hk_holdings=4, etf_above_ma200=True) == (0.0, 0.4, 0.6)
    assert al.weights_r5cs("Weak", "Strong", hk_holdings=4, etf_above_ma200=False) == (
        0.0,
        1.0,
        0.0,
    )


def test_weights_r5cs_resolves_sleeve_via_pick_when_not_injected() -> None:
    import pytest as _pytest

    with patch(
        "data_sync_service.service.multi_asset_sleeve._pick",
        return_value={"above_ma200": True},
    ):
        w = al.weights_r5cs("Strong", "Weak", cn_holdings=8)
        assert w[0] == _pytest.approx(0.8)
        assert w[1] == _pytest.approx(0.0)
        assert w[2] == _pytest.approx(0.2)
    with patch(
        "data_sync_service.service.multi_asset_sleeve._pick",
        side_effect=RuntimeError("db down"),
    ):
        # Fallback treats sleeve as unavailable -> no ETF leg
        assert al.weights_r5cs("Strong", "Weak", cn_holdings=8) == (1.0, 0.0, 0.0)


def test_weights_r5cs_zero_max_positions_guards_division() -> None:
    assert al.weights_r5cs(
        "Strong", "Weak", cn_holdings=5, max_positions=0, etf_above_ma200=True
    ) == (1.0, 0.0, 0.0)
