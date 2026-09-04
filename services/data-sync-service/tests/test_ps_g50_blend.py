"""PS-G50-X idle-to-core blend unit tests — synthetic NAV, no DB."""

from __future__ import annotations

import pytest

from data_sync_service.service.ps_g50_blend import (
    blend_nav_idle_to_core,
    blend_nav_opportunity,
    blend_nav_static,
    daily_ret_idle_to_core,
    sat_idle_frac,
)


class TestSatIdleFrac:
    def test_empty_is_fully_idle(self) -> None:
        assert sat_idle_frac(0) == 1.0

    def test_two_slots_half_idle(self) -> None:
        assert sat_idle_frac(2) == pytest.approx(0.5)

    def test_four_or_more_zero_idle(self) -> None:
        assert sat_idle_frac(4) == 0.0
        assert sat_idle_frac(15) == 0.0


class TestDailyRet:
    def test_fully_idle_equals_core(self) -> None:
        assert daily_ret_idle_to_core(0.10, 0.0, 1.0) == pytest.approx(0.10)

    def test_fully_invested_equals_static_half(self) -> None:
        assert daily_ret_idle_to_core(0.10, 0.20, 0.0) == pytest.approx(0.15)

    def test_half_idle_replaces_cash_zero_with_core(self) -> None:
        # 0.5*0.10 + 0.5*(0.04 + 0.5*0.10) = 0.05 + 0.045 = 0.095
        assert daily_ret_idle_to_core(0.10, 0.04, 0.5) == pytest.approx(0.095)


class TestBlendNav:
    def test_idle_book_tracks_core(self) -> None:
        core = [1.0, 1.10, 1.21]
        sat = [1.0, 1.0, 1.0]
        out = blend_nav_idle_to_core(core, sat, [0, 0, 0])
        assert out[-1] == pytest.approx(1.21)

    def test_full_book_matches_static(self) -> None:
        core = [1.0, 1.10, 1.21]
        sat = [1.0, 1.20, 1.44]
        slots = [12, 12, 12]
        idle = blend_nav_idle_to_core(core, sat, slots)
        static = blend_nav_static(core, sat)
        assert idle[-1] == pytest.approx(static[-1])

    def test_opportunity_binary_overweights_sparse_sat(self) -> None:
        # 1 slot at 25% clip: idle-to-core keeps 75% of the sat sleeve on core;
        # opportunity still 50/50.
        core = [1.0, 1.10]
        sat = [1.0, 1.0]  # sat flat (mostly cash) while "active"
        idle = blend_nav_idle_to_core(core, sat, [0, 1])
        opp = blend_nav_opportunity(core, sat, [False, True])
        assert idle[-1] > opp[-1]
        # idle: 0.5*0.1 + 0.5*(0 + 0.75*0.1) = 0.0875 → 1.0875
        assert idle[-1] == pytest.approx(1.0875)
        # opp: 0.1 + 0.5*(0-0.1) = 0.05 → 1.05
        assert opp[-1] == pytest.approx(1.05)
