"""双子星 (Twin-Star): blend + S-gap engine tests (no DB)."""

from __future__ import annotations

import pytest

from data_sync_service.service.pick_strong_track import build_twin_star_timeline


def _core_rows(navs: list[float]) -> list[dict]:
    rows = []
    nav = 1.0
    for i, ret in enumerate(navs):
        nav *= 1.0 + ret
        rows.append(
            {
                "date": f"2026-01-{i+1:02d}",
                "navSingle": round(nav, 6),
                "navSingleReturnPct": round((nav - 1) * 100, 2),
                "navMulti": round(nav, 6),
                "pick": "GOLD",
            }
        )
    return rows


def _sat_rows(navs: list[float]) -> list[dict]:
    rows = []
    nav = 1.0
    for i, ret in enumerate(navs):
        nav *= 1.0 + ret
        rows.append(
            {
                "date": f"2026-01-{i+1:02d}",
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "satPositions": i % 5,
            }
        )
    return rows


class TestTwinStarBlend:
    def test_equal_weight_blend(self) -> None:
        # core: +10%/day twice; sat: flat -> blend = +5%/day twice
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0])
        out = build_twin_star_timeline(core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat)
        assert out["strategy"] == "双子星 (Twin-Star)"
        assert out["mode"] == "twin_star"
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(10.25, abs=0.05)
        assert out["rows"][-1]["satNavReturnPct"] == pytest.approx(0.0, abs=1e-3)
        assert out["summary"]["corePct"] == 21.0
        assert out["summary"]["fusedPct"] == pytest.approx(10.25, abs=0.05)

    def test_core_flat_sat_up(self) -> None:
        core = _core_rows([0.0, 0.0])
        sat = _sat_rows([0.20, 0.20])
        out = build_twin_star_timeline(core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat)
        # 0.5 * 20% = 10%/day compounded twice -> +21%
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(21.0, abs=0.1)
        assert out["summary"]["maxDdFusedPct"] == 0.0

    def test_missing_sat_day_forward_fills(self) -> None:
        core = _core_rows([0.01, 0.01])
        sat = _sat_rows([0.01])
        out = build_twin_star_timeline(core_rows=core, core_summary={"fusedPct": 2.0, "basePct": 0.0}, sat_rows=sat)
        assert len(out["rows"]) == 2
        # 缺日 forward-fill 前值 (1.01) → 卫星按 0 收益计
        assert out["rows"][-1]["satNav"] == 1.01
        # day1: 0.5*1%+0.5*1% = +1% → 1.01; day2: 0.5*1%+0.5*0% = +0.5% → 1.01505
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(1.505, abs=0.01)

    def test_no_sat_rows(self) -> None:
        core = _core_rows([0.01])
        out = build_twin_star_timeline(core_rows=core, core_summary={"fusedPct": 1.0, "basePct": 0.0}, sat_rows=[])
        assert out["rows"][0]["navSingleReturnPct"] is None or out["rows"][0]["satNav"] is None
