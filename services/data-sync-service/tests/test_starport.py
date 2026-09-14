"""Pure unit tests for the Starport (Homeport x satellite overlay) timeline helper.

No DB, no network: synthetic rows only.
"""

from __future__ import annotations

from data_sync_service.service.starport import (
    SAT_WEIGHT,
    TWIN_STAR_WEIGHT,
    blend_starport_timeline,
    blend_twin_star_timeline,
)


def _homeport(navs: list[float]) -> dict:
    rows = []
    for i, nav in enumerate(navs):
        rows.append(
            {
                "date": f"2024-01-{i + 1:02d}",
                "prev": f"2023-12-{31 - (i == 0)}" if i == 0 else f"2024-01-{i:02d}",
                "navSingle": nav,
                "navMulti": nav,
                "navSingleReturnPct": round((nav - 1) * 100, 2),
            }
        )
    return {"ok": True, "rows": rows, "summary": {"fusedPct": round((navs[-1] - 1) * 100, 2)}}


def _sat(points: list[tuple[float, bool]]) -> dict:
    rows = [
        {
            "date": f"2024-01-{i + 1:02d}",
            "satNav": nav,
            "satActive": active,
        }
        for i, (nav, active) in enumerate(points)
    ]
    return {"ok": True, "rows": rows, "summary": {"satPct": 0.0}}


class TestStarportTimeline:
    def test_empty_rows_still_labels_strategy(self) -> None:
        out = blend_starport_timeline({"ok": True, "rows": [], "summary": {"fusedPct": 1.0}}, {})
        assert out["strategy"] == "星港" and out["mode"] == "starport"

    def test_inactive_days_reproduce_homeport(self) -> None:
        hp = _homeport([1.0, 1.05, 1.10])
        out = blend_starport_timeline(hp, _sat([(1.0, False), (1.2, False), (1.3, False)]))
        assert [r["navSingle"] for r in out["rows"]] == [1.0, 1.05, 1.10]
        assert out["summary"]["fusedPct"] == out["summary"]["homeportPct"]
        assert out["summary"]["activeDays"] == 0

    def test_active_day_applies_weighted_overlay(self) -> None:
        hp = _homeport([1.0, 1.10])
        out = blend_starport_timeline(hp, _sat([(1.0, False), (1.20, True)]))
        expected = 1.0 * (1.0 + 0.10 + SAT_WEIGHT * (0.20 - 0.10))
        assert abs(out["rows"][1]["navSingle"] - round(expected, 6)) < 1e-9
        assert out["summary"]["activeDays"] == 1

    def test_missing_satellite_day_counts_inactive(self) -> None:
        hp = _homeport([1.0, 1.10])
        sat = {"ok": True, "rows": [{"date": "2024-01-01", "satNav": 1.0, "satActive": True}], "summary": {}}
        out = blend_starport_timeline(hp, sat)
        assert abs(out["rows"][1]["navSingle"] - 1.10) < 1e-9
        assert out["rows"][1]["satActive"] is False

    def test_max_dd_recomputed_on_overlay(self) -> None:
        hp = _homeport([1.0, 1.20, 0.90])
        out = blend_starport_timeline(hp, _sat([(1.0, False), (1.2, False), (0.9, False)]))
        assert out["summary"]["maxDdFusedPct"] == -25.0
        assert out["summary"]["homeportPct"] == -10.0


class TestTwinStarTimeline:
    def test_half_weight_overlay_on_harbor_base(self) -> None:
        hp = _homeport([1.0, 1.10])
        out = blend_twin_star_timeline(hp, _sat([(1.0, False), (1.20, True)]))
        expected = 1.0 * (1.0 + 0.10 + TWIN_STAR_WEIGHT * (0.20 - 0.10))
        assert abs(out["rows"][1]["navSingle"] - round(expected, 6)) < 1e-9
        assert out["mode"] == "twin_star" and out["strategy"] == "双子星"
        assert out["summary"]["harborPct"] == 10.0

    def test_default_weight_is_half(self) -> None:
        assert TWIN_STAR_WEIGHT == 0.5
