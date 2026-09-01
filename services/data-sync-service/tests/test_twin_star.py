"""机会双子星 (Opportunity Twin-Star): opportunity blend + S-gap engine tests (no DB).

2026-09-01: fixed 50/50 daily-return blending is superseded — satellite
capital follows the core 100% of the time and only switches to candidates on
days it actually holds positions (R-wide open + executable fills).
"""

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


def _sat_rows(navs: list[float], pos: list[int]) -> list[dict]:
    rows = []
    nav = 1.0
    for i, ret in enumerate(navs):
        nav *= 1.0 + ret
        rows.append(
            {
                "date": f"2026-01-{i+1:02d}",
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "satPositions": pos[i] if i < len(pos) else 0,
            }
        )
    return rows


class TestOpportunityBlend:
    def test_no_sat_position_follows_core(self) -> None:
        # satellite never holds -> opportunity == pure core
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [0, 0])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat
        )
        assert out["strategy"] == "机会双子星 (Opportunity Twin-Star)"
        assert out["mode"] == "opportunity_twin_star"
        assert out["opportunity"] is True
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(21.0, abs=0.05)

    def test_sat_position_replaces_half_slice(self) -> None:
        # core flat; sat holds +20% on day2 -> opp_ret = 0 + 0.5*(0.2-0) = +10%
        core = _core_rows([0.0, 0.0])
        sat = _sat_rows([0.0, 0.20], [0, 1])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat
        )
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(10.0, abs=0.05)

    def test_sat_worse_than_core_dilutes_only_when_holding(self) -> None:
        # core +10%/day; sat holds but loses -> opp = 0.1 + 0.5*(-0.05-0.1) = +2.5%
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([-0.05, -0.05], [1, 1])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat
        )
        # day1: +2.5% -> 1.025; day2: +2.5% -> 1.050625
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(5.06, abs=0.05)

    def test_legacy_equal_weight_still_available(self) -> None:
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [1, 1])
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 21.0, "basePct": 0.0},
            sat_rows=sat,
            opportunity=False,
        )
        assert out["opportunity"] is False
        assert out["mode"] == "opportunity_twin_star"
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(10.25, abs=0.05)

    def test_missing_sat_day_forward_fills(self) -> None:
        core = _core_rows([0.01, 0.01])
        sat = _sat_rows([0.01], [1])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 2.0, "basePct": 0.0}, sat_rows=sat
        )
        assert len(out["rows"]) == 2
        # day2 forward-filled satNav with pos=1 -> sat keeps last holding state
        assert out["rows"][-1]["satNav"] == 1.01


class TestSgapUniverseFilter:
    def test_limit_lock_detection(self) -> None:
        from data_sync_service.service.state_bucket_track import _t1_limit_locked

        per_ts = {
            "600000.SH": [
                {"date": "2026-08-27", "close": 10.0, "pre_close": 9.40},
                {"date": "2026-08-28", "close": 11.0, "pre_close": 10.0},
            ]
        }
        date_idx = {"600000.SH": {"2026-08-27": 0, "2026-08-28": 1}}
        # 10% limit: 11.0 >= 10.0*1.096 -> locked
        assert _t1_limit_locked(per_ts, date_idx, "2026-08-28", "600000.SH") is True
        # 08-27 closed +6.4% (10.0/9.4) -> not locked
        assert _t1_limit_locked(per_ts, date_idx, "2026-08-27", "600000.SH") is False

    def test_chi_next_20pct_band(self) -> None:
        from data_sync_service.service.state_bucket_track import _t1_limit_locked

        per_ts = {
            "300001.SZ": [
                {"date": "2026-08-28", "close": 11.5, "pre_close": 10.0},
            ]
        }
        date_idx = {"300001.SZ": {"2026-08-28": 0}}
        # 20% band: 11.5 < 10.0*1.196 -> NOT locked (10% band would say locked)
        assert _t1_limit_locked(per_ts, date_idx, "2026-08-28", "300001.SZ") is False
        per_ts["300001.SZ"][0]["close"] = 12.0
        assert _t1_limit_locked(per_ts, date_idx, "2026-08-28", "300001.SZ") is True