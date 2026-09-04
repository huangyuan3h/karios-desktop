"""机会双子星 (Opportunity Twin-Star): opportunity blend + S-gap engine tests (no DB).

2026-09-01: fixed 50/50 daily-return blending is superseded — satellite
capital follows the core 100% of the time and only switches to candidates on
days it actually holds positions (R-wide open + executable fills).

2026-09-01 exit-day fix: satActive includes the body-exit close day so
round-trip costs in satNav enter opportunity NAV (satPositions alone zeros
that day after the engine closes).
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


def _sat_rows(
    navs: list[float],
    pos: list[int],
    *,
    active: list[bool] | None = None,
    slots: list[int] | None = None,
) -> list[dict]:
    rows = []
    nav = 1.0
    for i, ret in enumerate(navs):
        nav *= 1.0 + ret
        p = pos[i] if i < len(pos) else 0
        row: dict = {
            "date": f"2026-01-{i+1:02d}",
            "satNav": round(nav, 6),
            "satNavReturnPct": round((nav - 1) * 100, 2),
            "satPositions": p,
        }
        if slots is not None:
            row["satSlots"] = slots[i] if i < len(slots) else p
        if active is not None:
            row["satActive"] = active[i] if i < len(active) else bool(p)
        rows.append(row)
    return rows


class TestOpportunityBlend:
    def test_no_sat_position_follows_core(self) -> None:
        # satellite never holds -> opportunity == pure core
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [0, 0], active=[False, False])
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
        sat = _sat_rows([0.0, 0.20], [0, 1], active=[False, True])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat
        )
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(10.0, abs=0.05)

    def test_sat_worse_than_core_dilutes_only_when_holding(self) -> None:
        # core +10%/day; sat holds but loses -> opp = 0.1 + 0.5*(-0.05-0.1) = +2.5%
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([-0.05, -0.05], [1, 1], active=[True, True])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat
        )
        # day1: +2.5% -> 1.025; day2: +2.5% -> 1.050625
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(5.06, abs=0.05)

    def test_exit_day_costs_enter_opportunity_nav(self) -> None:
        """Body-exit day: satPositions=0 after close, but satActive=True.

        Engine charges COSTS_ROUNDTRIP into satNav on the exit day. Opportunity
        blend must still apply the sat slice that day — otherwise costs escape
        and opportunity NAV is overstated.
        """
        # core flat; sat +3%/+3%/-2% (exit day absorbs cost+last move)
        core = _core_rows([0.0, 0.0, 0.0])
        # overnight pos on d1/d2; exit day pos=0 but active
        sat = _sat_rows(
            [0.03, 0.03, -0.02],
            [1, 1, 0],
            active=[True, True, True],
            slots=[1, 1, 1],
        )
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat
        )
        # 0.5 of each sat day: 1.015 * 1.015 * 0.99 - 1
        expected = ((1.015 * 1.015 * 0.99) - 1) * 100
        assert out["rows"][-1]["navSingleReturnPct"] == pytest.approx(expected, abs=0.05)
        assert out["rows"][-1]["satActive"] is True
        assert out["rows"][-1]["satSlots"] == 1
        assert out["rows"][-1]["satPositions"] == 0

        # Regression: legacy rows without satActive (pos-only) miss the exit day
        # (here exit ret is negative → legacy overstates opportunity NAV)
        sat_legacy = _sat_rows([0.03, 0.03, -0.02], [1, 1, 0])  # no satActive key
        out_legacy = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat_legacy
        )
        assert out_legacy["rows"][-1]["navSingleReturnPct"] > out["rows"][-1]["navSingleReturnPct"]
        assert out_legacy["rows"][-1]["navSingleReturnPct"] == pytest.approx(
            ((1.015 * 1.015) - 1) * 100, abs=0.05
        )

    def test_legacy_equal_weight_still_available(self) -> None:
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [1, 1], active=[True, True])
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
        sat = _sat_rows([0.01], [1], active=[True])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 2.0, "basePct": 0.0}, sat_rows=sat
        )
        assert len(out["rows"]) == 2
        # day2 forward-filled satNav with pos=1 -> sat keeps last holding state
        assert out["rows"][-1]["satNav"] == 1.01

    def test_core_nav_survives_opportunity_blend(self) -> None:
        """Fused navSingle overwrites the core leg — keep coreNav for overlay."""
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [1, 1], active=[True, True])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat
        )
        last = out["rows"][-1]
        assert last["coreNav"] == core[-1]["navSingle"]
        assert last["coreNavReturnPct"] == core[-1]["navSingleReturnPct"]
        assert last["navSingle"] < last["coreNav"]
        assert out["summary"]["satActiveDays"] == 2


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
