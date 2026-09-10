"""机会双子星 (Opportunity Twin-Star): opportunity blend + S-gap engine tests (no DB).

2026-09-01: fixed 50/50 daily-return blending is superseded — satellite
capital follows the core 100% of the time and only switches to candidates on
days it actually holds positions (R-wide open + executable fills).

2026-09-01 exit-day fix: satActive includes the body-exit close day so
round-trip costs in satNav enter opportunity NAV (satPositions alone zeros
that day after the engine closes).
"""

from __future__ import annotations

from types import SimpleNamespace

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


def _core_rows_picks(navs: list[float], picks: list[str]) -> list[dict]:
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
                "pick": picks[i] if i < len(picks) else "GOLD",
            }
        )
    return rows


def _sim_curve(rets: list[float]) -> tuple[list[float], list[str]]:
    """(nav_curve, own_calendar) with terminal point: len = n_days + 1."""
    cal = [f"2026-01-{i+1:02d}" for i in range(len(rets))]
    nav = [1.0]
    for r in rets:
        nav.append(round(nav[-1] * (1.0 + r), 8))
    return nav, cal


class TestSimProductCurve:
    """OPT-152: product-structured curve — STOCK days earn what the S-3 sim earns."""

    def test_stock_day_uses_joint_sim_ret(self) -> None:
        # day2 pick=STOCK: CN +10%, HK +2% -> joint 6%; benchmark core ret 0.
        core = _core_rows_picks([0.0, 0.0], ["GOLD", "STOCK"])
        sat = _sat_rows([0.0, 0.0], [0, 0], active=[False, False])
        nav_cn, cal_cn = _sim_curve([0.0, 0.10])
        nav_hk, cal_hk = _sim_curve([0.0, 0.02])
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 0.0, "basePct": 0.0},
            sat_rows=sat,
            sim_nav_cn=nav_cn,
            sim_cal_cn=cal_cn,
            sim_nav_hk=nav_hk,
            sim_cal_hk=cal_hk,
        )
        last = out["rows"][-1]
        # Benchmark (100% on pick) stays flat; product core = 6%.
        assert last["navSingleReturnPct"] == pytest.approx(0.0, abs=0.01)
        assert last["navSimReturnPct"] == pytest.approx(6.0, abs=0.05)
        assert last["navSimMultiReturnPct"] == pytest.approx(6.0, abs=0.05)
        assert out["summary"]["simPct"] == pytest.approx(6.0, abs=0.05)
        assert out["summary"]["simMultiPct"] == pytest.approx(6.0, abs=0.05)

    def test_etf_day_keeps_replay_return(self) -> None:
        # pick=GOLD: 择强 100% 硬切即真实行为 — product == benchmark path.
        core = _core_rows_picks([0.05, 0.05], ["GOLD", "GOLD"])
        sat = _sat_rows([0.0, 0.0], [0, 0], active=[False, False])
        nav_cn, cal_cn = _sim_curve([0.0, 0.99])  # sim wildly different
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 10.25, "basePct": 0.0},
            sat_rows=sat,
            sim_nav_cn=nav_cn,
            sim_cal_cn=cal_cn,
        )
        last = out["rows"][-1]
        assert last["navSimReturnPct"] == pytest.approx(last["navSingleReturnPct"], abs=0.01)
        assert last["navSimMultiReturnPct"] == pytest.approx(last["navMultiReturnPct"], abs=0.01)

    def test_active_day_blends_half_sat_slice_on_sim_core(self) -> None:
        # STOCK day joint sim +6%; sat +20% active -> multi = 6 + 0.5*(20-6) = 13%.
        core = _core_rows_picks([0.0, 0.0], ["STOCK", "STOCK"])
        sat = _sat_rows([0.0, 0.20], [0, 1], active=[False, True])
        nav_cn, cal_cn = _sim_curve([0.0, 0.10])
        nav_hk, cal_hk = _sim_curve([0.0, 0.02])
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 0.0, "basePct": 0.0},
            sat_rows=sat,
            sim_nav_cn=nav_cn,
            sim_cal_cn=cal_cn,
            sim_nav_hk=nav_hk,
            sim_cal_hk=cal_hk,
        )
        last = out["rows"][-1]
        assert last["navSimReturnPct"] == pytest.approx(6.0, abs=0.05)
        assert last["navSimMultiReturnPct"] == pytest.approx(13.0, abs=0.05)

    def test_missing_market_day_forward_fills(self) -> None:
        # CN calendar lacks day3 (CN closed, HK traded): CN ret 0, HK ret counts.
        core = _core_rows_picks([0.0, 0.0, 0.0], ["STOCK", "STOCK", "STOCK"])
        sat = _sat_rows([0.0, 0.0, 0.0], [0, 0, 0], active=[False, False, False])
        nav_cn, cal_cn = _sim_curve([0.0, 0.0])  # own cal = d1,d2 only
        nav_hk, cal_hk = _sim_curve([0.0, 0.0, 0.04])
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 0.0, "basePct": 0.0},
            sat_rows=sat,
            sim_nav_cn=nav_cn,
            sim_cal_cn=cal_cn,
            sim_nav_hk=nav_hk,
            sim_cal_hk=cal_hk,
        )
        last = out["rows"][-1]
        # CN forward-filled (0), HK +4% -> joint 2%.
        assert last["navSimReturnPct"] == pytest.approx(2.0, abs=0.05)

    def test_no_sim_curves_falls_back_to_none(self) -> None:
        core = _core_rows([0.10, 0.10])
        sat = _sat_rows([0.0, 0.0], [0, 0], active=[False, False])
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 21.0, "basePct": 0.0}, sat_rows=sat
        )
        for row in out["rows"]:
            assert row["navSim"] is None
            assert row["navSimMulti"] is None
        assert out["summary"]["simPct"] is None


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


class TestPostureAnnotation:
    """TIP-016 posture bands: line circuit (realized 30d) + CN sentiment."""

    def _rows(self, n: int):
        core = _core_rows([0.0] * n)
        sat = _sat_rows([0.0] * n, [0] * n, active=[False] * n)
        return core, sat

    def test_circuit_flags_and_sentiment_passthrough(self) -> None:
        core, sat = self._rows(4)
        cn_trades = [
            SimpleNamespace(close_date="2026-01-02", pnl_pct=-10.0),
            SimpleNamespace(close_date="2026-01-03", pnl_pct=-10.0),
            SimpleNamespace(close_date="2026-01-04", pnl_pct=-10.0),
        ]
        # HK: sum -30 but only 2 trades (< min 3) -> never ON
        hk_trades = [
            SimpleNamespace(close_date="2026-01-02", pnl_pct=-15.0),
            SimpleNamespace(close_date="2026-01-03", pnl_pct=-15.0),
        ]
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 0.0, "basePct": 0.0},
            sat_rows=sat,
            cn_trades=cn_trades,
            hk_trades=hk_trades,
            sentiment_by_day={"2026-01-03": "extreme_caution"},
        )
        rows = out["rows"]
        assert [r["cnCircuit"] for r in rows] == [False, False, False, True]
        assert [r["hkCircuit"] for r in rows] == [False, False, False, False]
        assert [r["sentiment"] for r in rows] == [
            None,
            None,
            "extreme_caution",
            None,
        ]

    def test_no_trades_defaults_off(self) -> None:
        core, sat = self._rows(2)
        out = build_twin_star_timeline(
            core_rows=core, core_summary={"fusedPct": 0.0, "basePct": 0.0}, sat_rows=sat
        )
        assert all(r["cnCircuit"] is False for r in out["rows"])
        assert all(r["hkCircuit"] is False for r in out["rows"])
        assert all(r["flow"] is None for r in out["rows"])

    def test_flow_passthrough(self) -> None:
        """TIP-017 flow layer rides rows; missing days stay None (fail-open)."""
        core, sat = self._rows(3)
        flow = {
            "2026-01-01": {
                "etfShareD20Pct": 1.2,
                "marginD20Pct": -0.5,
                "northD20": 320.5,
                "smNetPct": 2.1,
            }
        }
        out = build_twin_star_timeline(
            core_rows=core,
            core_summary={"fusedPct": 0.0, "basePct": 0.0},
            sat_rows=sat,
            flow_by_day=flow,
        )
        rows = out["rows"]
        assert rows[0]["flow"]["etfShareD20Pct"] == 1.2
        assert rows[1]["flow"] is None
        assert rows[2]["flow"] is None
