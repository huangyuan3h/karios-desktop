"""Pure unit tests for the Homeport (Harbor x B3 risk-budget) timeline helper.

No DB, no network: synthetic panels / rows only.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

from data_sync_service.service.homeport import (
    RISK_UNIVERSE,
    blend_homeport_timeline,
    blend_monthly_nav,
    inverse_vol_weights,
    risk_budget_nav,
    risk_budget_run,
)


def _days(n: int, start: date = date(2024, 1, 1)) -> list[str]:
    out: list[str] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _panel(days: list[str], *, shock_from: int | None = None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for k, ts in enumerate(RISK_UNIVERSE):
        amp = 0.004 * (k + 1)
        vals: dict[str, float] = {}
        for i, d in enumerate(days):
            px = 100.0 * (1.0 + 0.0004 * (k + 1)) ** i * (1.0 + amp * math.sin(i / 3.0))
            if shock_from is not None and i >= shock_from:
                px *= 1.5
            vals[d] = px
        out[ts] = vals
    return out


class TestWeights:
    def test_inverse_vol_proportional(self) -> None:
        w = inverse_vol_weights({"a": 0.1, "b": 0.2, "c": 0.4})
        assert abs(w["a"] - 4 / 7) < 1e-12
        assert abs(w["b"] - 2 / 7) < 1e-12
        assert abs(w["c"] - 1 / 7) < 1e-12

    def test_degenerate_vols_fall_back_to_equal(self) -> None:
        w = inverse_vol_weights({"a": 0.0, "b": 0.0})
        assert w == {"a": 0.5, "b": 0.5}


class TestRiskBudgetNav:
    def test_warmup_is_flat_then_causal(self) -> None:
        days = _days(120)
        base = _panel(days)
        shocked = _panel(days, shock_from=80)
        nav_a = risk_budget_nav(base, days)
        nav_b = risk_budget_nav(shocked, days)
        assert nav_a[:80] == nav_b[:80]
        assert nav_a[80:] != nav_b[80:]

    def test_flat_panel_stays_flat(self) -> None:
        days = _days(90)
        flat = {ts: {d: 100.0 for d in days} for ts in RISK_UNIVERSE}
        nav = risk_budget_nav(flat, days)
        assert all(abs(v - 1.0) < 1e-12 for v in nav)


class TestBlend:
    def test_identical_legs_reproduce_series(self) -> None:
        nav = [1.0, 1.01, 1.02, 1.03]
        cal = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]
        mix = blend_monthly_nav(nav, nav, cal)
        assert all(abs(a - b) < 1e-12 for a, b in zip(mix, nav, strict=True))


class TestRiskBudgetRun:
    def test_nav_matches_legacy_function(self) -> None:
        days = _days(300)
        panel = _panel(days)
        assert risk_budget_nav(panel, days) == risk_budget_run(panel, days)["nav"]

    def test_events_cover_initial_and_month_turns(self) -> None:
        days = _days(300)
        panel = _panel(days)
        run = risk_budget_run(panel, days)
        events = run["events"]
        assert events[0] == {
            "date": days[0],
            "weights": {ts: 0.2 for ts in RISK_UNIVERSE},
            "turnover": 0.0,
        }
        assert len(events) > 3  # ~10 month turns in 300 sessions
        seen_months = [e["date"][:7] for e in events]
        assert seen_months == sorted(seen_months) and len(set(seen_months)) == len(events)
        for e in events[1:]:
            assert abs(sum(e["weights"].values()) - 1.0) < 1e-3  # 4dp display rounding
            assert e["turnover"] >= 0.0
        assert len(run["weights"]) == len(days)


class TestHomeportRiskLeg:
    def _blended(self) -> dict:
        days = _days(300)
        panel = _panel(days)
        # 130 rows: the 60-session vol warmup must clear before the first
        # month-turn rebalance can fire.
        rows = [
            {
                "date": days[i],
                "prev": days[i - 1],
                "navSingle": 1.0 + 0.001 * i,
                "navBase": 1.0 + 0.0005 * i,
                "navSingleReturnPct": round(0.1 * i, 2),
            }
            for i in range(1, 131)
        ]
        return blend_homeport_timeline(
            {"ok": True, "rows": rows, "summary": {"fusedPct": 6.0}}, risk_closes=panel
        )

    def test_risk_universe_blotter_held_attached(self) -> None:
        out = self._blended()
        universe = out["riskUniverse"]
        assert [u["ts"] for u in universe] == list(RISK_UNIVERSE)
        assert all(u["name"] for u in universe)
        blotter = out["riskBlotter"]
        assert len(blotter) >= 2
        assert blotter[0]["turnover"] == 0.0
        held = out["riskHeld"]
        assert held["date"] == out["rows"][-1]["date"]
        assert abs(sum(held["weights"].values()) - 1.0) < 1e-3  # 4dp display rounding

    def test_rows_carry_daily_top_holding(self) -> None:
        out = self._blended()
        for r in out["rows"]:
            assert r["riskTop"] in RISK_UNIVERSE
            assert 0.0 < r["riskTopW"] <= 1.0

    def test_summary_carries_sleeve_total_and_dd(self) -> None:
        out = self._blended()
        assert out["summary"]["riskPct"] != 0.0
        assert out["summary"]["riskMaxDdPct"] >= 0.0
        assert out["summary"]["harborPct"] == 6.0


class TestHomeportTimeline:
    def test_empty_rows_still_labels_strategy(self) -> None:
        out = blend_homeport_timeline({"ok": True, "rows": [], "summary": {"fusedPct": 1.0}})
        assert out["strategy"] == "母港" and out["mode"] == "homeport"

    def test_blends_rows_and_keeps_harbor_reference(self) -> None:
        days = _days(300)
        panel = _panel(days)
        rows = [
            {
                "date": days[i],
                "prev": days[i - 1],
                "navSingle": 1.0 + 0.001 * i,
                "navBase": 1.0 + 0.0005 * i,
                "navSingleReturnPct": round(0.1 * i, 2),
            }
            for i in range(1, 61)
        ]
        out = blend_homeport_timeline(
            {"ok": True, "rows": rows, "summary": {"fusedPct": 6.0}}, risk_closes=panel
        )
        assert out["mode"] == "homeport" and out["strategy"] == "母港"
        assert len(out["rows"]) == 60
        assert out["summary"]["harborPct"] == 6.0
        assert out["summary"]["fusedPct"] != 6.0
        for r in out["rows"]:
            assert r["navSingle"] == r["navMulti"]
            assert r["navSingleReturnPct"] == r["navMultiReturnPct"]


class TestHomeportM30:
    """H-MIX-TUNE defensive tier: same legs, harbor weight 0.7 (default 0.5 frozen)."""

    def _rows(self) -> tuple[list[str], list[dict]]:
        days = _days(300)
        panel = _panel(days)
        rows = [
            {
                "date": days[i],
                "prev": days[i - 1],
                "navSingle": 1.0 + 0.001 * i,
                "navBase": 1.0 + 0.0005 * i,
                "navSingleReturnPct": round(0.1 * i, 2),
            }
            for i in range(1, 131)
        ]
        return panel, rows

    def test_m30_marks_weight_and_beats_m50_when_harbor_rises(self) -> None:
        panel, rows = self._rows()
        flat = {ts: {d: 100.0 for d in _days(300)} for ts in panel}
        base = {"ok": True, "rows": rows, "summary": {"fusedPct": 6.0}}
        m50 = blend_homeport_timeline(dict(base), risk_closes=flat)
        m30 = blend_homeport_timeline(dict(base), risk_closes=flat, w_harbor=0.7)
        assert m50["summary"].get("harborWeight", 0.5) == 0.5
        assert m30["summary"]["harborWeight"] == 0.7
        # Rising harbor + flat passive => more harbor weight wins here.
        assert m30["summary"]["fusedPct"] > m50["summary"]["fusedPct"]
        # Leg-level detail is weight-independent.
        assert m30["riskUniverse"] == m50["riskUniverse"]
        assert m30["riskBlotter"] == m50["riskBlotter"]
        assert [r["riskTop"] for r in m30["rows"]] == [r["riskTop"] for r in m50["rows"]]

    def test_default_weight_unchanged(self) -> None:
        panel, rows = self._rows()
        base = {"ok": True, "rows": rows, "summary": {"fusedPct": 6.0}}
        assert blend_homeport_timeline(dict(base), risk_closes=panel)["summary"][
            "fusedPct"
        ] == blend_homeport_timeline(dict(base), risk_closes=panel, w_harbor=0.5)["summary"][
            "fusedPct"
        ]


class TestHomeportM30Route:
    """Route dispatch for the M30 defensive tier (mocked Harbor leg, no DB)."""

    def test_homeport_m30_blends_at_70_pct_harbor(self, monkeypatch) -> None:  # noqa: ANN001
        from data_sync_service.api import backtest_routes as br
        from data_sync_service.service import homeport as hp

        days = _days(300)
        panel = _panel(days)
        rows = [
            {
                "date": days[i],
                "prev": days[i - 1],
                "navSingle": 1.0 + 0.001 * i,
                "navBase": 1.0 + 0.0005 * i,
                "navSingleReturnPct": round(0.1 * i, 2),
            }
            for i in range(1, 61)
        ]
        harbor_result = {"ok": True, "rows": rows, "summary": {"fusedPct": 6.0}}
        orig = br._get_or_build_timeline

        def _fake(start: str, end: str, *, strategy: str = "harbor", **kw: object) -> object:
            if strategy == "harbor":
                return harbor_result, None
            return orig(start, end, strategy=strategy, **kw)  # type: ignore[arg-type]

        monkeypatch.setattr(br, "_get_or_build_timeline", _fake)
        monkeypatch.setattr(hp, "load_risk_closes", lambda: panel)
        out, _ = br._get_or_build_timeline(
            "2024-01-01", "2024-03-01", strategy="homeport_m30"
        )
        assert out["summary"]["harborWeight"] == 0.7
        direct = hp.blend_homeport_timeline(
            {"ok": True, "rows": [dict(r) for r in rows], "summary": {"fusedPct": 6.0}},
            risk_closes=panel,
            w_harbor=0.7,
        )
        assert out["summary"]["fusedPct"] == direct["summary"]["fusedPct"]
