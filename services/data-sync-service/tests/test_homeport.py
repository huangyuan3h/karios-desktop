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
