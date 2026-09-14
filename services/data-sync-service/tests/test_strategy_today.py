"""Tests for the B3 monthly decision state (display helper, no DB)."""

from __future__ import annotations

import math
from datetime import date, timedelta

from data_sync_service.service.homeport import RISK_UNIVERSE
from data_sync_service.service.strategy_today import B3_LABELS, b3_state


def _days(n: int) -> list[str]:
    out: list[str] = []
    d = date(2026, 6, 1)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _panel(days: list[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for k, ts in enumerate(RISK_UNIVERSE):
        amp = 0.001 * (k + 1)
        vals: dict[str, float] = {}
        for i, d in enumerate(days):
            vals[d] = 100.0 * (1.0 + 0.0005 * (k + 1)) ** i * (1.0 + amp * math.sin(i / 3.0))
        out[ts] = vals
    return out


class TestB3State:
    def test_state_shape_and_ordering(self) -> None:
        days = _days(120)
        out = b3_state(_panel(days))
        assert out["ok"] is True
        assert out["asOf"] == days[-1]
        month = days[-1][:7]
        assert out["rebalanceDate"] == next(d for d in days if d[:7] == month)
        assert len(out["universe"]) == len(RISK_UNIVERSE)

        # lowest-vol asset (k=0) gets the largest target weight
        ordered = sorted(out["universe"], key=lambda u: -u["targetPct"])
        assert ordered[0]["symbol"] == RISK_UNIVERSE[0]
        assert ordered[0]["name"] == B3_LABELS[RISK_UNIVERSE[0]]

        assert abs(sum(u["targetPct"] for u in out["universe"]) - 100) <= 0.5
        assert abs(sum(u["driftPct"] for u in out["universe"]) - 100) <= 0.5
        for u in out["universe"]:
            assert abs(u["deltaPct"] - (u["targetPct"] - u["driftPct"])) <= 0.2

    def test_trades_filter_and_side(self) -> None:
        out = b3_state(_panel(_days(120)))
        for t in out["trades"]:
            assert t["side"] in ("BUY", "SELL")
            assert abs(t["deltaPct"]) >= 0.5
            assert (t["side"] == "BUY") == (t["deltaPct"] > 0)

    def test_missing_panel_and_short_history_fail_open(self) -> None:
        assert b3_state({})["ok"] is False
        days = _days(3)
        panel = {ts: {d: 100.0 for d in days} for ts in RISK_UNIVERSE}
        assert b3_state(panel)["ok"] is False
