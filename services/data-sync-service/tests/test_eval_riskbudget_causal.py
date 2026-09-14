"""Causality regressions for the B3 / Homeport evaluation scripts.

The vol window ``range(i - 60, i)`` used to wrap at ``i == 60``: ``j - 1``
became ``-1`` and injected the LAST close of the whole series (future data)
into that rebalance's vol estimate. The test asserts the truncated-prefix
property: NAV through index 61 must not change when later bars are appended.
"""

from __future__ import annotations

from datetime import date, timedelta

from scripts.eval_b3_cap import RP_UNIVERSE, _rp_nav_capped
from scripts.eval_harbor_riskbudget import _rp_nav


def _calendar(n: int = 120) -> list[str]:
    # cal[60] is 2025-03-01 (month start) and cal[59] 2025-02-28, so the first
    # month-boundary rebalance happens exactly at i == 60 (the wrap trigger).
    start = date(2024, 12, 31)
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


def _panel(cal: list[str]) -> dict[str, dict[str, float]]:
    px: dict[str, dict[str, float]] = {}
    for k, ts in enumerate(RP_UNIVERSE):
        px[ts] = {d: 1.0 + 0.0005 * (k + 1) * i for i, d in enumerate(cal)}
    # Future-only spike on the LAST bar of one series: a causal vol estimate at
    # i == 60 cannot see it; the negative-index wrap used exactly this value.
    px[RP_UNIVERSE[0]][cal[-1]] *= 50.0
    return px


def test_harbor_riskbudget_rp_nav_prefix_is_causal() -> None:
    cal = _calendar()
    px = _panel(cal)
    full = _rp_nav(px, cal, cost=0.0)
    trunc = _rp_nav(px, cal[:62], cost=0.0)
    assert [round(v, 9) for v in full[:62]] == [round(v, 9) for v in trunc]


def test_b3_cap_rp_nav_prefix_is_causal() -> None:
    cal = _calendar()
    px = _panel(cal)
    full = _rp_nav_capped(px, cal, 0.0, None)
    trunc = _rp_nav_capped(px, cal[:62], 0.0, None)
    assert [round(v, 9) for v in full[:62]] == [round(v, 9) for v in trunc]
