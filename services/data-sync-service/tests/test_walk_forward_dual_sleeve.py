"""Tests for the dual-pool R5CS sleeve leg + R5 weight bugfix (2026-08-21)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from scripts.run_walk_forward_dual import (  # noqa: E402
    _clamp_pair,
    idle_pct_by_day,
    joint_stats,
    weekly_weights,
)


def _mk_run():
    class _Run:
        def __init__(self, snaps):
            self.positions_by_day = snaps

    return _Run(
        [
            {"date": d, "positions": [{"position_pct": 0.2}, {"position_pct": 0.1}]}
            for d in ("2026-01-05", "2026-01-06", "2026-01-07")
        ]
    )


def test_idle_pct_by_day_sums_positions():
    out = idle_pct_by_day(_mk_run(), ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"])
    assert out["2026-01-05"] == pytest.approx(0.7, abs=0.001)
    assert out["2026-01-08"] == pytest.approx(1.0, abs=0.001)  # no snapshot -> fully idle


def test_joint_stats_r5c_baseline_unchanged_without_sleeve():
    nav = [1.0, 1.05, 1.10, 1.08]
    stats = joint_stats(nav, nav, ["d1", "d2", "d3", "d4"], {"d1": (1.0, 0.0)})
    assert stats["totalNetPnlPct"] == pytest.approx(8.0, abs=0.1)


def test_joint_stats_sleeve_charges_idle_only():
    """CN selected (1,0), idle 0.5, sleeve +10%/day -> pool gains 0.5 * 10%."""
    nav_cn = [1.0, 1.0, 1.0]  # no deployed pnl
    nav_hk = [1.0, 1.0, 1.0]
    sleeve = [1.0, 1.1, 1.21]
    # joint_stats[i] uses calendar[i] — idle must key the DAYS (d2, d3).
    idle = {"CN": {"d2": 0.5, "d3": 0.5}, "HK": {"d2": 0.0, "d3": 0.0}}
    stats = joint_stats(
        nav_cn, nav_hk, ["d1", "d2", "d3"], {"d1": (1.0, 0.0)},
        sleeve_nav=sleeve, idle_by_day=idle,
    )
    # d2: +0.5*10% = +5%; d3: +0.5*10% = +5% -> ~10.25% compounded
    assert stats["totalNetPnlPct"] == pytest.approx(10.25, abs=0.1)


def test_joint_stats_sleeve_full_pool_when_both_weak():
    """(0,0) weights + sleeve leg -> the whole pool earns the sleeve."""
    nav = [1.0, 1.0, 1.0]
    sleeve = [1.0, 1.05, 1.05]
    stats = joint_stats(
        nav, nav, ["d1", "d2", "d3"], {"d1": (0.0, 0.0)},
        sleeve_nav=sleeve, idle_by_day={"CN": {}, "HK": {}},
    )
    assert stats["totalNetPnlPct"] == pytest.approx(5.0, abs=0.1)


def test_weekly_weights_r5c_is_cn_first_not_momentum():
    """2026-08-21 bugfix: R5C must be weights_from_regimes (CN-first), NOT
    the unconditional momentum softmax that used to override it."""
    regimes = {
        "CN": {"2026-01-05": "Strong", "2026-01-12": "Weak", "2026-01-19": "Weak"},
        "HK": {"2026-01-05": "Weak", "2026-01-12": "Strong", "2026-01-19": "Weak"},
    }
    with patch("scripts.run_walk_forward_dual._index_momentum", return_value=0.0):
        w = weekly_weights("2026-01-05", "2026-01-20", "R5C", {}, regimes=regimes)
    assert w["2026-01-05"] == (1.0, 0.0)  # CN strong -> 100% CN
    assert w["2026-01-12"] == (0.0, 1.0)  # HK strong -> 100% HK
    assert w["2026-01-19"] == (0.0, 0.0)  # both weak -> cash


def test_weekly_weights_r5cs_matches_r5c():
    """R5CS uses the SAME CN-first weights as R5C (only the sleeve leg differs)."""
    regimes = {
        "CN": {"2026-01-05": "Strong"},
        "HK": {"2026-01-05": "Weak"},
    }
    with patch("scripts.run_walk_forward_dual._index_momentum", return_value=0.0):
        w5c = weekly_weights("2026-01-05", "2026-01-12", "R5C", {}, regimes=regimes)
        w5cs = weekly_weights("2026-01-05", "2026-01-12", "R5CS", {}, regimes=regimes)
    assert w5c == w5cs


def test_weekly_weights_r5a_both_strong_is_5050():
    """R5A's both-strong 50/50 branch (was dead code: 'R5a' != 'R5A')."""
    regimes = {
        "CN": {"2026-01-05": "Strong"},
        "HK": {"2026-01-05": "Strong"},
    }
    with patch("scripts.run_walk_forward_dual._index_momentum", return_value=0.0):
        w = weekly_weights("2026-01-05", "2026-01-12", "R5A", {}, regimes=regimes)
    assert w["2026-01-05"] == (0.5, 0.5)


def test_clamp_pair():
    lo, hi = _clamp_pair(0.1)
    assert lo == pytest.approx(0.2, abs=0.001)
    assert hi == pytest.approx(0.8, abs=0.001)
    lo, hi = _clamp_pair(0.9)
    assert lo == pytest.approx(0.8, abs=0.001)
    assert hi == pytest.approx(0.2, abs=0.001)