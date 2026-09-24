"""Unit tests for the hysteresis parking sleeve (research-only).

Synthetic panels, hand-computed expectations. No DB, no network.
"""

from __future__ import annotations

import pytest

from data_sync_service.service.parking_sleeve import HYST_BAND, hysteresis_parking_replay


def _px() -> dict[str, dict[str, float]]:
    """GOLD trends up, OIL chops; 250+ sessions for MA200/mom60 relevance.

    Hysteresis tests only need relative moms; long flat history keeps
    MA200 satisfied everywhere.
    """
    days = [f"2026-03-{d:02d}" for d in range(1, 32)]
    days += [f"2026-04-{d:02d}" for d in range(1, 31)]
    hist = [f"2025-{((i // 28) % 12) + 1:02d}-{((i % 28) + 1):02d}" for i in range(280)]
    cal = hist + days
    gold = {d: 10.0 for d in hist}
    oil = {d: 5.0 for d in hist}
    # March: GOLD rallies +30% (mom leader), OIL flat.
    for i, d in enumerate(days[:31]):
        gold[d] = 10.0 * (1 + 0.30 * (i + 1) / 31)
        oil[d] = 5.0
    # April: GOLD fades back toward 11, OIL rallies +30% -> takes a wide lead.
    for i, d in enumerate(days[31:]):
        gold[d] = 13.0 - 2.0 * (i + 1) / 30
        oil[d] = 5.0 * (1 + 0.30 * (i + 1) / 30)
    return {
        # Only GOLD/OIL have full history; NASDAQ/BOND10 absent -> covered < 3
        # would force REPO. Pad them flat so coverage passes (mom 0, never picked).
        "518880.SH": gold,
        "513350.SH": oil,
        "513110.SH": {d: 2.0 for d in cal},
        "511260.SH": {d: 100.0 for d in cal},
    }


def test_picks_momentum_leader_and_trails() -> None:
    px = _px()
    days = sorted({d for m in px.values() for d in m})
    recs = hysteresis_parking_replay(px, [d for d in days if d >= "2026-03-01"], band=0.0)
    by_day = {r["date"]: r for r in recs}
    # GOLD leads all March (mom gap >> 0) -> held throughout March.
    assert by_day["2026-03-15"]["pick_key"] == "GOLD"
    # Late April: OIL leads by a wide margin -> rotates despite band.
    assert by_day["2026-04-28"]["pick_key"] == "OIL"
    assert all(r["pick_ts"] in ("518880.SH", "513350.SH", "GC001") for r in recs)


def test_hysteresis_blocks_noise_rotation() -> None:
    px = _px()
    days = sorted({d for m in px.values() for d in m})
    cal = [d for d in days if d >= "2026-03-01"]
    free = hysteresis_parking_replay(px, cal, band=0.0)
    gated = hysteresis_parking_replay(px, cal, band=HYST_BAND)
    # Band 0 rotates on any leadership flip; H2 holds through sub-2pt gaps.
    free_sides = sum(r["sides"] for r in free)
    gated_sides = sum(r["sides"] for r in gated)
    assert gated_sides <= free_sides
    # Shape compatibility with parking_replay records.
    for r in gated:
        assert set(r) >= {"date", "prev", "pick_key", "pick_ts", "want_key",
                          "parking_ret", "sides", "trail_exit", "cooldown_active"}
        assert r["cooldown_active"] is False


def _px_leadership_flip() -> dict[str, dict[str, float]]:
    """GOLD leads; OIL overtakes by <2pt (must be BLOCKED), then by >2pt."""
    hist = [f"2025-{((i // 28) % 12) + 1:02d}-{((i % 28) + 1):02d}" for i in range(280)]
    days = [f"2026-05-{d:02d}" for d in range(1, 32)]
    cal = hist + days
    gold = {d: 10.0 for d in hist}
    oil = {d: 5.0 for d in hist}
    for i, d in enumerate(days):
        # GOLD leads at +30% mom60 through 05-13, then +33% (a real return on
        # the blocked day, so REPO would be distinguishable from holding).
        gold[d] = 13.0 if i < 13 else 13.3
        # OIL: +31% mom60 from 05-13 (gap 1pt -> blocked), +38% from 05-22
        # (gap 5pt -> rotates).
        if i < 12:
            oil[d] = 5.0
        elif i < 21:
            oil[d] = 6.55
        else:
            oil[d] = 6.90
    return {
        "518880.SH": gold,
        "513350.SH": oil,
        "513110.SH": {d: 2.0 for d in cal},
        "511260.SH": {d: 100.0 for d in cal},
    }


def test_blocked_rotation_keeps_incumbent_leg() -> None:
    """2026-09-17 audit regression: band block = HOLD, never sell to REPO."""
    px = _px_leadership_flip()
    cal = sorted({d for m in px.values() for d in m})
    recs = hysteresis_parking_replay(px, cal)
    by_day = {r["date"]: r for r in recs}

    # 05-14: OIL's 05-13 close leads by ~1pt -> blocked. The incumbent GOLD is
    # kept, no sell, and the day return is GOLD's +2.31% (the old bug sold to
    # cash and booked 0).
    blocked = by_day["2026-05-14"]
    assert blocked["pick_key"] == "GOLD" and blocked["pick_ts"] == "518880.SH"
    assert blocked["sides"] == 0
    assert blocked["hyst_blocked"] is True
    assert blocked["parking_ret"] == pytest.approx(13.3 / 13.0 - 1.0)
    assert blocked["want_key"] == "GOLD"  # effective target = incumbent

    # 05-23: OIL clears the 2pt band -> real rotation (sell + buy).
    rotate = by_day["2026-05-23"]
    assert rotate["pick_key"] == "OIL" and rotate["pick_ts"] == "513350.SH"
    assert rotate["sides"] == 2
    assert rotate["hyst_blocked"] is False

    # Never a REPO day in the May window while a candidate exists.
    may = [d for d in sorted(by_day) if d >= "2026-05-01"]
    assert all(by_day[d]["pick_key"] != "REPO" for d in may)


def test_trail_exit_parks_repo_then_rotates_next_day() -> None:
    px = _px()
    # Crash GOLD -50% at the 03-20 close. T+1 mechanics: the crash is realized
    # on 03-20; the trail fires on 03-21 (prev close below peak -8%) and the
    # exit day always parks in REPO (no same-day re-entry, canonical rule);
    # rotation resumes the next session.
    px["518880.SH"] = dict(px["518880.SH"])
    # Sustained crash (not a one-day bad tick): GOLD prints 6.0 from 03-20
    # through April, so it stays MA200-ineligible and OIL takes over.
    for d in [dd for dd in px["518880.SH"] if dd >= "2026-03-20"]:
        px["518880.SH"][d] = 6.0
    days = sorted({d for m in px.values() for d in m})
    recs = hysteresis_parking_replay(px, [d for d in days if d >= "2026-03-01"])
    by_day = {r["date"]: r for r in recs}
    assert by_day["2026-03-21"]["trail_exit"] is True
    assert by_day["2026-03-21"]["pick_key"] == "REPO"
    assert by_day["2026-03-22"]["pick_key"] == "OIL"
