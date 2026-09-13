"""Pure unit tests for the Harbor parking state machine cooldown (H-PARK-C).

No DB, no network. Synthetic ETF panels only.
See docs/designs/parking-cooldown-prereg-2026-09-13.md
"""

from __future__ import annotations

from datetime import date, timedelta

from data_sync_service.service.harbor import (
    MULTI_TS,
    parking_replay,
    pick_parking,
)


def _days(n: int, start: date = date(2024, 1, 1)) -> list[str]:
    out: list[str] = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _gold_dip(days: list[str]) -> dict[str, float]:
    """Rise 100->220 over 260 sessions, dip -9.1% to 200, then recover."""
    n = len(days)
    vals: list[float] = []
    for i in range(n):
        if i <= 259:
            v = 100 + 120 * i / 259
        elif i <= 269:
            v = 220 - 2 * (i - 259)
        else:
            v = 200 + 30 * (i - 269) / (n - 1 - 269)
        vals.append(round(v, 4))
    return dict(zip(days, vals, strict=True))


def _panel(other_above: bool) -> dict[str, dict[str, float]]:
    days = _days(300)
    n = len(days)
    if other_above:
        oil = [round(100 + 5 * i / (n - 1), 4) for i in range(n)]
        bond = [round(100 + 1.5 * i / (n - 1), 4) for i in range(n)]
    else:
        oil = [round(120 - 25 * i / (n - 1), 4) for i in range(n)]
        bond = [round(120 - 20 * i / (n - 1), 4) for i in range(n)]
    return {
        MULTI_TS["GOLD"]: _gold_dip(days),
        MULTI_TS["OIL"]: dict(zip(days, oil, strict=True)),
        MULTI_TS["BOND10"]: dict(zip(days, bond, strict=True)),
    }


def _find_trail(records: list[dict]) -> int:
    for i, rec in enumerate(records):
        if rec["trail_exit"]:
            return i
    raise AssertionError("synthetic panel never triggered a trail exit")


class TestPickParkingExclude:
    def test_exclude_falls_back_to_next_eligible(self) -> None:
        days = _days(300)
        px = _panel(other_above=True)
        base = pick_parking(px, days[269])
        assert base is not None and base["key"] == "GOLD"
        assert pick_parking(px, days[269], exclude_keys={"GOLD"})["key"] == "OIL"
        assert pick_parking(px, days[269], exclude_keys={"GOLD", "OIL", "BOND10"}) is None

    def test_exclude_keeps_raw_diagnostics(self) -> None:
        days = _days(300)
        px = _panel(other_above=True)
        out = pick_parking(px, days[269], exclude_keys={"GOLD"})
        assert out["all_mom"]["GOLD"] is not None
        assert out["all_above"]["GOLD"] is True


class TestParkingCooldown:
    def test_default_is_incumbent_and_reenters_next_session(self) -> None:
        days = _days(300)
        px = _panel(other_above=True)
        default = parking_replay(px, days)
        explicit = parking_replay(px, days, cooldown_days=0)
        assert [r["pick_key"] for r in default] == [r["pick_key"] for r in explicit]
        assert not any(r["cooldown_active"] for r in default)
        t = _find_trail(default)
        assert default[t]["pick_key"] == "REPO"
        assert default[t + 1]["pick_key"] == "GOLD"

    def test_cooldown_rotates_then_returns_to_exited_key(self) -> None:
        days = _days(300)
        px = _panel(other_above=True)
        recs = parking_replay(px, days, cooldown_days=2)
        t = _find_trail(recs)
        assert recs[t]["pick_key"] == "REPO"
        assert recs[t + 1]["pick_key"] == "OIL"
        assert recs[t + 1]["cooldown_active"] is True
        assert recs[t + 2]["pick_key"] == "OIL"
        assert recs[t + 2]["cooldown_active"] is True
        assert recs[t + 3]["pick_key"] == "GOLD"
        assert recs[t + 3]["cooldown_active"] is False

    def test_cooldown_one_session(self) -> None:
        days = _days(300)
        px = _panel(other_above=True)
        recs = parking_replay(px, days, cooldown_days=1)
        t = _find_trail(recs)
        assert recs[t + 1]["pick_key"] == "OIL"
        assert recs[t + 2]["pick_key"] == "GOLD"

    def test_cooldown_goes_to_repo_when_no_other_candidate(self) -> None:
        days = _days(300)
        px = _panel(other_above=False)
        incumbent = parking_replay(px, days)
        t = _find_trail(incumbent)
        assert incumbent[t + 1]["pick_key"] == "GOLD"
        recs = parking_replay(px, days, cooldown_days=3)
        for offset in (0, 1, 2, 3):
            assert recs[t + offset]["pick_key"] == "REPO"
        assert recs[t + 4]["pick_key"] == "GOLD"
