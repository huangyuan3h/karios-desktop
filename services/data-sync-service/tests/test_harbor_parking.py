"""Pure unit tests for the Harbor parking state machine cooldown (H-PARK-C).

No DB, no network. Synthetic ETF panels only.
See docs/designs/parking-cooldown-prereg-2026-09-13.md
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from data_sync_service.service.harbor import (
    MULTI_TS,
    build_harbor_timeline,
    merge_recent_db_closes,
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


class TestHarborTimelineLegs:
    """Timeline carries every leg: full stock list + parking audit trail.

    No DB: synthetic panel / positions / engine NAV only.
    """

    def _inputs(self) -> tuple[list[str], dict, list[dict], dict[str, float]]:
        days = _days(300)
        panel = _panel(other_above=True)
        poses = [
            {
                "ts_code": f"60000{i}.SH",
                "symbol": f"CN:60000{i}",
                "position_pct": 0.1,
                "entry_date": days[0],
            }
            for i in range(5)
        ]
        positions_by_day = [{"date": d, "positions": [dict(p) for p in poses]} for d in days]
        engine_nav = {d: 1.0 for d in days}
        return days, panel, positions_by_day, engine_nav

    def test_rows_carry_parking_markers_and_full_symbols(self) -> None:
        days, panel, positions_by_day, engine_nav = self._inputs()
        out = build_harbor_timeline(
            calendar=days,
            positions_by_day=positions_by_day,
            engine_nav_by_day=engine_nav,
            etf_close=panel,
        )
        rows = out["rows"]
        assert len(rows) == len(days) - 1
        for r in rows:
            assert {"parkedSides", "parkedRetPct", "parkedTrail"} <= set(r)
        assert any(r["parkedSides"] > 0 for r in rows)
        assert any(r["parkedTrail"] for r in rows)
        # 5 tickets held -> all 5 named (the old 3-symbol cap is gone).
        assert rows[0]["positions"] == 5
        assert rows[0]["stockSymbols"] == [f"CN:60000{i}" for i in range(5)]

    def test_parked_blotter_and_held_match_replay(self) -> None:
        days, panel, positions_by_day, engine_nav = self._inputs()
        out = build_harbor_timeline(
            calendar=days,
            positions_by_day=positions_by_day,
            engine_nav_by_day=engine_nav,
            etf_close=panel,
        )
        events = out["parkedBlotter"]
        assert events and events[0]["kind"] == "buy" and events[0]["key"] == "GOLD"
        assert any(e["kind"] == "sell" and e["reason"] == "trail" for e in events)
        held = out["parkedHeld"]
        assert held is not None and held["key"] == "GOLD"
        assert held["ts"] == MULTI_TS["GOLD"]
        # P1 parks the whole idle fraction: 5 x 10% deployed -> 50% parked.
        assert held["weight"] == 0.5
        assert out["summary"]["parkedTrades"] == len(events)
        assert out["summary"]["parkedTrailExits"] == out["summary"]["trailExits"] >= 1


class TestMergeRecentDbClosesBasis:
    """2026-09-17 audit: ETF tail must be scaled onto the CSV (close_adj) basis.

    ``daily.close`` for ETFs is raw and ``adj_factor`` is NULL, so the old
    ``close * COALESCE(adj_factor, 1)`` append created −80%/+193% fake jumps.
    """

    class _FakeCursor:
        def __init__(self, anchor, tail):
            self._anchor = anchor
            self._tail = tail
            self._mode = ""

        def execute(self, sql: str, params) -> None:  # noqa: ANN001
            self._mode = "anchor" if "DESC" in sql else "tail"

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def fetchone(self):
            return self._anchor if self._mode == "anchor" else None

        def fetchall(self):
            return list(self._tail) if self._mode == "tail" else []

    class _FakeConn:
        def __init__(self, cur):
            self._cur = cur

        def cursor(self):
            return self._cur

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _run(self, monkeypatch, out, anchor, tail):
        cur = self._FakeCursor(anchor, tail)
        monkeypatch.setattr(
            "data_sync_service.db.get_connection", lambda: self._FakeConn(cur)
        )
        return merge_recent_db_closes(out, ["513100.SH"])

    def test_tail_scaled_by_overlap_ratio_when_adj_factor_missing(
        self, monkeypatch
    ) -> None:  # noqa: ANN001
        # CSV last 11.009182 (= raw 2.201 x 5.0019); DB tail raw 2.191 was
        # appended verbatim before the fix -> a -80.1% fake day.
        out = {"513100.SH": {"2026-09-11": 11.009182}}
        anchor = (date(2026, 9, 11), 2.201, None)
        tail = [(date(2026, 9, 14), 2.191, None), (date(2026, 9, 15), 2.196, None)]
        merged = self._run(monkeypatch, out, anchor, tail)
        assert merged["513100.SH"]["2026-09-11"] == 11.009182  # history intact
        assert merged["513100.SH"]["2026-09-14"] == pytest.approx(
            2.191 * 11.009182 / 2.201, rel=1e-9
        )
        day_ret = merged["513100.SH"]["2026-09-14"] / 11.009182 - 1.0
        assert abs(day_ret) < 0.01  # continuity, no −80% break

    def test_tail_prefers_stored_adj_factor(self, monkeypatch) -> None:  # noqa: ANN001
        out = {"513100.SH": {"2026-09-11": 11.009182}}
        anchor = (date(2026, 9, 11), 2.201, 5.0019)
        tail = [(date(2026, 9, 14), 2.191, 5.0019)]
        merged = self._run(monkeypatch, out, anchor, tail)
        assert merged["513100.SH"]["2026-09-14"] == pytest.approx(2.191 * 5.0019)

    def test_no_common_basis_falls_back_to_raw_and_keeps_going(
        self, monkeypatch
    ) -> None:  # noqa: ANN001
        out = {"513100.SH": {}}
        tail = [(date(2026, 9, 14), 2.191, None)]
        merged = self._run(monkeypatch, out, None, tail)
        assert merged["513100.SH"]["2026-09-14"] == 2.191
