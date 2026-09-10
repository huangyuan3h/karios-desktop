"""TIP-017 risk-state gate tests — pure window/state logic + sync round-trips.

Pure-function tests use tiny MA windows (ma_window=2/price_ma=2) so synthetic
series stay readable. Sync tests use a fake pro factory (H3 convention) and
teardown their sentinel rows (DB discipline: prefix dates 1999-01-01).
"""

from __future__ import annotations

from data_sync_service.service.risk_state_gate import (
    breadth_state_by_day,
    national_team_state_by_day,
    state_runs,
)


def _cal(days: int, start: str = "2026-01-01") -> list[str]:
    from datetime import date as _d
    from datetime import timedelta as _td

    base = _d.fromisoformat(start)
    return [(base + _td(days=i)).isoformat() for i in range(days)]


def _series(values: list[float], cal: list[str], *, every: int = 1) -> list[tuple[str, float]]:
    return [(cal[i], v) for i, v in enumerate(values) if i % every == 0]


def _six_series(values: list[float], cal: list[str]) -> dict[str, list[tuple[str, float]]]:
    return {name: _series(values, cal) for name in ("CN", "HK", "GOLD", "OIL", "NASDAQ", "BOND10")}


class TestBreadthState:
    def test_trigger_when_all_below(self) -> None:
        cal = _cal(6)
        # ma_window=2: closes [1, 1] → equal → NOT above (below). All 6 below.
        series = _six_series([1.0, 1.0, 1.0, 1.0, 1.0, 1.0], cal)
        state = breadth_state_by_day(series, cal, k=2, release="breadth", ma_window=2)
        # day0 no data; day1 held (MA2 warmup completes at data-through-d1);
        # day2 onward breadth=0 < 2 → ON
        assert [state[d] for d in cal] == [False, False, True, True, True, True]

    def test_release_breadth(self) -> None:
        cal = _cal(6)
        # first 2 closes low (below MA2), then rising closes → above
        values = [1.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        series = _six_series(values, cal)
        state = breadth_state_by_day(series, cal, k=2, release="breadth", ma_window=2)
        # above(d) = latest close > MA2 of closes so far → strictly rising after d2:
        # flags = [F, F, T, T, T, T]. Trigger day2 (breadth=0), release day3 (breadth=6 ≥ 2).
        assert [state[d] for d in cal] == [False, False, True, False, False, False]

    def test_release_price_beats_stale_breadth(self) -> None:
        cal = _cal(5)
        rising = [1.0, 1.0, 2.0, 3.0, 4.0]
        series = _six_series(rising, cal)
        state = breadth_state_by_day(series, cal, k=2, release="price", ma_window=2, price_ma=2)
        # trigger day2 (breadth=0 < 2); CN price release at day3 (close > MA2)
        assert state[cal[2]] is True
        assert state[cal[3]] is False
        assert state[cal[4]] is False

    def test_release_cooldown(self) -> None:
        cal = _cal(8)
        flat = [1.0] * 8
        series = _six_series(flat, cal)
        state = breadth_state_by_day(
            series, cal, k=2, release="cooldown", ma_window=2, cooldown_days=3
        )
        # ON from day2 (warmup), cooldown 3 sessions → OFF at day5, re-ON day6
        flags = [state[d] for d in cal]
        assert flags == [False, False, True, True, True, False, True, True]

    def test_data_vacuum_holds_state(self) -> None:
        """Pre-reg §1 refinement: >1 series missing warmup → state held (no fake ON)."""
        cal = _cal(5)
        flat = [1.0] * 5
        series = _six_series(flat, cal)
        # two series without MA warmup (OIL pre-listing analog) → held
        series["OIL"] = series["OIL"][:1]
        series["BOND10"] = series["BOND10"][:1]
        state = breadth_state_by_day(series, cal, k=2, release="breadth", ma_window=2)
        assert not any(state.values())


class TestNationalTeamState:
    def test_trigger_and_release(self) -> None:
        cal = _cal(6)
        index = [(d, 100.0) for d in cal]  # flat → below MA2? 100 > MA2(100,100)=100 → False
        shares = {
            code: [(d, 100.0) for d in cal] for code in ("510300.SH", "510500.SH")
        }
        state = national_team_state_by_day(index, shares, cal, n=2, ma_window=2)
        # index never above; share delta = 0 ≤ 0 → ON from day3 (needs n+1=3 share pts)
        assert [state[d] for d in cal] == [False, False, False, True, True, True]

    def test_share_growth_releases(self) -> None:
        cal = _cal(6)
        index = [(d, 100.0) for d in cal]
        shares = {code: [(d, 100.0 + 10.0 * i) for i, d in enumerate(cal)] for code in ("510300.SH",)}
        state = national_team_state_by_day(index, shares, cal, n=2, ma_window=2)
        assert not any(state.values())

    def test_insufficient_share_history_off(self) -> None:
        cal = _cal(4)
        index = [(d, 100.0) for d in cal]
        shares = {"510300.SH": [(cal[0], 100.0)]}
        state = national_team_state_by_day(index, shares, cal, n=2, ma_window=2)
        assert not any(state.values())


class TestNationalTeamLoader:
    def test_for_calendar_uses_lookback_and_pool(self, monkeypatch) -> None:
        from data_sync_service.service import risk_state_gate as rsg

        captured: dict = {}

        def fake_close(table, ts, start, end):
            captured["ts"] = ts
            captured["start"] = start
            return [(d, 100.0) for d in _cal(5)]

        def fake_share(codes, start, end):
            captured["codes"] = tuple(codes)
            captured["share_start"] = start
            return {c: [(d, 100.0) for d in _cal(5)] for c in codes}

        monkeypatch.setattr(rsg, "_load_close_series", fake_close)
        monkeypatch.setattr(rsg, "load_etf_share_series", fake_share)
        cal = _cal(3, start="2026-06-01")
        state = rsg.national_team_state_for_calendar(cal, lookback_days=400)
        assert captured["ts"] == "000300.SH"
        assert captured["codes"] == rsg.BROAD_ETF_CODES
        assert captured["start"] < cal[0]  # lookback warms MA200 before window
        assert captured["share_start"] == captured["start"]
        assert set(state) == set(cal)

    def test_state_on_fail_open_without_data(self, monkeypatch) -> None:
        from data_sync_service.service import risk_state_gate as rsg

        class _Cur:
            def execute(self, *a):  # noqa: ANN002
                pass

            def fetchall(self):
                return []

            def __enter__(self):
                return self

            def __exit__(self, *a):  # noqa: ANN002
                return False

        class _Conn:
            def cursor(self):
                return _Cur()

            def __enter__(self):
                return self

            def __exit__(self, *a):  # noqa: ANN002
                return False

        monkeypatch.setattr(rsg, "get_connection", lambda: _Conn())
        assert rsg.national_team_state_on("2026-06-01") is False


def test_state_runs_contiguous() -> None:
    cal = _cal(5)
    state = {d: i in (1, 2, 4) for i, d in enumerate(cal)}
    windows = state_runs(state, cal)
    assert windows == ((cal[1], cal[2]), (cal[4], cal[4]))


def test_above_flags_uses_lookback_closes() -> None:
    """Regression: pre-window (lookback) closes must warm the MA at window start.

    300 lookback closes at 50.0, then window closes rising at 51+ → every
    calendar day must be above MA(2) immediately (was: cold-MA fail-closed
    for the first ma_window sessions, manufacturing artificial gates).
    """
    from data_sync_service.service.risk_state_gate import _above_flags

    lookback = [("2026-01-01", 50.0)] * 300
    cal = _cal(3, start="2027-01-01")
    series = lookback + [(cal[0], 51.0), (cal[1], 52.0), (cal[2], 53.0)]
    flags = _above_flags(series, cal, ma=2)
    assert flags == [True, True, True]
