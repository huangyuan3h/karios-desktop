"""Twin-Star intraday approximate signal tests (no DB — mocks data loaders).

The 12:30 full-market snapshot stands in for today's close (user buys at
14:30 at an approximately closing price); the S-gap screen re-runs with the
same formulas as the frozen engine (gap = open/pre_close-1, amp =
(high-low)/close, R-wide = close > MA20 share, low-vol bottom-1/3, and the
T-1 limit-locked filter applied to the snapshot price).
"""

from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.service import twin_star_intraday as m


def _series(closes: list[float], day0: str = "2026-08-01") -> list[dict]:
    rows = []
    for i, c in enumerate(closes):
        prev = closes[i - 1] if i > 0 else c
        rows.append(
            {
                "date": f"2026-08-{i+1:02d}" if day0 == "2026-08-01" else day0,
                "open": c,
                "high": c * 1.02,
                "low": c * 0.98,
                "close": c,
                "pre_close": prev,
                "amount": 1e6,
            }
        )
    return rows


def _mk_per_ts() -> dict:
    """20-day flat history (close=10 -> MA20=10) for three symbols."""
    per_ts: dict = {}
    for ts in ("600001.SH", "600002.SH", "600003.SH"):
        per_ts[ts] = [
            {
                "date": f"2026-08-{i:02d}",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.0,
                "pre_close": 10.0,
                "amount": 1e6,
            }
            for i in range(1, 21)
        ]
    return per_ts


def _flat_snapshot(today: str = "2026-08-20") -> dict:
    """Snapshot with a single gap stock (gap>3%) plus normal ones."""
    return {
        # gap 5% via open vs pre_close 10.0, amp (high-low)/close small
        "600001.SH": {
            "open": 10.5,
            "high": 10.55,
            "low": 10.45,
            "close": 10.5,
            "pre_close": 10.0,
            "amount": 1e6,
        },
        # no gap (open == pre_close)
        "600002.SH": {
            "open": 10.0,
            "high": 10.1,
            "low": 9.95,
            "close": 10.0,
            "pre_close": 10.0,
            "amount": 1e6,
        },
        # gap but limit-locked AND high amplitude so it is NOT in the top 1/3
        # (strict: rank all gaps first, then drop locked — do not refill).
        "600003.SH": {
            "open": 10.96,
            "high": 11.0,
            "low": 10.0,
            "close": 10.96,
            "pre_close": 10.0,
            "amount": 1e6,
        },
    }


class TestBuildIntradaySat:
    @pytest.fixture(autouse=True)
    def _mock_data(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.today = date(2026, 8, 20)
        monkeypatch.setattr(
            m,
            "fetch_market_snapshot",
            lambda: _flat_snapshot("2026-08-20"),
        )
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [f"2026-08-{i:02d}" for i in range(1, 21)])
        monkeypatch.setattr(m, "_load_rows", lambda s, e: _mk_per_ts())
        monkeypatch.setattr(
            m,
            "_load_mv",
            lambda s, e: {
                "2026-08-19": {
                    "600001.SH": 1e5,
                    "600002.SH": 1e5,
                    "600003.SH": 1e5,
                }
            },
        )

    def test_signal_shape_and_approx_flag(self) -> None:
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        assert sat["asOf"] == "2026-08-20"
        assert sat["approx"] is True
        assert sat.get("snapshotAt")

    def test_limit_locked_filtered_out(self) -> None:
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        # 600003 gapped 9.6% and the snapshot price is at the 10% limit -> excluded.
        assert "600003.SH" not in [c["ts"] for c in sat["candidates"]]
        # 600001 gapped 5% and is executable -> included.
        assert "600001.SH" in [c["ts"] for c in sat["candidates"]]
        assert sat["gapCount"] == 2

    def test_breadth_and_gate(self) -> None:
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        # MA20=10.0; snapshot closes 10.5/10.96 above, 10.0 equal -> 2/3 above
        assert sat["breadth"] == pytest.approx(2 / 3, abs=0.001)
        assert sat["gateOpen"] is True

    def test_cache_roundtrip(self, tmp_path: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        m.cache_intraday_sat(sat, self.today)
        loaded = m.load_intraday_sat(self.today)
        assert loaded == sat
        assert m.load_intraday_sat(date(2026, 8, 21), lookback=False) is None
        overnight = m.load_intraday_sat(date(2026, 8, 21))
        assert overnight is not None
        assert overnight.get("heldOvernight") is True

    def test_missing_snapshot_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(m, "fetch_market_snapshot", lambda: {})
        assert m.build_intraday_sat(self.today) is None


class TestSessionWindow:
    def test_session_date_before_9am_is_yesterday(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        now = datetime(2026, 9, 2, 8, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
        assert m.session_date(now) == date(2026, 9, 1)

    def test_session_date_after_9am_is_today(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        now = datetime(2026, 9, 2, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        assert m.session_date(now) == date(2026, 9, 2)

    def test_live_window(self) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        assert m.in_live_tape_window(datetime(2026, 9, 1, 10, 0, tzinfo=tz)) is True
        assert m.in_live_tape_window(datetime(2026, 9, 1, 8, 0, tzinfo=tz)) is False
        assert m.in_live_tape_window(datetime(2026, 9, 1, 15, 1, tzinfo=tz)) is False
        assert m.in_live_tape_window(datetime(2026, 9, 5, 10, 0, tzinfo=tz)) is False  # Saturday

    def test_overnight_skips_refresh(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
        called = {"n": 0}

        def boom() -> dict:
            called["n"] += 1
            return {}

        monkeypatch.setattr(m, "build_intraday_sat", boom)
        cached = {"asOf": "2026-09-01", "snapshotAt": "2026-09-01T15:00:00+08:00", "candidates": []}
        m.cache_intraday_sat(cached, date(2026, 9, 1))
        now = datetime(2026, 9, 2, 8, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        out = m.maybe_refresh_intraday_sat(now=now)
        assert out is not None
        assert out["asOf"] == "2026-09-01"
        assert called["n"] == 0