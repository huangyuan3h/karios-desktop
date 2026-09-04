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
        monkeypatch.setattr(m, "fill_candidate_names", lambda *a, **k: None)
        # Hermetic: the fixture date must read as a trading day without a DB.
        # (CI runs on a fresh-migrated empty DB; trade_calendar has no rows.)
        monkeypatch.setattr(m, "_is_trading_day", lambda day: True)

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

    def test_snapshot_name_on_candidates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def snap() -> dict:
            s = _flat_snapshot("2026-08-20")
            s["600001.SH"]["name"] = "测试股"
            return s

        monkeypatch.setattr(m, "fetch_market_snapshot", snap)
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        hit = next(c for c in sat["candidates"] if c["ts"] == "600001.SH")
        assert hit["name"] == "测试股"

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

    def test_c1_skips_runup_over_3pct_strict_no_refill(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def snap() -> dict:
            s = _flat_snapshot("2026-08-20")
            # 600001: 14:30/open-1 = 10.9/10.5-1 = 3.8% > 3% -> C1 skip
            s["600001.SH"]["close"] = 10.9
            s["600001.SH"]["high"] = 11.0
            s["600001.SH"]["low"] = 10.4
            return s

        monkeypatch.setattr(m, "fetch_market_snapshot", snap)
        sat = m.build_intraday_sat(self.today)
        assert sat is not None
        assert "600001.SH" not in [c["ts"] for c in sat["candidates"]]
        assert "600001.SH" in [c["ts"] for c in sat["skippedC1"]]
        assert sat["skippedC1Count"] == 1
        assert sat["entryFilter"] == "c1_3pct"
        assert sat["exitHhmm"] == "1430"


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


class TestIntradaySnapshotStatus:
    """12:30 is the first snapshot the 14:30 fill must have (E3)."""

    def test_before_1230_missing_is_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        monkeypatch.setattr(m, "_read_cache", lambda day: None)
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [s])
        now = datetime(2026, 9, 2, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        st = m.intraday_snapshot_status(now=now)
        assert st["ok"] is True
        assert st["missing"] is False
        assert st["required"] is False

    def test_after_1230_missing_is_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        monkeypatch.setattr(m, "_read_cache", lambda day: None)
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [s])
        now = datetime(2026, 9, 2, 12, 35, tzinfo=ZoneInfo("Asia/Shanghai"))
        st = m.intraday_snapshot_status(now=now)
        assert st["missing"] is True
        assert st["ok"] is False
        assert st["reason"] == "no_session_snapshot"

    def test_after_close_today_file_ok_even_if_old(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        sat = {"snapshotAt": "2026-09-02T14:59:00+08:00", "approx": True}
        monkeypatch.setattr(m, "_read_cache", lambda day: sat)
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [s])
        now = datetime(2026, 9, 2, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        st = m.intraday_snapshot_status(now=now)
        assert st["ok"] is True
        assert st["stale"] is False

    def test_live_stale_when_age_over_20min(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        sat = {"snapshotAt": "2026-09-02T12:31:00+08:00"}
        monkeypatch.setattr(m, "_read_cache", lambda day: sat)
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [s])
        now = datetime(2026, 9, 2, 13, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        st = m.intraday_snapshot_status(now=now)
        assert st["stale"] is True
        assert st["reason"] == "snapshot_stale"

    def test_holiday_weekday_not_required(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from datetime import datetime
        from zoneinfo import ZoneInfo

        monkeypatch.setattr(m, "_read_cache", lambda day: None)
        monkeypatch.setattr(m, "_load_calendar", lambda s, e: [])
        now = datetime(2026, 9, 2, 13, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        st = m.intraday_snapshot_status(now=now)
        assert st["required"] is False
        assert st["ok"] is True


def test_intraday_job_records_failure_after_1230(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from data_sync_service.scheduler import twin_star_intraday_job as job

    recorded: list[dict] = []
    monkeypatch.setattr(job, "in_live_tape_window", lambda now=None: True)
    monkeypatch.setattr(
        job, "now_cn", lambda: datetime(2026, 9, 2, 12, 40, tzinfo=ZoneInfo("Asia/Shanghai"))
    )
    monkeypatch.setattr(job, "maybe_refresh_intraday_sat", lambda now=None: {"heldOvernight": True})
    monkeypatch.setattr(
        job,
        "intraday_snapshot_status",
        lambda now=None: {
            "ok": False,
            "required": True,
            "missing": True,
            "reason": "no_session_snapshot",
        },
    )
    monkeypatch.setattr(job, "_record_once", lambda **kw: recorded.append(kw))
    job.run()
    assert recorded == [{"success": False, "error": "no_session_snapshot"}]