"""twin_star_intraday uncovered branches (H1): EM fetch, guards, cache, refresh."""

from __future__ import annotations

import sys
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.service import twin_star_intraday as m

CN = ZoneInfo("Asia/Shanghai")


def _dt(h: int, minute: int = 0, day: int = 2) -> datetime:
    return datetime(2026, 9, day, h, minute, tzinfo=CN)


# -- EM snapshot fetch -----------------------------------------------------------------


def test_em_snapshot_params_shape() -> None:
    p = m._em_snapshot_params(3)
    assert p["pn"] == "3" and p["pz"] == str(m.SNAPSHOT_PAGE_SIZE)
    assert "f12" in p["fields"] and "m:0+t:6" in p["fs"]


def test_em_snapshot_request_fails_over_and_raises(monkeypatch) -> None:
    calls = {"n": 0}

    def _flaky(url, params=None, referer=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("push2 down")
        return {"ok": True}

    monkeypatch.setattr(m, "em_get_json", _flaky)
    assert m._em_snapshot_request({}) == {"ok": True}

    monkeypatch.setattr(m, "em_get_json", lambda **k: (_ for _ in ()).throw(RuntimeError("down")))
    with pytest.raises(RuntimeError, match="push2"):
        m._em_snapshot_request({})


def _row(code, market="1", close=10.5, pre=10.0, name="平安"):
    return {"f12": code, "f13": market, "f14": name, "f2": close, "f18": pre,
            "f17": 10.4, "f15": 10.6, "f16": 10.3, "f6": 1e6}


def test_fetch_market_snapshot_paging_and_filters(monkeypatch) -> None:
    pages = [
        {"data": {"total": 2, "diff": [
            _row("600001"),                       # SH valid
            _row("000001", market="0"),            # SZ valid
            _row("BJ123456"),                      # Beijing → skip
            _row(""),                              # empty code → skip
            _row("600002", close=0),               # zero close → skip
            _row("600003", pre=0),                 # zero pre_close → skip
            _row("600004", name="-"),              # dash name → no name key
        ]}},
        {"data": {"total": 2, "diff": []}},       # empty page → stop
    ]
    monkeypatch.setattr(m, "em_get_json", lambda *a, **k: pages.pop(0))
    out = m.fetch_market_snapshot()
    assert set(out) == {"600001.SH", "000001.SZ", "600004.SH"}
    assert out["600001.SH"]["name"] == "平安"
    assert "name" not in out["600004.SH"]


def test_fetch_market_snapshot_total_break_and_none(monkeypatch) -> None:
    monkeypatch.setattr(m, "em_get_json", lambda *a, **k: {"data": {"total": 1, "diff": [_row("600001")]}})
    out = m.fetch_market_snapshot()
    assert list(out) == ["600001.SH"]  # total reached → single page

    monkeypatch.setattr(m, "em_get_json", lambda *a, **k: None)
    assert m.fetch_market_snapshot() == {}


# -- calendar / parse guards -----------------------------------------------------------------


def test_is_trading_day_db_and_failopen(monkeypatch) -> None:
    class _Cur:
        def __init__(self, row):
            self._row = row

        def execute(self, *a):
            pass

        def fetchone(self):
            return self._row

    class _Conn:
        def __init__(self, row):
            self._row = row

        def cursor(self):
            return _Cur(self._row)

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda url: _Conn((True,))))
    assert m._is_trading_day("2026-09-02") is True
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda url: _Conn(None)))
    assert m._is_trading_day("2026-09-03") is False

    def _boom(url):
        raise RuntimeError("db down")

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_boom))
    assert m._is_trading_day("2026-09-02") is True  # fail-open


def test_f_parse() -> None:
    assert m._f(None) is None
    assert m._f("-") is None
    assert m._f("abc") is None
    assert m._f("12.5") == 12.5


def test_naive_datetimes_assumed_shanghai(monkeypatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.is_non_trading_day", lambda d: False
    )
    assert m.session_date(datetime(2026, 9, 2, 8, 0)) == date(2026, 9, 1)
    assert m.session_date(datetime(2026, 9, 2, 10, 0)) == date(2026, 9, 2)
    assert m.in_live_tape_window(datetime(2026, 9, 2, 10, 0)) is True
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.is_non_trading_day", lambda d: True
    )
    assert m.in_live_tape_window(datetime(2026, 9, 2, 10, 0)) is False
    monkeypatch.setattr(m, "_read_cache", lambda day: None)
    monkeypatch.setattr(m, "_load_calendar", lambda s, e: [s])
    st = m.intraday_snapshot_status(now=datetime(2026, 9, 2, 10, 0))
    assert st["ok"] is True


def test_is_cn_session_day_failover(monkeypatch) -> None:
    monkeypatch.setattr(m, "_load_calendar", lambda s, e: (_ for _ in ()).throw(RuntimeError("db")))
    assert m._is_cn_session_day(date(2026, 9, 2)) is True   # Wednesday
    assert m._is_cn_session_day(date(2026, 9, 5)) is False  # Saturday


def test_snapshot_age_seconds_edges() -> None:
    assert m.snapshot_age_seconds(None) is None
    assert m.snapshot_age_seconds({}) is None
    assert m.snapshot_age_seconds({"snapshotAt": 123}) is None
    assert m.snapshot_age_seconds({"snapshotAt": "not-a-date"}) is None
    now = _dt(13, 0)
    assert m.snapshot_age_seconds({"snapshotAt": "2026-09-02T12:00:00"}, now=now) == pytest.approx(3600.0)
    assert m.snapshot_age_seconds({"snapshotAt": "2026-09-02T12:00:00+08:00"}, now=now) == pytest.approx(3600.0)


# -- build guards -------------------------------------------------------------------------------


def test_build_load_failure_no_cal_not_trading(monkeypatch) -> None:
    monkeypatch.setattr(m, "fetch_market_snapshot", lambda: {"600001.SH": {}})
    monkeypatch.setattr(m, "_load_calendar", lambda s, e: (_ for _ in ()).throw(RuntimeError("db")))
    assert m.build_intraday_sat(date(2026, 9, 2)) is None

    monkeypatch.setattr(m, "_load_calendar", lambda s, e: [])
    monkeypatch.setattr(m, "_load_rows", lambda s, e: {})
    monkeypatch.setattr(m, "_load_mv", lambda s, e: {})
    assert m.build_intraday_sat(date(2026, 9, 2)) is None

    monkeypatch.setattr(m, "_load_calendar", lambda s, e: ["2026-09-01"])
    monkeypatch.setattr(m, "_is_trading_day", lambda day: False)
    assert m.build_intraday_sat(date(2026, 9, 2)) is None


# -- cache IO --------------------------------------------------------------------------------------


def test_read_cache_bad_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    assert m._read_cache(date(2026, 9, 2)) is None
    (tmp_path / "2026-09-02.json").write_text("{broken", encoding="utf-8")
    assert m._read_cache(date(2026, 9, 2)) is None
    (tmp_path / "2026-09-02.json").write_text("[1,2]", encoding="utf-8")
    assert m._read_cache(date(2026, 9, 2)) is None


def test_load_lookback_miss(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    assert m.load_intraday_sat(date(2026, 9, 2)) is None


# -- maybe_refresh -------------------------------------------------------------------------------------


def test_refresh_fresh_cache_skips_build(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    now = _dt(10, 0)
    cached = {"asOf": "2026-09-02", "snapshotAt": now.isoformat(), "candidates": []}
    m.cache_intraday_sat(cached, date(2026, 9, 2))
    monkeypatch.setattr(m, "build_intraday_sat", lambda day: (_ for _ in ()).throw(AssertionError("no build")))
    out = m.maybe_refresh_intraday_sat(now=now)
    assert out is not None and out["asOf"] == "2026-09-02"


def test_refresh_force_rebuilds_and_freezes_after_close(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    fresh = {"asOf": "2026-09-02", "note": None, "frozen": False}
    monkeypatch.setattr(m, "build_intraday_sat", lambda day: dict(fresh))
    out = m.maybe_refresh_intraday_sat(force=True, now=_dt(15, 30))
    assert out is not None and out["frozen"] is True and "冻结" in (out["note"] or "")
    # Cached file written through.
    assert m._read_cache(date(2026, 9, 2)) is not None


def test_refresh_lock_sees_fresh_cache(tmp_path, monkeypatch) -> None:
    """First check stale, in-lock re-read fresh → return without build (race path)."""
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    now = _dt(10, 0)
    stale = {"asOf": "2026-09-02", "snapshotAt": "2026-09-02T09:00:00+08:00"}
    fresh = {"asOf": "2026-09-02", "snapshotAt": now.isoformat()}
    calls = {"n": 0}

    def _reads(day):
        calls["n"] += 1
        return stale if calls["n"] == 1 else fresh

    monkeypatch.setattr(m, "_read_cache", _reads)
    monkeypatch.setattr(m, "build_intraday_sat", lambda day: (_ for _ in ()).throw(AssertionError("no build")))
    out = m.maybe_refresh_intraday_sat(now=now)
    assert out == fresh


def test_refresh_build_none_falls_back(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(m, "_CACHE_DIR", str(tmp_path))
    monkeypatch.setattr(m, "build_intraday_sat", lambda day: None)
    assert m.maybe_refresh_intraday_sat(force=True, now=_dt(10, 0)) is None
    cached = {"asOf": "2026-09-01", "candidates": []}
    m.cache_intraday_sat(cached, date(2026, 9, 1))
    out = m.maybe_refresh_intraday_sat(force=True, now=_dt(10, 0, day=2))
    assert out is not None and out.get("heldOvernight") is True
