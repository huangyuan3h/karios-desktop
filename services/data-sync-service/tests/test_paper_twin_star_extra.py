"""paper_twin_star uncovered branches (H1): 5min ensure, 1430 map, skips, failures.

Hermetic: sys.modules stubs for bar_5min/psycopg, module-attr stubs for DB.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from data_sync_service.db.paper_trading import SOURCE_TWIN_STAR
from data_sync_service.service import paper_twin_star as pts
from data_sync_service.service.paper_twin_star import (
    _ensure_5min_today as _real_ensure_5min,
)
from data_sync_service.service.paper_twin_star import (
    _fetch_1430_px_map as _real_1430_map,
)


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch):
    monkeypatch.setattr(pts, "is_cn_trading_day", lambda d: True)
    monkeypatch.setattr(pts, "_ensure_5min_today", lambda ts, day: None)
    monkeypatch.setattr(pts, "_fetch_1430_px_map", lambda ts, day: ({}, {}))


def _action(monkeypatch, cands, **sat_kw):
    sat = {"gateOpen": True, "candidates": cands}
    sat.update(sat_kw)
    monkeypatch.setattr(pts, "build_twin_star_daily_action", lambda: {"sat": sat})
    monkeypatch.setattr(pts, "_open_twin_star", lambda: [])
    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", lambda ts, days=5: {})


# -- _ensure_5min_today ----------------------------------------------------------


def test_ensure_5min_empty_noop(monkeypatch) -> None:
    _real_ensure_5min([""], "2026-09-02")
    _real_ensure_5min([], "2026-09-02")


def test_ensure_5min_success_and_failure(monkeypatch) -> None:
    seen: dict = {}
    fake = SimpleNamespace(
        SOURCE_BAOSTOCK="baostock",
        backfill_symbols=lambda **k: seen.update(k) or {"ok": True, "stored": 3, "failed": 0, "skipped": 0},
    )
    monkeypatch.setitem(sys.modules, "data_sync_service.service.bar_5min", fake)
    _real_ensure_5min(["000001.SZ", "000001.SZ"], "2026-09-02")
    assert seen["ts_codes"] == ["000001.SZ"] and seen["skip_covered"] is True

    def _boom(**k):
        raise RuntimeError("baostock down")

    monkeypatch.setitem(
        sys.modules,
        "data_sync_service.service.bar_5min",
        SimpleNamespace(SOURCE_BAOSTOCK="b", backfill_symbols=_boom),
    )
    _real_ensure_5min(["000001.SZ"], "2026-09-02")  # warns, no raise


# -- _fetch_1430_px_map -----------------------------------------------------------


def test_1430_map_empty_and_success(monkeypatch) -> None:
    assert _real_1430_map([], "2026-09-02") == ({}, {})

    class _Cur:
        def execute(self, *a):
            pass

        def fetchall(self):
            # SQL already filters NULL/non-positive; mirror that contract.
            return [("000001.SZ", 10.5)]

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda url: _Conn()))
    px, src = _real_1430_map(["000001.SZ", "000002.SZ"], "2026-09-02")
    assert px == {"000001.SZ": 10.5}
    assert src == {"000001.SZ": "bar_5min_1430"}


def test_1430_map_failure_falls_back(monkeypatch) -> None:
    def _boom(url):
        raise RuntimeError("db down")

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=_boom))
    assert _real_1430_map(["000001.SZ"], "2026-09-02") == ({}, {})


# -- intake skips / failures -------------------------------------------------------


def test_open_twin_star_filters_source(monkeypatch) -> None:
    monkeypatch.setattr(
        pts,
        "list_paper_trades",
        lambda **k: [
            {"symbol": "CN:1", "source": SOURCE_TWIN_STAR},
            {"symbol": "CN:2", "source": "s3"},
            {"symbol": "CN:3"},
        ],
    )
    assert pts._open_twin_star() == [{"symbol": "CN:1", "source": SOURCE_TWIN_STAR}]


def test_held_days_empty() -> None:
    assert pts._held_days(None, "2026-09-02") == 0
    assert pts._held_days("", "2026-09-02") == 0


def test_intake_bad_date_proceeds(monkeypatch) -> None:
    _action(monkeypatch, [{"ts": "000001.SZ", "close": 10}])
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: {"id": "x"})
    out = pts.run_intake_twin_star(trade_date="not-a-date")
    assert out["inserted"] == 1 and "non_session" not in out["skippedReasons"]


def test_intake_action_failure(monkeypatch) -> None:
    def _boom():
        raise RuntimeError("action down")

    monkeypatch.setattr(pts, "build_twin_star_daily_action", _boom)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert "twin_star action failed" in out["error"]


def test_intake_no_candidates(monkeypatch) -> None:
    _action(monkeypatch, [])
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["skippedReasons"]["no_candidates"] == 1


def test_intake_list_open_failure(monkeypatch) -> None:
    _action(monkeypatch, [{"ts": "000001.SZ", "close": 10}])

    def _boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(pts, "_open_twin_star", _boom)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert "list open twin_star failed" in out["error"]


def test_intake_close_fetch_uses_daily_and_survives_error(monkeypatch) -> None:
    _action(monkeypatch, [{"ts": "000001.SZ"}])
    monkeypatch.setattr(
        pts,
        "fetch_last_ohlcv_batch",
        lambda ts, days=5: {
            "000001.SZ": [["2026-09-01", 1, 2, 3, 10.0], ["2026-09-02", 1, 2, 3]],  # short row → None
            "000002.SZ": [],  # empty rows → skip
        },
    )
    seen: list[dict] = []
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: seen.append(k) or {"id": "x"})
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    # 000001 short row yields no close; snapshot close absent → no_price.
    assert out["skippedReasons"]["no_price"] == 1

    monkeypatch.setattr(
        pts,
        "fetch_last_ohlcv_batch",
        lambda ts, days=5: {"000001.SZ": [["2026-09-01", 1, 2, 3, 10.0], ["2026-09-02", 1, 2, 3, 10.4]]},
    )
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 1
    assert seen[0]["entry_price"] == 10.4
    assert seen[0]["signal_snapshot"]["entryPxSrc"] == "daily_close"

    def _boom(ts, days=5):
        raise RuntimeError("fetch down")

    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", _boom)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert "no_price" in out["skippedReasons"]  # daily fallback also gone


def test_intake_no_ts_and_bad_close_and_no_price(monkeypatch) -> None:
    _action(
        monkeypatch,
        [
            {"no": "ts"},
            {"ts": "000001.SZ", "close": "nonsense"},
            {"ts": "000002.SZ"},
        ],
    )
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: {"id": "x"})
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["skippedReasons"]["no_ts"] == 1
    # "nonsense" close keeps px None → no_price; 000002 has no px either.
    assert out["skippedReasons"]["no_price"] == 2


def test_intake_insert_failed_and_idempotent(monkeypatch) -> None:
    _action(monkeypatch, [{"ts": "000001.SZ", "close": 10}, {"ts": "000002.SZ", "close": 11}])
    calls = {"n": 0}

    def _flaky(**k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("write down")
        return None

    monkeypatch.setattr(pts, "insert_paper_trade", _flaky)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["skippedReasons"]["insert_failed"] == 1
    assert out["skippedReasons"]["idempotent"] == 1


# -- update failures -----------------------------------------------------------------


def test_update_list_open_failure_and_empty(monkeypatch) -> None:
    def _boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(pts, "_open_twin_star", _boom)
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    assert "list open twin_star failed" in out["error"]

    monkeypatch.setattr(pts, "_open_twin_star", lambda: [])
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    assert out["scanned"] == 0 and out["closed"] == 0


def test_update_fetch_failure_and_unpriced_skip(monkeypatch) -> None:
    monkeypatch.setattr(
        pts,
        "_open_twin_star",
        lambda: [
            {"id": "", "symbol": "CN:000001", "entryDate": "2026-08-31", "entryPrice": 10.0},
            {"id": "b", "symbol": "CN:000002", "entryDate": "2026-08-31", "entryPrice": 0},
        ],
    )

    def _boom(ts, days=8):
        raise RuntimeError("fetch down")

    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", _boom)
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    # No prices anywhere → both skipped silently, nothing closed.
    assert out["closed"] == 0


def test_update_close_failure_continues(monkeypatch) -> None:
    monkeypatch.setattr(
        pts,
        "_open_twin_star",
        lambda: [
            {"id": "a", "symbol": "CN:000001", "entryDate": "2026-08-31", "entryPrice": 10.0},
            {"id": "b", "symbol": "CN:600002", "entryDate": "2026-08-31", "entryPrice": 10.0},
        ],
    )
    monkeypatch.setattr(
        pts,
        "fetch_last_ohlcv_batch",
        lambda ts, days=8: {
            "000001.SZ": [["2026-09-02", 0, 0, 0, 10.5]],
            "600002.SH": [["2026-09-02", 0, 0, 0, 10.6]],
        },
    )

    def _flaky(**k):
        if k["trade_id"] == "a":
            raise RuntimeError("close down")
        return {"id": "b"}

    monkeypatch.setattr(pts, "close_paper_trade", _flaky)
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    assert out["closed"] == 1
    # exitPxSrc counts priced candidates before the close attempt.
    assert out["exitPxSrc"] == {"daily_close": 2}
