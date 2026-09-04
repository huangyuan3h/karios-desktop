"""clip4 satellite paper book — no DB."""

from __future__ import annotations

import pytest

from data_sync_service.db.paper_trading import (
    CLOSE_REASON_BODY_EXIT,
    SOURCE_TWIN_STAR,
)
from data_sync_service.service import paper_trading as pt_svc
from data_sync_service.service import paper_twin_star as pts


@pytest.fixture(autouse=True)
def _no_external_io(monkeypatch):
    """Keep paper unit tests hermetic: session open, no 5min import/fetch."""
    monkeypatch.setattr(pts, "is_cn_trading_day", lambda d: True)
    monkeypatch.setattr(pts, "_ensure_5min_today", lambda ts, day: None)
    monkeypatch.setattr(pts, "_fetch_1430_px_map", lambda ts, day: ({}, {}))


def test_intake_caps_at_four_slots(monkeypatch) -> None:
    inserted: list[str] = []

    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {
            "sat": {
                "gateOpen": True,
                "candidates": [
                    {"ts": f"00000{i}.SZ", "amp": i, "gapPct": 5, "close": 10 + i}
                    for i in range(1, 7)
                ],
            }
        },
    )
    monkeypatch.setattr(pts, "_open_twin_star", lambda: [])
    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", lambda ts, days=5: {})

    def fake_insert(**kwargs):
        inserted.append(kwargs["symbol"])
        assert kwargs["source"] == SOURCE_TWIN_STAR
        assert kwargs["sleeve_pct"] == pts.SLEEVE_PCT
        return {"id": kwargs["symbol"]}

    monkeypatch.setattr(pts, "insert_paper_trade", fake_insert)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 4
    assert inserted == ["CN:000001", "CN:000002", "CN:000003", "CN:000004"]
    assert out["skippedReasons"]["slots_full"] >= 1


def test_intake_skips_when_slots_full(monkeypatch) -> None:
    opens = [{"symbol": f"CN:60000{i}", "source": SOURCE_TWIN_STAR} for i in range(4)]
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {"sat": {"gateOpen": True, "candidates": [{"ts": "000001.SZ", "close": 10}]}},
    )
    monkeypatch.setattr(pts, "_open_twin_star", lambda: opens)
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: (_ for _ in ()).throw(AssertionError("no insert")))
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 0
    assert out["skippedReasons"]["slots_full"] == 1


def test_update_closes_body3_not_protect_stop(monkeypatch) -> None:
    closed: list[tuple[str, str]] = []

    monkeypatch.setattr(
        pts,
        "_open_twin_star",
        lambda: [
            {
                "id": "a",
                "symbol": "CN:000001",
                "source": SOURCE_TWIN_STAR,
                "entryDate": "2026-08-31",  # Mon; as_of Sep 2 Wed = 3 weekdays
                "entryPrice": 10.0,
            },
            {
                "id": "b",
                "symbol": "CN:600000",
                "source": SOURCE_TWIN_STAR,
                "entryDate": "2026-09-02",
                "entryPrice": 10.0,
            },
        ],
    )
    monkeypatch.setattr(
        pts,
        "fetch_last_ohlcv_batch",
        lambda ts, days=8: {
            "000001.SZ": [["2026-09-02", 0, 0, 0, 10.2]],
            "600000.SH": [["2026-09-02", 0, 0, 0, 9.0]],
        },
    )

    def fake_close(**kwargs):
        closed.append((kwargs["trade_id"], kwargs["close_reason"]))
        return {"id": kwargs["trade_id"]}

    monkeypatch.setattr(pts, "close_paper_trade", fake_close)
    monkeypatch.setattr(pts, "round_trip_cost_pct", lambda m: 0.0)
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    assert out["closed"] == 1
    assert closed == [("a", CLOSE_REASON_BODY_EXIT)]


def test_intake_skips_already_open_and_gate_closed(monkeypatch) -> None:
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {"sat": {"gateOpen": True, "candidates": [{"ts": "000001.SZ", "close": 10}]}},
    )
    monkeypatch.setattr(
        pts,
        "_open_twin_star",
        lambda: [{"symbol": "CN:000001", "source": SOURCE_TWIN_STAR}],
    )
    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", lambda ts, days=5: {})
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: (_ for _ in ()).throw(AssertionError("no insert")))
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 0
    assert out["skippedReasons"]["already_open"] == 1

    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {"sat": {"gateOpen": False, "candidates": [{"ts": "000002.SZ", "close": 10}]}},
    )
    closed = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert closed["inserted"] == 0
    assert closed["skippedReasons"]["gate_closed"] == 1


def test_s3_update_skips_twin_star_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        pt_svc.pt_db,
        "get_open_paper_trades",
        lambda: [
            {
                "id": "x",
                "symbol": "CN:000001",
                "source": SOURCE_TWIN_STAR,
                "entryDate": "2026-01-01",
                "entryPrice": 10.0,
            }
        ],
    )
    monkeypatch.setattr(pt_svc.pt_db, "today_iso", lambda: "2026-09-02")
    monkeypatch.setattr(pt_svc, "fetch_last_ohlcv_batch", lambda *a, **k: {})
    monkeypatch.setattr(pt_svc.wa_db, "list_registry", lambda: [])
    out = pt_svc.run_update(today_iso="2026-09-02")
    assert out["scanned"] == 1
    assert out["closed"] == 0


def test_sleeve_is_twelve_point_five() -> None:
    assert pts.SLEEVE_PCT == 0.125


def test_intake_skips_snapshot_bad_and_non_session(monkeypatch) -> None:
    monkeypatch.setattr(pts, "_open_twin_star", lambda: [])
    monkeypatch.setattr(pts, "insert_paper_trade", lambda **k: (_ for _ in ()).throw(AssertionError("no insert")))

    # Stale/missing intraday tape: T-1 fallback list must not be bought.
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {
            "sat": {
                "gateOpen": True,
                "candidates": [{"ts": "000001.SZ", "close": 10}],
                "snapshotMissing": True,
                "snapshotReason": "no_session_snapshot",
            }
        },
    )
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 0
    assert out["skippedReasons"]["snapshot_bad"] == 1

    # SSE holiday: no session, no intake even with an open gate.
    monkeypatch.setattr(pts, "is_cn_trading_day", lambda d: False)
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {"sat": {"gateOpen": True, "candidates": [{"ts": "000001.SZ", "close": 10}]}},
    )
    out = pts.run_intake_twin_star(trade_date="2026-09-03")
    assert out["inserted"] == 0
    assert out["skippedReasons"]["non_session"] == 1


def test_intake_prefers_bar_1430_and_records_source(monkeypatch) -> None:
    seen: list[dict] = []
    monkeypatch.setattr(
        pts,
        "build_twin_star_daily_action",
        lambda: {
            "sat": {
                "gateOpen": True,
                "candidates": [{"ts": "000001.SZ", "amp": 1, "gapPct": 5, "close": 10.5}],
            }
        },
    )
    monkeypatch.setattr(pts, "_open_twin_star", lambda: [])
    monkeypatch.setattr(pts, "fetch_last_ohlcv_batch", lambda ts, days=5: {})
    monkeypatch.setattr(
        pts, "_fetch_1430_px_map", lambda ts, day: ({"000001.SZ": 10.2}, {"000001.SZ": "bar_5min_1430"})
    )

    def fake_insert(**kwargs):
        seen.append(kwargs)
        return {"id": "x"}

    monkeypatch.setattr(pts, "insert_paper_trade", fake_insert)
    out = pts.run_intake_twin_star(trade_date="2026-09-02")
    assert out["inserted"] == 1
    assert seen[0]["entry_price"] == 10.2
    assert seen[0]["signal_snapshot"]["entryPxSrc"] == "bar_5min_1430"
    assert out["entryPxSrc"] == {"bar_5min_1430": 1}


def test_update_records_exit_source_and_hhmm(monkeypatch) -> None:
    closed: list[dict] = []
    monkeypatch.setattr(
        pts,
        "_open_twin_star",
        lambda: [
            {
                "id": "a",
                "symbol": "CN:000001",
                "source": SOURCE_TWIN_STAR,
                "entryDate": "2026-08-31",
                "entryPrice": 10.0,
            },
        ],
    )
    monkeypatch.setattr(
        pts,
        "fetch_last_ohlcv_batch",
        lambda ts, days=8: {"000001.SZ": [["2026-09-02", 0, 0, 0, 10.2]]},
    )
    monkeypatch.setattr(
        pts, "_fetch_1430_px_map", lambda ts, day: ({"000001.SZ": 10.1}, {"000001.SZ": "bar_5min_1430"})
    )

    def fake_close(**kwargs):
        closed.append(kwargs)
        return {"id": kwargs["trade_id"]}

    monkeypatch.setattr(pts, "close_paper_trade", fake_close)
    monkeypatch.setattr(pts, "round_trip_cost_pct", lambda m: 0.0)
    out = pts.run_update_twin_star(today_iso_s="2026-09-02")
    assert out["closed"] == 1
    assert out["exitHhmm"] == pts.HABIT_EXIT_HHMM
    assert closed[0]["close_price"] == 10.1
    assert closed[0]["signal_snapshot_extra"] == {"exitPx": 10.1, "exitPxSrc": "bar_5min_1430"}
    assert out["exitPxSrc"] == {"bar_5min_1430": 1}
