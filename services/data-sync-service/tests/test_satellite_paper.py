"""OPT-228: satellite forward paper book projection (pure, no DB)."""

from __future__ import annotations

import pytest

from data_sync_service.service import satellite_paper
from data_sync_service.service.state_bucket_track import COSTS_ROUNDTRIP

_RT = COSTS_ROUNDTRIP * 100.0  # static CN round-trip cost in pct points


def _sat() -> dict:
    return {
        "blotter": [
            {
                "kind": "fill",
                "ts": "000001.SZ",
                "entryDate": "2026-09-18",
                "exitDate": "2026-09-22",
                "entryPxSrc": "bar_1430",
                "exitPxSrc": "bar_1430",
                "pnlPct": 5.0,
                "heldDays": 3,
                "closeReason": "body_exit",
                "amp": 1.1,
                "ampRank": 2,
            },
            {
                "kind": "fill",
                "ts": "000002.SZ",
                "entryDate": "2026-09-19",
                "exitDate": "2026-09-23",
                "pnlPct": -4.0,
                "heldDays": 3,
                "closeReason": "body_exit",
                "amp": 2.0,
                "ampRank": 1,
            },
            # Pre-inception fill must be dropped.
            {
                "kind": "fill",
                "ts": "000003.SZ",
                "entryDate": "2026-08-01",
                "exitDate": "2026-08-04",
                "pnlPct": 9.0,
                "heldDays": 3,
                "closeReason": "body_exit",
            },
            # Skips are not trades.
            {"kind": "skip_t1", "ts": "000004.SZ", "date": "2026-09-18"},
        ],
        "openPositions": [
            {
                "ts": "000005.SZ",
                "entryDate": "2026-09-21",
                "entryPrice": 10.0,
                "close": 10.5,
                "heldDays": 1,
                "daysLeft": 2,
                "exitDue": "2026-09-24",
                "pnlPct": 5.0,
            },
            # Pre-inception open leg must be dropped.
            {"ts": "000006.SZ", "entryDate": "2026-07-01", "entryPrice": 1.0},
        ],
        "summary": {"satPct": 3.2, "satMaxDdPct": 1.1, "avgHeldDays": 3.0},
    }


def test_projection_counts_and_shapes() -> None:
    book = satellite_paper.satellite_paper_from_sat(_sat(), start="2026-09-18", end="2026-09-24")
    assert book["prereq"]["closedCount"] == 2
    assert book["prereq"]["target"] == 20
    assert book["prereq"]["met"] is False
    assert book["stats"]["closedCount"] == 2
    assert book["stats"]["openCount"] == 1
    assert book["stats"]["winCount"] == 1
    assert book["stats"]["winRate"] == pytest.approx(0.5)
    # Net = gross - static CN round-trip cost.
    assert book["stats"]["avgNetPnlPct"] == pytest.approx(
        ((5.0 - _RT) + (-4.0 - _RT)) / 2, abs=0.01
    )
    assert book["stats"]["closeReasons"] == {"body_exit": 2}
    assert book["stats"]["paperPct"] == 3.2


def test_projection_filters_and_sorts_newest_first() -> None:
    book = satellite_paper.satellite_paper_from_sat(_sat(), start="2026-09-18", end="2026-09-24")
    ts = [t["ts"] for t in book["closed"]]
    assert ts == ["000002.SZ", "000001.SZ"]  # exit 09-23 before 09-22
    assert all(t["ts"] != "000003.SZ" for t in book["closed"])  # pre-inception dropped
    assert [p["ts"] for p in book["openLegs"]] == ["000005.SZ"]
    net = {t["ts"]: t["netPnlPct"] for t in book["closed"]}
    assert net["000001.SZ"] == pytest.approx(5.0 - _RT, abs=0.01)
    assert net["000002.SZ"] == pytest.approx(-4.0 - _RT, abs=0.01)


def test_empty_book() -> None:
    book = satellite_paper.satellite_paper_from_sat(
        {"blotter": [], "openPositions": [], "summary": {}},
        start="2026-09-18",
        end="2026-09-24",
    )
    assert book["prereq"] == {"closedCount": 0, "target": 20, "met": False}
    assert book["stats"]["winRate"] is None
    assert book["closed"] == []
    assert book["openLegs"] == []


# ---------------------------------------------------------------------------
# User book (2026-09-23): the journal-sourced forward book.
# ---------------------------------------------------------------------------


def _row(sym, side, day, px, pct, created="") -> dict:
    return {
        "symbol": sym,
        "side": side,
        "tradeDate": day,
        "price": px,
        "positionPct": pct,
        "createdAt": created,
    }


def test_user_book_fifo_matches_and_counts_closed() -> None:
    out = satellite_paper.satellite_user_book_from_rows(
        [
            _row("CN:300932", "BUY", "2026-09-21", 11.01, 25, "1"),
            _row("CN:300932", "SELL", "2026-09-23", 11.20, 25, "2"),
            _row("CN:603019", "BUY", "2026-09-22", 84.77, 25, "3"),
        ],
        start="2026-09-18",
        end="2026-09-23",
    )
    assert out["prereq"] == {"closedCount": 1, "target": 20, "met": False}
    assert out["stats"]["openCount"] == 1
    assert out["stats"]["winCount"] == 1
    # net is computed from the unrounded gross (11.20/11.01) minus the static
    # round trip, then rounded — 1.4029 → 1.4 (the displayed gross is 1.73).
    assert out["closed"][0]["grossPnlPct"] == 1.73
    assert out["closed"][0]["netPnlPct"] == pytest.approx(1.4)
    assert [leg["ts"] for leg in out["openLegs"]] == ["CN:603019"]
    assert out["source"] == "user_journal"


def test_user_book_ignores_trades_after_end() -> None:
    out = satellite_paper.satellite_user_book_from_rows(
        [
            _row("CN:300932", "BUY", "2026-09-22", 10.0, 25, "1"),
            _row("CN:300932", "SELL", "2026-09-24", 11.0, 25, "2"),
            _row("CN:603019", "BUY", "2026-09-24", 20.0, 25, "3"),
        ],
        start="2026-09-18",
        end="2026-09-23",
    )
    assert out["prereq"]["closedCount"] == 0
    assert out["stats"]["openCount"] == 1
    assert [leg["ts"] for leg in out["openLegs"]] == ["CN:300932"]


def test_user_book_partial_sell_closes_only_the_matched_share() -> None:
    out = satellite_paper.satellite_user_book_from_rows(
        [
            _row("CN:300932", "BUY", "2026-09-21", 10.0, 25, "1"),
            _row("CN:300932", "SELL", "2026-09-23", 11.0, 10, "2"),
        ],
        start="2026-09-18",
        end="2026-09-23",
    )
    assert out["prereq"]["closedCount"] == 1
    assert out["closed"][0]["positionPct"] == pytest.approx(10.0)
    # 15pt of the lot is still open
    assert out["openLegs"][0]["positionPct"] == pytest.approx(15.0)


def test_user_book_drops_pre_inception_and_ignores_bad_rows() -> None:
    out = satellite_paper.satellite_user_book_from_rows(
        [
            _row("CN:000001", "BUY", "2026-09-01", 10.0, 25, "1"),  # pre-inception
            _row("CN:000001", "SELL", "2026-09-22", 11.0, 25, "2"),
            _row("CN:000002", "BUY", "2026-09-22", 0.0, 25, "3"),  # no price
            _row("CN:000003", "ADD", "2026-09-22", 10.0, 0, "4"),  # no size
        ],
        start="2026-09-18",
        end="2026-09-23",
    )
    assert out["prereq"]["closedCount"] == 0
    assert out["openLegs"] == []


def test_user_book_query_is_bounded(monkeypatch) -> None:
    seen: dict[str, object] = {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, sql, params):
            seen["sql"] = sql
            seen["params"] = params

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def cursor(self):
            return Cursor()

    monkeypatch.setattr("data_sync_service.db.get_connection", lambda: Connection())
    satellite_paper.build_user_satellite_book(start="2026-09-18", end="2026-09-23")
    assert "trade_date >= %s" in str(seen["sql"])
    assert "trade_date <= %s" in str(seen["sql"])
    assert seen["params"] == ("satellite", "2026-09-18", "2026-09-23")


def test_user_book_empty() -> None:
    out = satellite_paper.satellite_user_book_from_rows([], start="2026-09-18", end="2026-09-23")
    assert out["prereq"]["closedCount"] == 0
    assert out["stats"]["winRate"] is None
    assert out["closed"] == [] and out["openLegs"] == []
