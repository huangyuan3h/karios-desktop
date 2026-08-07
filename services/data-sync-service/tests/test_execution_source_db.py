"""DB integration tests for TIP-011 source attribution.

These tests verify:
- paper_trades.source round-trips through insert + _row_to_dict
- count_by_source aggregates correctly (TV / ALPHA / MANUAL / UNKNOWN)
- count_changes_by_source groups changes by source field
- aggregate_source_stats combines both tables into a stable shape
- backfill_paper_trades_source (dry_run) walks pre-TIP-011 rows
- Source closed enum is enforced (insert_paper_trade raises on garbage)

All tests are gated by ``requires_postgres`` (see conftest.py).
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from data_sync_service.db import execution_journal as ej_db
from data_sync_service.db import paper_trading as pt_db


pytestmark = pytest.mark.requires_postgres


_CREATED_SYMBOLS: set[str] = set()

# Fake snapshot ids this module inserts into execution_decision_changes
# (test_aggregate_source_stats_shape / test_backfill_paper_trades_source_dry_run).
_FAKE_SNAPSHOT_IDS = ("snap-agg", "snap-bf")


@pytest.fixture(autouse=True)
def _ensure_tables() -> None:
    """Ensure tables, then clean up every row this module inserted.

    These integration tests write real rows into the dev Postgres; without a
    teardown they pollute the paper_trades / execution_snapshots /
    execution_decision_changes tables (2026-08-07 incident: 230+ CN:99xxxx
    paper rows + 72 'manual-test' snapshots + 67 fake-id change rows masked
    all real decision-log data). Every test's rows are deleted after the test.
    """
    pt_db.ensure_tables()
    ej_db.ensure_table()
    yield
    if _CREATED_SYMBOLS:
        from data_sync_service.db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM paper_trades WHERE symbol = ANY(%s)",
                    (sorted(_CREATED_SYMBOLS),),
                )
                cur.execute(
                    """
                    DELETE FROM execution_decision_changes
                    WHERE symbol = ANY(%s)
                       OR to_snapshot_id = ANY(%s)
                       OR from_snapshot_id = ANY(%s)
                    """,
                    (sorted(_CREATED_SYMBOLS), list(_FAKE_SNAPSHOT_IDS), list(_FAKE_SNAPSHOT_IDS)),
                )
                cur.execute(
                    "DELETE FROM execution_snapshots WHERE source = 'manual-test'"
                )
            conn.commit()
        _CREATED_SYMBOLS.clear()


def _fresh_symbol(prefix: str = "CN") -> str:
    import uuid

    u = uuid.uuid4().hex[:6]
    sym = f"{prefix}:99{u}"
    _CREATED_SYMBOLS.add(sym)
    return sym


def test_insert_paper_trade_round_trip_source() -> None:
    sym = _fresh_symbol()
    pt_db.insert_paper_trade(
        symbol=sym,
        entry_date=date.today().isoformat(),
        side="BUY",
        entry_price=10.0,
        source="ALPHA",
    )
    rows = pt_db.list_paper_trades(since=date.today().isoformat(), limit=200)
    target = next((r for r in rows if r.get("symbol") == sym), None)
    assert target is not None
    assert target.get("source") == "ALPHA"


def test_insert_paper_trade_rejects_unknown_source() -> None:
    sym = _fresh_symbol()
    with pytest.raises(ValueError, match="source must be one of"):
        pt_db.insert_paper_trade(
            symbol=sym,
            entry_date=date.today().isoformat(),
            side="BUY",
            entry_price=10.0,
            source="GARBAGE",
        )


def test_count_by_source_aggregates_per_source() -> None:
    sym_a = _fresh_symbol()
    sym_b = _fresh_symbol()
    sym_c = _fresh_symbol()
    today = date.today().isoformat()
    pt_db.insert_paper_trade(
        symbol=sym_a,
        entry_date=today,
        side="BUY",
        entry_price=10.0,
        source="TV",
    )
    pt_db.insert_paper_trade(
        symbol=sym_b,
        entry_date=today,
        side="BUY",
        entry_price=11.0,
        source="ALPHA",
    )
    pt_db.insert_paper_trade(
        symbol=sym_c,
        entry_date=today,
        side="BUY",
        entry_price=12.0,
        source="MANUAL",
    )
    counts = pt_db.count_by_source(since=today, status="open")
    assert counts.get("TV", {}).get("total", 0) >= 1
    assert counts.get("ALPHA", {}).get("total", 0) >= 1
    assert counts.get("MANUAL", {}).get("total", 0) >= 1


def test_count_by_source_unknown_bucket_for_null() -> None:
    sym = _fresh_symbol()
    today = date.today().isoformat()
    # Insert with NULL source (legacy row equivalent)
    pt_db.insert_paper_trade(
        symbol=sym,
        entry_date=today,
        side="BUY",
        entry_price=10.0,
        source=None,
    )
    counts = pt_db.count_by_source(since=today, status="open")
    assert "UNKNOWN" in counts
    assert counts["UNKNOWN"]["total"] >= 1


def test_count_changes_by_source_filters_by_field_and_new_value() -> None:
    sym = _fresh_symbol()
    today = date.today().isoformat()
    snap = ej_db.insert_snapshot(
        trade_date=today,
        source="manual-test",
        gate={"mode": "ATTACK", "allowNewEntries": True},
        cards=[
            {"symbol": sym, "action": "WATCH", "why": "WATCH", "source": "TV"},
        ],
        content_hash="hash-1",
    )
    ej_db.insert_snapshot(
        trade_date=today,
        source="manual-test",
        gate={"mode": "ATTACK", "allowNewEntries": True},
        cards=[
            {"symbol": sym, "action": "BUY", "why": "MAINLINE_OK", "source": "TV"},
        ],
        content_hash="hash-2",
    )
    # We need a previous-state snapshot to compute the diff; use this as the prev.
    prev_snap = ej_db.fetch_snapshot_by_id(snap["id"])
    ej_db.insert_changes(
        [
            {
                "trade_date": today,
                "from_snapshot_id": prev_snap["id"],
                "to_snapshot_id": "snap-curr",
                "scope": "symbol",
                "symbol": sym,
                "field": "action",
                "old_value": "WATCH",
                "new_value": "BUY",
                "source": "TV",
            },
        ]
    )
    counts = ej_db.count_changes_by_source(field="action", new_value="BUY")
    assert counts.get("TV", 0) >= 1


def test_insert_changes_persists_source_field() -> None:
    today = date.today().isoformat()
    # _fresh_symbol registers with the teardown; a hardcoded real symbol
    # (previously CN:600000) would leak fake rows into a live symbol's log.
    sym = _fresh_symbol()
    persisted = ej_db.insert_changes(
        [
            {
                "trade_date": today,
                "to_snapshot_id": "snap-tgt",
                "scope": "symbol",
                "symbol": sym,
                "field": "action",
                "old_value": "WATCH",
                "new_value": "BUY",
                "source": "ALPHA",
            },
        ]
    )
    assert persisted[0]["source"] == "ALPHA"
    listed = ej_db.list_changes(trade_date=today, limit=10)
    assert any(c.get("source") == "ALPHA" for c in listed)


def test_aggregate_source_stats_shape() -> None:
    from data_sync_service.service.execution_source import aggregate_source_stats

    today = date.today().isoformat()
    sym = _fresh_symbol()
    pt_db.insert_paper_trade(
        symbol=sym,
        entry_date=today,
        side="BUY",
        entry_price=10.0,
        source="ALPHA",
    )
    ej_db.insert_changes(
        [
            {
                "trade_date": today,
                "to_snapshot_id": "snap-agg",
                "scope": "symbol",
                "symbol": sym,
                "field": "action",
                "old_value": "WATCH",
                "new_value": "BUY",
                "source": "ALPHA",
            },
        ]
    )
    result = aggregate_source_stats(since_days=30)
    assert "sinceDays" in result
    assert "bySource" in result
    assert "openTradesBySource" in result
    if "ALPHA" in result["bySource"]:
        bucket = result["bySource"]["ALPHA"]
        assert "buySignals" in bucket
        assert "closed" in bucket
        assert "wins" in bucket
        assert "losses" in bucket
        assert "winRate" in bucket


def test_backfill_paper_trades_source_dry_run() -> None:
    from data_sync_service.service.execution_source import backfill_paper_trades_source

    sym = _fresh_symbol()
    today = date.today().isoformat()
    pt_db.insert_paper_trade(
        symbol=sym,
        entry_date=today,
        side="BUY",
        entry_price=10.0,
        source=None,
    )
    ej_db.insert_changes(
        [
            {
                "trade_date": today,
                "to_snapshot_id": "snap-bf",
                "scope": "symbol",
                "symbol": sym,
                "field": "action",
                "old_value": "WATCH",
                "new_value": "BUY",
                "source": "TV",
            },
        ]
    )
    counts = backfill_paper_trades_source(dry_run=True)
    assert "backfilled" in counts
    assert "already_set" in counts
    assert "no_match" in counts
    assert counts["backfilled"] >= 1
    # Dry run — paper_trades.source is still None
    rows = pt_db.list_paper_trades(since=today, limit=200)
    target = next((r for r in rows if r.get("symbol") == sym), None)
    assert target is not None
    assert target.get("source") is None