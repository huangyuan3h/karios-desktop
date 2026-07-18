"""Unit tests for execution decision journal hash/diff (no DB required)."""

from __future__ import annotations

from data_sync_service.service.execution_journal import (
    compute_content_hash,
    diff_snapshots,
    build_journal_markdown,
)


def test_content_hash_stable_for_same_decisions():
    gate = {"mode": "ATTACK", "allowNewEntries": True}
    cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_5D_TOP3", "trigger": 9.5, "positionPct": None},
        {"symbol": "CN:000001", "action": "WATCH", "why": "WATCH", "trigger": None, "positionPct": 5},
    ]
    # Order of cards should not matter
    h1 = compute_content_hash(gate, cards)
    h2 = compute_content_hash(gate, list(reversed(cards)))
    assert h1 == h2


def test_content_hash_changes_on_action():
    gate = {"mode": "ATTACK", "allowNewEntries": True}
    a = [{"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK"}]
    b = [{"symbol": "CN:600000", "action": "WATCH", "why": "INTRADAY_SURGE_BLOCK"}]
    assert compute_content_hash(gate, a) != compute_content_hash(gate, b)


def test_diff_gate_mode_and_symbol_action():
    prev = {
        "id": "s1",
        "gate": {"mode": "HOLD_ONLY", "allowNewEntries": False},
        "cards": [
            {"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK", "trigger": 10, "positionPct": 5},
        ],
    }
    curr_gate = {"mode": "ATTACK", "allowNewEntries": True}
    curr_cards = [
        {
            "symbol": "CN:600000",
            "action": "WATCH",
            "why": "INTRADAY_SURGE_BLOCK",
            "trigger": 10,
            "positionPct": 5,
        },
    ]
    changes = diff_snapshots(
        prev,
        curr_gate,
        curr_cards,
        trade_date="2026-07-18",
        from_snapshot_id="s1",
        to_snapshot_id="s2",
    )
    fields = {(c["scope"], c["field"], c["symbol"]) for c in changes}
    assert ("gate", "mode", None) in fields
    assert ("symbol", "action", "CN:600000") in fields
    assert ("symbol", "why", "CN:600000") in fields


def test_diff_skips_symbol_flood_on_first_snapshot():
    changes = diff_snapshots(
        None,
        {"mode": "ATTACK", "allowNewEntries": True},
        [{"symbol": "CN:600000", "action": "BUY", "why": "OK"}],
        trade_date="2026-07-18",
        from_snapshot_id=None,
        to_snapshot_id="s1",
    )
    assert changes == []


def test_build_journal_markdown_shape(monkeypatch):
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.fetch_latest_snapshot",
        lambda trade_date=None: {
            "id": "s1",
            "tradeDate": "2026-07-18",
            "capturedAt": "2026-07-18T08:00:00+00:00",
            "source": "manual",
            "gate": {"mode": "ATTACK"},
            "cards": [
                {
                    "symbol": "CN:600000",
                    "action": "BUY",
                    "why": "MAINLINE_5D_TOP3",
                    "trigger": 9.1,
                    "positionPct": None,
                    "mainlineOk": True,
                    "mainlineTag": "5D_TOP3",
                }
            ],
        },
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_snapshots",
        lambda trade_date=None, limit=50: [],
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_changes",
        lambda trade_date=None, since=None, limit=100: [
            {
                "changedAt": "2026-07-18T07:00:00+00:00",
                "tradeDate": "2026-07-18",
                "scope": "gate",
                "symbol": None,
                "field": "mode",
                "oldValue": "HOLD_ONLY",
                "newValue": "ATTACK",
            }
        ],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    assert "## Decision Journal" in md
    assert "### Changes (today)" in md
    assert "HOLD_ONLY" in md
    assert "CN:600000" in md
    assert "MAINLINE_5D_TOP3" in md
