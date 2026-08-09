"""Unit tests for source propagation in execution_journal (no DB required).

Verifies that diff_snapshots threads ``card.source`` into every change row
that originates from that card, while pre-TIP-011 cards (no source) yield
``source=None`` for downstream bucketing as 'UNKNOWN'.
"""

from __future__ import annotations

from data_sync_service.service.execution_journal import diff_snapshots


def _snap(cards, gate=None, sid="snap-prev"):
    return {
        "id": sid,
        "gate": gate or {"mode": "ATTACK", "allowNewEntries": True},
        "cards": cards,
    }


def test_diff_propagates_source_to_action_change():
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "WATCH", "why": "WATCH"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK", "source": "TV"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    action_changes = [c for c in changes if c.get("field") == "action"]
    assert action_changes, "expected at least one action change"
    assert all(c.get("source") == "TV" for c in action_changes), action_changes


def test_diff_propagates_source_to_why_change():
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "ALPHA_SURGE", "source": "ALPHA"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    why_changes = [c for c in changes if c.get("field") == "why"]
    assert why_changes
    assert all(c.get("source") == "ALPHA" for c in why_changes)


def test_diff_unknown_source_when_card_unattributed():
    """Pre-TIP-011 snapshots don't carry source; changes stay source=None."""
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "WATCH"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    action_changes = [c for c in changes if c.get("field") == "action"]
    assert action_changes
    assert all(c.get("source") is None for c in action_changes)


def test_diff_gate_changes_carry_no_source():
    """Gate-level diffs are scope='gate' / symbol=None — source irrelevant."""
    prev = _snap(
        cards=[],
        gate={"mode": "HOLD_ONLY", "allowNewEntries": False},
    )
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        [],
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    assert any(c.get("scope") == "gate" for c in changes)
    gate_changes = [c for c in changes if c.get("scope") == "gate"]
    assert all(c.get("source") is None for c in gate_changes)


def test_diff_manual_source_distinct_from_tv():
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "WATCH"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "MANUAL_ADD", "source": "MANUAL"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    action_changes = [c for c in changes if c.get("field") == "action"]
    assert action_changes[0]["source"] == "MANUAL"


def test_diff_rejects_lowercase_source_to_none():
    """Lowercase source is normalized to closed enum or None; lowercase → None."""
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "WATCH"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "X", "source": "tv"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    action_changes = [c for c in changes if c.get("field") == "action"]
    assert action_changes[0]["source"] == "TV"


def test_diff_unknown_string_source_to_none():
    prev = _snap(
        cards=[{"symbol": "CN:600000", "action": "WATCH"}],
    )
    curr_cards = [
        {"symbol": "CN:600000", "action": "BUY", "why": "X", "source": "GARBAGE"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-04",
        from_snapshot_id="snap-prev",
        to_snapshot_id="snap-curr",
    )
    action_changes = [c for c in changes if c.get("field") == "action"]
    assert action_changes[0]["source"] is None