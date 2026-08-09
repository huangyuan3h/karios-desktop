"""Unit tests for execution decision journal hash/diff (no DB required)."""

from __future__ import annotations

from data_sync_service.service.execution_journal import (
    build_journal_markdown,
    compute_content_hash,
    diff_snapshots,
    filter_journal_changes,
    symbols_with_latest_action_deltas,
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


def test_content_hash_changes_on_hard_stop():
    gate = {"mode": "ATTACK", "allowNewEntries": True}
    a = [{"symbol": "CN:600000", "action": "HOLD", "why": "HOLD", "hardStop": 9.0, "trailStop": None}]
    b = [{"symbol": "CN:600000", "action": "HOLD", "why": "HOLD", "hardStop": 9.5, "trailStop": None}]
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


def test_diff_tracks_hard_stop_and_trail_stop():
    prev = {
        "id": "s1",
        "gate": {"mode": "ATTACK", "allowNewEntries": True},
        "cards": [
            {
                "symbol": "CN:600000",
                "action": "HOLD",
                "why": "HOLD",
                "trigger": 10,
                "hardStop": 9.0,
                "trailStop": None,
            },
        ],
    }
    curr_cards = [
        {
            "symbol": "CN:600000",
            "action": "HOLD",
            "why": "HOLD",
            "trigger": 10.5,
            "hardStop": 9.0,
            "trailStop": 10.2,
        },
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-07-18",
        from_snapshot_id="s1",
        to_snapshot_id="s2",
    )
    fields = {c["field"] for c in changes if c["scope"] == "symbol"}
    assert "trigger" in fields
    assert "trailStop" in fields


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


def test_symbols_with_latest_action_deltas_ignores_silent_watch():
    day_changes = [
        {"scope": "symbol", "symbol": "CN:A", "field": "why", "oldValue": "WATCH", "newValue": "WATCH"},
        {"scope": "symbol", "symbol": "CN:B", "field": "action", "oldValue": "WATCH", "newValue": "BUY"},
        {"scope": "gate", "symbol": None, "field": "mode"},
    ]
    assert symbols_with_latest_action_deltas(day_changes) == {"CN:B"}


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
                    "hardStop": 8.5,
                    "trailStop": None,
                    "positionPct": None,
                    "mainlineOk": True,
                    "mainlineTag": "5D_TOP3",
                },
                {
                    "symbol": "CN:000001",
                    "action": "WATCH",
                    "why": "WATCH",
                    "trigger": None,
                    "positionPct": None,
                    "mainlineOk": False,
                },
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
            },
            {
                "changedAt": "2026-07-18T07:05:00+00:00",
                "tradeDate": "2026-07-18",
                "scope": "symbol",
                "symbol": "CN:600000",
                "field": "action",
                "oldValue": "WATCH",
                "newValue": "BUY",
            },
        ],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    assert "## Decision Journal" in md
    assert "### Changes (today)" in md
    assert "HOLD_ONLY" in md
    assert "delta-only" in md
    assert "CN:600000" in md
    assert "MAINLINE_5D_TOP3" in md
    # Silent WATCH with no Action/Trigger/Stop delta must not pollute Latest Actions
    latest_section = md.split("### Latest Actions")[1].split("###")[0]
    assert "CN:600000" in latest_section
    assert "CN:000001" not in latest_section


def test_build_journal_markdown_omits_silent_watch_pool(monkeypatch):
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.fetch_latest_snapshot",
        lambda trade_date=None: {
            "id": "s1",
            "tradeDate": "2026-07-18",
            "capturedAt": "2026-07-18T08:00:00+00:00",
            "source": "poll",
            "gate": {"mode": "HOLD_ONLY"},
            "cards": [
                {"symbol": "CN:A", "action": "WATCH", "why": "WATCH"},
                {"symbol": "CN:B", "action": "WATCH", "why": "WATCH"},
            ],
        },
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_snapshots",
        lambda trade_date=None, limit=50: [],
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_changes",
        lambda trade_date=None, since=None, limit=100: [],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    latest_section = md.split("### Latest Actions")[1]
    assert "CN:A" not in latest_section
    assert "CN:B" not in latest_section


def test_build_journal_markdown_collapses_bulk_init_changes(monkeypatch):
    """C6: a same-timestamp bulk initialization (50 rows) collapses to one row."""
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.fetch_latest_snapshot",
        lambda trade_date=None: None,
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_snapshots",
        lambda trade_date=None, limit=50: [],
    )
    bulk = [
        {
            "changedAt": "2026-07-18T06:00:00+00:00",
            "tradeDate": "2026-07-18",
            "scope": "symbol",
            "symbol": f"CN:{i:06d}",
            "field": "action",
            "oldValue": "",
            "newValue": "WATCH",
        }
        for i in range(50)
    ]
    single = {
        "changedAt": "2026-07-18T07:00:00+00:00",
        "tradeDate": "2026-07-18",
        "scope": "symbol",
        "symbol": "CN:600000",
        "field": "action",
        "oldValue": "WATCH",
        "newValue": "BUY",
    }
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_changes",
        lambda trade_date=None, since=None, limit=100: bulk + [single],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    changes_section = md.split("### Changes (today)")[1].split("### Latest Actions")[0]
    assert "批量初始化 50 项" in changes_section
    # Only the collapsed row + the single real change remain (was 51 rows).
    assert changes_section.count("\n| 2026-07-18") == 2
    assert "| CN:600000 | action | WATCH | BUY |" in changes_section
def test_build_journal_markdown_drops_recent_snapshots_block(monkeypatch):
    """2026-08-01 · Recent snapshots block removed (only signal changes matter)."""
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.fetch_latest_snapshot",
        lambda trade_date=None: {
            "id": "s1",
            "tradeDate": "2026-07-18",
            "capturedAt": "2026-07-18T08:00:00+00:00",
            "source": "manual",
            "gate": {"mode": "ATTACK"},
            "cards": [],
        },
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_snapshots",
        lambda trade_date=None, limit=50: [],
    )
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.list_changes",
        lambda trade_date=None, since=None, limit=100: [],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    assert "### Recent snapshots" not in md


def test_filter_journal_changes_drops_small_hardstop_drifts():
    """hardStop drift < 1% is suppressed."""
    changes = [
        {"scope": "symbol", "symbol": "CN:A", "field": "hardStop", "oldValue": 36.87, "newValue": 36.94},  # ~0.19% — noise
        {"scope": "symbol", "symbol": "CN:A", "field": "hardStop", "oldValue": 10.0, "newValue": 9.85},  # 1.5% — kept
        {"scope": "symbol", "symbol": "CN:B", "field": "hardStop", "oldValue": 10.0, "newValue": 8.5},  # 15% — kept
        {"scope": "symbol", "symbol": "CN:C", "field": "action", "oldValue": "HOLD", "newValue": "TRIM"},
        {"scope": "gate", "field": "mode", "oldValue": "ATTACK", "newValue": "DEFEND"},
    ]
    out = filter_journal_changes(changes)
    assert len(out) == 4
    fields = [(c.get("symbol"), c.get("field")) for c in out]
    # CN:A drift 36.87→36.94 (~0.19%) is dropped as noise
    assert fields.count(("CN:A", "hardStop")) == 1  # only the 1.5% drift kept
    assert ("CN:B", "hardStop") in fields  # 15% drift kept
    assert ("CN:C", "action") in fields  # Action transition always kept
    assert (None, "mode") in fields  # Gate change always kept


def test_build_journal_markdown_filters_small_hardstop_drifts(monkeypatch):
    """End-to-end: small hardStop drifts should not appear in markdown."""
    monkeypatch.setattr(
        "data_sync_service.service.execution_journal.ej_db.fetch_latest_snapshot",
        lambda trade_date=None: {
            "id": "s1",
            "tradeDate": "2026-07-18",
            "capturedAt": "2026-07-18T08:00:00+00:00",
            "source": "manual",
            "gate": {"mode": "ATTACK"},
            "cards": [],
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
                "scope": "symbol",
                "symbol": "CN:A",
                "field": "hardStop",
                "oldValue": 36.8727,
                "newValue": 36.9402,
            },
            {
                "changedAt": "2026-07-18T07:01:00+00:00",
                "tradeDate": "2026-07-18",
                "scope": "symbol",
                "symbol": "CN:B",
                "field": "action",
                "oldValue": "HOLD",
                "newValue": "TRIM",
            },
        ],
    )
    md = build_journal_markdown(trade_date="2026-07-18")
    assert "36.8727" not in md
    assert "36.9402" not in md
    assert "HOLD" in md
    assert "TRIM" in md
    assert "hardStop drifts" in md  # Note is shown


# ---------------------------------------------------------------------------
# Upstream symbol defense (2026-08-07): malformed symbols never enter the journal
# ---------------------------------------------------------------------------


def test_is_valid_watchlist_symbol():
    from data_sync_service.service.execution_journal import is_valid_watchlist_symbol

    assert is_valid_watchlist_symbol("CN:600000")
    assert is_valid_watchlist_symbol("CN:000001")
    assert is_valid_watchlist_symbol("HK:00700")
    assert is_valid_watchlist_symbol("HK:0001")
    assert is_valid_watchlist_symbol("ETF:510300")
    assert is_valid_watchlist_symbol("cn:600000")  # case-insensitive
    # Malformed — these poisoned the journal (CN:99{uuid} test rows).
    assert not is_valid_watchlist_symbol("CN:9901ae04")
    assert not is_valid_watchlist_symbol("CN:9946eb51")
    assert not is_valid_watchlist_symbol("CN:6000000")
    assert not is_valid_watchlist_symbol("HK:007000")
    assert not is_valid_watchlist_symbol("600000")
    assert not is_valid_watchlist_symbol("")
    assert not is_valid_watchlist_symbol(None)


def test_diff_ignores_malformed_symbol_cards():
    """Cards with malformed symbols must not produce journal changes."""
    prev = {
        "id": "s1",
        "gate": {"mode": "ATTACK", "allowNewEntries": True},
        "cards": [
            {"symbol": "CN:600000", "action": "BUY", "why": "MAINLINE_OK"},
            {"symbol": "CN:9901ae04", "action": "BUY", "why": "MAINLINE_OK"},
        ],
    }
    curr_cards = [
        {"symbol": "CN:600000", "action": "HOLD", "why": "HOLD"},
        {"symbol": "CN:9946eb51", "action": "WATCH", "why": "WATCH"},
    ]
    changes = diff_snapshots(
        prev,
        {"mode": "ATTACK", "allowNewEntries": True},
        curr_cards,
        trade_date="2026-08-08",
        from_snapshot_id="s1",
        to_snapshot_id="s2",
    )
    symbols = {c.get("symbol") for c in changes}
    assert "CN:9901ae04" not in symbols
    assert "CN:9946eb51" not in symbols
    assert "CN:600000" in symbols


def test_split_valid_cards_counts_rejects():
    from data_sync_service.service.execution_journal import _split_valid_cards

    valid, rejected = _split_valid_cards(
        [
            {"symbol": "CN:600000", "action": "BUY"},
            {"symbol": "CN:9901ae04", "action": "BUY"},  # malformed
            {"symbol": "HK:00700", "action": "WATCH"},
            "not-a-dict",  # junk
            {"symbol": "", "action": "WATCH"},  # empty
        ]
    )
    assert rejected == 3
    assert [c["symbol"] for c in valid] == ["CN:600000", "HK:00700"]
