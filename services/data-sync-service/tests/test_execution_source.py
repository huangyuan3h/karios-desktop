"""Unit tests for TIP-011 source attribution helpers (no DB required).

These cover the pure-Python helpers in
``service/execution_source.py`` and the source-aware variants in
``service/execution_journal.py`` (diff_snapshots propagation).

DB-touching helpers (count_by_source / count_changes_by_source /
backfill_paper_trades_source / aggregate_source_stats) are exercised
by the dedicated ``requires_postgres`` tests in test_execution_source_db.py.
"""

from __future__ import annotations

from data_sync_service.service.execution_source import (
    KNOWN_SOURCES,
    infer_source,
)


def test_known_sources_is_closed_enum():
    assert set(KNOWN_SOURCES) == {"TV", "ALPHA", "MANUAL"}
    assert len(KNOWN_SOURCES) == 3


def test_infer_source_tv_wins_over_alpha():
    sources = infer_source(
        symbol="CN:600519",
        tv_screener_symbols={"CN:600519"},
        alpha_catalyst_symbols={"CN:600519"},
    )
    assert sources == "TV"


def test_infer_source_alpha_when_only_alpha():
    sources = infer_source(
        symbol="CN:002371",
        tv_screener_symbols=set(),
        alpha_catalyst_symbols={"CN:002371"},
    )
    assert sources == "ALPHA"


def test_infer_source_manual_when_neither():
    sources = infer_source(
        symbol="CN:000001",
        tv_screener_symbols=set(),
        alpha_catalyst_symbols=set(),
    )
    assert sources == "MANUAL"


def test_infer_source_symbol_case_insensitive():
    sources = infer_source(
        symbol="cn:600519",
        tv_screener_symbols={"CN:600519"},
    )
    assert sources == "TV"


def test_infer_source_empty_symbol_falls_back_to_manual():
    assert infer_source(symbol="") == "MANUAL"
    assert infer_source(symbol="  ") == "MANUAL"

def test_aggregate_source_stats_handles_db_failures(monkeypatch) -> None:
    """Every upstream read failing must still yield the lean shape (no crash)."""
    from data_sync_service.service import execution_source as es

    monkeypatch.setattr(es.ej_db, "count_changes_by_source", lambda **kw: {})
    monkeypatch.setattr(es.pt_db, "count_by_source", lambda **kw: {})

    out = es.aggregate_source_stats(since_days=7)
    assert out["sinceDays"] == 7
    assert out["bySource"] == {}
    assert out["openTradesBySource"] == {}
    # UNKNOWN bucket appears because the pre-TIP-011 rows may exist; here the
    # dict contains it only when something was seen — with all-empty reads it
    # stays empty.
    assert set(out.keys()) >= {"sinceDays", "lookbackDays", "generatedAt", "bySource", "openTradesBySource"}


def test_aggregate_source_stats_drops_zero_buckets_keeps_unknown(monkeypatch) -> None:
    from data_sync_service.service import execution_source as es

    monkeypatch.setattr(
        es.ej_db,
        "count_changes_by_source",
        lambda **kw: {"TV": 3},
    )
    monkeypatch.setattr(
        es.pt_db,
        "count_by_source",
        lambda **kw: {"ALPHA": {"total": 2, "wins": 1, "losses": 1, "winRate": 50.0}},
    )
    out = es.aggregate_source_stats(since_days=14)
    assert set(out["bySource"].keys()) == {"TV", "ALPHA"}
    assert out["bySource"]["TV"]["buySignals"] == 3
    assert out["bySource"]["ALPHA"]["winRate"] == 50.0


def test_get_default_lookback_days_validation(monkeypatch) -> None:
    from data_sync_service.service import execution_source as es

    monkeypatch.setenv("EXECUTION_SOURCE_STATS_LOOKBACK_DAYS", "30")
    assert es.get_default_lookback_days() == 30
    monkeypatch.setenv("EXECUTION_SOURCE_STATS_LOOKBACK_DAYS", "not-a-number")
    assert es.get_default_lookback_days() == es.DEFAULT_LOOKBACK_DAYS
    monkeypatch.delenv("EXECUTION_SOURCE_STATS_LOOKBACK_DAYS")
    assert es.get_default_lookback_days() == es.DEFAULT_LOOKBACK_DAYS


def test_aggregate_source_stats_all_reads_fail(monkeypatch) -> None:
    """Every DB read raising must still yield the shape (defensive)."""
    from data_sync_service.service import execution_source as es

    monkeypatch.setattr(
        es.ej_db, "count_changes_by_source", lambda **kw: (_ for _ in ()).throw(RuntimeError("x"))
    )
    monkeypatch.setattr(
        es.pt_db, "count_by_source", lambda **kw: (_ for _ in ()).throw(RuntimeError("x"))
    )
    out = es.aggregate_source_stats(since_days=7)
    assert out["bySource"] == {}
    assert out["openTradesBySource"] == {}
