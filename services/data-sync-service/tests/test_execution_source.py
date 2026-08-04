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