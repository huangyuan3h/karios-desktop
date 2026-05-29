"""Tests for Alpha Radar structured trend fields."""

from __future__ import annotations

import threading

from data_sync_service.db.alpha_radar import _trend_row, ensure_tables
from data_sync_service.service.alpha_radar_process import _resolve_trend_storage_fields


def test_resolve_trend_storage_fields_prefers_macro_theme_and_catalyst_grade():
    fields = _resolve_trend_storage_fields(
        {
            "macro_theme": "Next-Gen Energy",
            "catalyst_grade": "S",
            "trend_name": "Next-Gen Energy (储能)",
            "urgency_level": "B",
        }
    )
    assert fields["macro_theme"] == "Next-Gen Energy"
    assert fields["catalyst_grade"] == "S"
    assert fields["urgency_level"] == "S"
    assert fields["trend_name"] == "Next-Gen Energy (储能)"


def test_resolve_trend_storage_fields_falls_back_to_legacy_names():
    fields = _resolve_trend_storage_fields(
        {
            "trend_name": "HBM Supply Chain",
            "urgency_level": "A",
        }
    )
    assert fields["macro_theme"] == "HBM Supply Chain"
    assert fields["catalyst_grade"] == "A"
    assert fields["trend_name"] == "HBM Supply Chain"


def test_trend_row_exposes_macro_theme_and_catalyst_grade():
    row = (
        "id1",
        "doc1",
        "Legacy Name",
        "catalyst text",
        "NVDA",
        "A",
        "Next-Gen Energy",
        "S",
        '["液冷"]',
        "[]",
        None,
        "waiting_v2_flow",
        "{}",
        "2026-01-01T00:00:00+00:00",
    )
    item = _trend_row(row)
    assert item["macroTheme"] == "Next-Gen Energy"
    assert item["catalystGrade"] == "S"
    assert item["trendName"] == "Legacy Name"


def test_trend_row_falls_back_when_macro_fields_null():
    row = (
        "id1",
        "doc1",
        "Legacy Name",
        None,
        None,
        "B",
        None,
        None,
        "[]",
        "[]",
        None,
        "waiting_v2_flow",
        "{}",
        "2026-01-01T00:00:00+00:00",
    )
    item = _trend_row(row)
    assert item["macroTheme"] == "Legacy Name"
    assert item["catalystGrade"] == "B"


def test_ensure_tables_is_safe_under_concurrent_calls():
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            ensure_tables()
        except BaseException as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
