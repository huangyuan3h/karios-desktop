"""Tests for Alpha Radar structured trend fields."""

from __future__ import annotations

import threading

from data_sync_service.db.alpha_radar import _trend_row, ensure_tables
from data_sync_service.service.alpha_radar_process import _resolve_trend_storage_fields


def test_resolve_trend_storage_fields_v4():
    fields = _resolve_trend_storage_fields(
        {
            "macro_theme": "国家级设备更新",
            "driver_type": "Domestic_Policy",
            "catalyst_grade": "S",
            "event_focus": "发改委下达1万亿超长期特别国债",
            "a_share_mapping": ["三一重工"],
            "logic_summary": "万亿国债驱动设备更新",
        },
        category_hint="policy",
    )
    assert fields is not None
    assert fields["macro_theme"] == "国家级设备更新"
    assert fields["catalyst_grade"] == "S"
    assert fields["driver_type"] == "Domestic_Policy"
    assert fields["event_focus"].startswith("发改委")
    assert len(fields["logic_summary"]) <= 30


def test_resolve_trend_storage_fields_drops_b_grade():
    fields = _resolve_trend_storage_fields(
        {
            "macro_theme": "Noise",
            "catalyst_grade": "B",
            "event_focus": "minor",
        }
    )
    assert fields is None


def test_resolve_trend_storage_fields_infers_driver_from_category():
    fields = _resolve_trend_storage_fields(
        {
            "macro_theme": "铜供给挤压",
            "catalyst_grade": "A",
            "event_focus": "铜价连续上涨",
        },
        category_hint="cycle",
    )
    assert fields is not None
    assert fields["driver_type"] == "Cycle_Reversal"


def test_trend_row_exposes_v4_fields():
    row = (
        "id1",
        "doc1",
        "国家级设备更新",
        "发改委下达1万亿超长期特别国债",
        None,
        "S",
        "国家级设备更新",
        "S",
        "Domestic_Policy",
        "发改委下达1万亿超长期特别国债",
        "万亿国债驱动设备更新",
        '["三一重工"]',
        "[]",
        None,
        "waiting_v2_flow",
        "{}",
        "2026-01-01T00:00:00+00:00",
    )
    item = _trend_row(row)
    assert item["macroTheme"] == "国家级设备更新"
    assert item["catalystGrade"] == "S"
    assert item["driverType"] == "Domestic_Policy"
    assert item["eventFocus"].startswith("发改委")
    assert item["logicSummary"] == "万亿国债驱动设备更新"


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
        None,
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
    assert item["driverType"] == "Global_Tech"


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
