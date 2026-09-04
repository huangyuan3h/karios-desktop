"""Unit tests for paper next_open fill helper (no DB required)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.paper_entry_fill import (
    merge_entry_snapshot,
    resolve_next_open_fill,
    try_resolve_pending_open,
)


def test_resolve_uses_next_session_open_when_bar_exists() -> None:
    with (
        patch(
            "data_sync_service.service.paper_entry_fill._next_session_after",
            return_value="2026-08-08",
        ),
        patch(
            "data_sync_service.service.paper_entry_fill.fetch_ohlcv_batch_between",
            return_value={
                "600000.SH": [("2026-08-08", "10.5", "11", "10", "10.8", "1")],
            },
        ),
    ):
        fill = resolve_next_open_fill("600000.SH", "2026-08-07", signal_close=10.0)
    assert fill is not None
    assert fill["entry_date"] == "2026-08-08"
    assert fill["entry_price"] == 10.5
    assert fill["pending_open_fill"] is False
    assert fill["signal_snapshot"]["entryMode"] == "next_open"
    assert fill["signal_snapshot"]["pendingOpenFill"] is False


def test_resolve_placeholder_when_open_missing() -> None:
    with (
        patch(
            "data_sync_service.service.paper_entry_fill._next_session_after",
            return_value="2026-08-08",
        ),
        patch(
            "data_sync_service.service.paper_entry_fill.fetch_ohlcv_batch_between",
            return_value={"600000.SH": []},
        ),
    ):
        fill = resolve_next_open_fill("600000.SH", "2026-08-07", signal_close=9.9)
    assert fill is not None
    assert fill["entry_date"] == "2026-08-08"
    assert fill["entry_price"] == 9.9
    assert fill["pending_open_fill"] is True
    assert fill["signal_snapshot"]["pendingOpenFill"] is True


def test_merge_preserves_entry_env() -> None:
    out = merge_entry_snapshot(
        {"entryEnv": "uptrend"},
        {"signal_snapshot": {"entryMode": "next_open", "pendingOpenFill": True}},
    )
    assert out["entryEnv"] == "uptrend"
    assert out["entryMode"] == "next_open"


def test_try_resolve_pending_open() -> None:
    with patch(
        "data_sync_service.service.paper_entry_fill.fetch_ohlcv_batch_between",
        return_value={"600000.SH": [("2026-08-08", "11.2", "12", "11", "11.5", "1")]},
    ):
        px = try_resolve_pending_open(
            ts_code="600000.SH",
            entry_date="2026-08-08",
            signal_snapshot={"entryMode": "next_open", "pendingOpenFill": True},
        )
    assert px == 11.2
