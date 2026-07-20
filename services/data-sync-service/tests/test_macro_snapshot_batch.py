"""Batch read tests for macro snapshot (OPT-024)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.macro_snapshot import (  # type: ignore[import-not-found]
    MACRO_CARDS,
    build_macro_snapshot,
)


def test_build_macro_snapshot_uses_batch_db_reads() -> None:
    sids = [str(m["seriesId"]) for m in MACRO_CARDS]
    closes = {sid: [("2024-06-17", 100.0), ("2024-06-18", 101.0)] for sid in sids}
    latest = {
        sid: {
            "series_id": sid,
            "trade_date": "2024-06-18",
            "source": "test",
            "underlying_ts_code": None,
            "pct_chg": 1.0,
        }
        for sid in sids
    }
    with (
        patch(
            "data_sync_service.service.macro_snapshot.get_index_signals",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.macro_snapshot.fetch_last_closes_batch",
            return_value=closes,
        ) as fetch_batch,
        patch(
            "data_sync_service.service.macro_snapshot.get_latest_rows_batch",
            return_value=latest,
        ) as latest_batch,
        patch(
            "data_sync_service.service.macro_snapshot.fetch_last_closes",
        ) as fetch_single,
        patch(
            "data_sync_service.service.macro_snapshot.get_latest_row",
        ) as get_single,
        patch(
            "data_sync_service.service.macro_snapshot.fetch_realtime_quotes",
            return_value={"ok": True, "items": []},
        ),
        patch(
            "data_sync_service.service.macro_snapshot.enrich_macro_items_on_demand",
            side_effect=lambda items: items,
        ),
        patch(
            "data_sync_service.service.macro_snapshot.macro_snapshot_warning",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.macro_snapshot.resolve_put_iv_for_snapshot",
            return_value={
                "close": 24.0,
                "asOfDate": "2024-06-18",
                "pctChg": 2.0,
                "source": "eastmoney",
                "underlyingTsCode": "510300.SH",
                "realtime": True,
                "signal": "yellow",
                "signalLabel": "Elevated Fear",
                "warning": None,
                "diagnostics": {},
            },
        ) as resolve_put_iv,
    ):
        out = build_macro_snapshot()

    resolve_put_iv.assert_called_once_with(write_db=True)
    fetch_batch.assert_called_once()
    latest_batch.assert_called_once()
    fetch_single.assert_not_called()
    get_single.assert_not_called()
    assert "macro" in out
    assert len(out["macro"]) == len(MACRO_CARDS)
