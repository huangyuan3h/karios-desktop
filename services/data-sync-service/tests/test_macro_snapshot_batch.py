"""Batch read tests for macro snapshot (OPT-024)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service.macro_snapshot import (  # type: ignore[import-not-found]
    MACRO_CARDS,
    build_macro_snapshot,
)
from data_sync_service.service.option_iv import (
    PUT_IV_LIVE_FETCH_FAILED_USING_DB,  # type: ignore[import-not-found]
)


def _patch_macro_snapshot_deps(**resolve_put_iv_kwargs):
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
    resolve_default = {
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
    }
    resolve_default.update(resolve_put_iv_kwargs)
    return (
        patch(
            "data_sync_service.service.macro_snapshot.get_index_signals",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.macro_snapshot.fetch_last_closes_batch",
            return_value=closes,
        ),
        patch(
            "data_sync_service.service.macro_snapshot.get_latest_rows_batch",
            return_value=latest,
        ),
        patch(
            "data_sync_service.service.macro_snapshot.fetch_last_closes",
        ),
        patch(
            "data_sync_service.service.macro_snapshot.get_latest_row",
        ),
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
            return_value=resolve_default,
        ),
        patch(
            "data_sync_service.service.macro_snapshot._macro_as_of_stale",
            return_value=False,
        ),
    )


def test_build_macro_snapshot_uses_batch_db_reads() -> None:
    patches = _patch_macro_snapshot_deps()
    with (
        patches[0],
        patches[1] as fetch_batch,
        patches[2] as latest_batch,
        patches[3] as fetch_single,
        patches[4] as get_single,
        patches[5],
        patches[6],
        patches[7],
        patches[8] as resolve_put_iv,
        patches[9],
    ):
        out = build_macro_snapshot()

    resolve_put_iv.assert_called_once_with(write_db=True)
    fetch_batch.assert_called_once()
    latest_batch.assert_called_once()
    fetch_single.assert_not_called()
    get_single.assert_not_called()
    assert "macro" in out
    assert len(out["macro"]) == len(MACRO_CARDS)
    assert "warning" not in out


def test_build_macro_snapshot_soft_put_iv_fallback_not_page_warning() -> None:
    patches = _patch_macro_snapshot_deps(
        close=18.5,
        realtime=False,
        source="macro_daily",
        warning=PUT_IV_LIVE_FETCH_FAILED_USING_DB,
        diagnostics={"error": "no_510300_put_iv_candidate"},
    )
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8], patches[9]:
        out = build_macro_snapshot()

    assert "warning" not in out
    put = next(m for m in out["macro"] if m.get("seriesId") == "510300_PUT_IV")
    assert put["warning"] == PUT_IV_LIVE_FETCH_FAILED_USING_DB
    assert put["realtime"] is False
