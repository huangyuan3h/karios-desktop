"""G4 tests: S-3 paper intake (service/paper_s3.py).

Unit tests patch the DB/service seams so they run anywhere:

- ``_load_today_scores`` (DB read) is patched per test.
- ``_load_regime_by_day`` / ``_load_flow_mainline_data`` / ``_load_rs_ranks``
  are backtest-engine DB loaders — patched with in-memory maps.
- ``get_panic_cooldown`` / ``get_cn_sentiment`` patched to control the
  panic protection; ``_live_held_symbols`` / ``_open_paper_symbols`` patched.
- ``insert_paper_trade`` patched to record calls (no DB writes).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from data_sync_service.service import paper_s3

CN_A = "CN:600001"
CN_B = "CN:600002"


@pytest.fixture(autouse=True)
def _patch_loaders():
    with (
        patch.object(paper_s3, "_load_today_scores", return_value={}),
        patch.object(paper_s3, "_live_held_symbols", return_value=set()),
        patch.object(paper_s3, "_open_paper_symbols", return_value=set()),
        patch.object(
            paper_s3,
            "get_panic_cooldown",
            return_value={"lastPanicDate": None, "cooldownEndDate": None, "active": False},
        ),
        patch.object(paper_s3, "get_cn_sentiment", return_value={"items": []}),
        patch.object(paper_s3, "BacktestConfig", wraps=paper_s3.BacktestConfig),
    ):
        yield


def _patch_day_gates(*, regime="Strong", flow_ok=True, mainline=None):
    day = "2026-08-07"
    return patch.multiple(
        paper_s3,
        _load_regime_by_day=lambda cfg, cal: {day: regime},
        _load_flow_mainline_data=lambda cfg, cal: (
            {day: flow_ok},
            {day: set(mainline or ["计算机"])},
        ),
        _load_rs_ranks=lambda cfg, cal, universe: {
            day: {ts: 0.8 for ts in universe}
        },
        _load_industries=lambda ts_codes: {ts: "计算机" for ts in ts_codes},
    )


def test_build_s3_candidates_basic() -> None:
    """Score + RS + mainline + non-Weak regime => candidates in score order."""
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {CN_A: 90.0, CN_B: 70.0}
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07")
    assert [c["symbol"] for c in out] == [CN_A, CN_B]
    assert out[0]["score"] == 90.0
    assert out[0]["regime"] == "Strong"


def test_build_s3_candidates_blocks_weak_regime() -> None:
    with _patch_day_gates(regime="Weak"):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_blocks_flow_outflow() -> None:
    with _patch_day_gates(flow_ok=False):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_blocks_low_rs() -> None:
    with patch.multiple(
        paper_s3,
        _load_regime_by_day=lambda cfg, cal: {"2026-08-07": "Strong"},
        _load_flow_mainline_data=lambda cfg, cal: ({"2026-08-07": True}, {"2026-08-07": {"计算机"}}),
        _load_rs_ranks=lambda cfg, cal, universe: {"2026-08-07": {ts: 0.3 for ts in universe}},
        _load_industries=lambda ts_codes: {ts: "计算机" for ts in ts_codes},
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_blocks_panic_cooldown() -> None:
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        paper_s3.get_panic_cooldown.return_value = {
            "lastPanicDate": "2026-08-05",
            "cooldownEndDate": "2026-08-11",
            "active": True,
        }
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_blocks_held_and_open() -> None:
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {CN_A: 90.0, CN_B: 70.0}
        paper_s3._live_held_symbols.return_value = {CN_A}
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07")
        assert [c["symbol"] for c in out] == [CN_B]


def test_run_intake_s3_inserts_with_s3_source() -> None:
    with _patch_day_gates(), patch.object(
        paper_s3, "_lookup_stock_basic", return_value=({"600001.SH": "测试A"}, {})
    ), patch.object(paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        inserted: list[dict] = []

        def fake_insert(**kw):
            inserted.append(kw)
            return {"symbol": kw["symbol"]}

        with patch.object(paper_s3, "insert_paper_trade", side_effect=fake_insert):
            summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 1
    row = inserted[0]
    assert row["symbol"] == CN_A
    assert row["side"] == "BUY"
    assert row["source"] == "S3"
    assert row["score_at_entry"] == 90.0
    assert row["entry_price"] == 10.5
    assert row["sleeve_pct"] == paper_s3.S3_POSITION_PCT
    assert "S-3" in row["why_at_entry"]


def test_run_intake_s3_idempotent_duplicate() -> None:
    with _patch_day_gates(), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        with patch.object(paper_s3, "insert_paper_trade", return_value=None):
            summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 0
    assert summary["skippedReasons"].get("duplicate") == 1


def test_run_intake_s3_missing_close_skipped() -> None:
    with _patch_day_gates(), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={}
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 0
    assert summary["skippedReasons"].get("no-close-price") == 1
