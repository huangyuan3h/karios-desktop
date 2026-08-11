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
        patch(
            "data_sync_service.service.allocation.week_weights",
            return_value={"weekStart": "2026-08-03", "decision": {"w_cn": 1.0, "w_hk": 1.0}},
        ),
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


def test_build_s3_candidates_excludes_chinext_board() -> None:
    """300xxx symbols are excluded (S3_EXCLUDE_BOARDS=('300',) — user-approved
    A4 focus-pool fix 2026-08-09); main-board and STAR pass."""
    chi_next = "CN:300001"
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {
            CN_A: 90.0,
            CN_B: 80.0,
            chi_next: 95.0,
        }
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07")
    assert sorted(c["symbol"] for c in out) == [CN_A, CN_B]


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


# ---------------------------------------------------------------------------
# RS rotation swap (2026-08-09 · user-approved S-3 enhancement)
# ---------------------------------------------------------------------------


def _patch_swap_env(holds, candidates, rs_map, closes):
    """Patch the seams _swap_holds_for_candidates relies on (pure function
    calling close_paper_trade via the module import)."""

    patchers = [
        patch.object(paper_s3, "close_paper_trade"),
        patch.object(paper_s3, "insert_paper_trade"),
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 20),
        patch.object(paper_s3, "round_trip_cost_pct", lambda m: 0.003),
    ]
    return patchers


def test_swap_holds_replaces_weak_with_strong() -> None:
    """A weak-RS held S-3 trade is closed (swapped) and the strong candidate
    is returned for insertion; the candidate is removed from the rest."""
    holds = [{"id": "h1", "symbol": CN_A, "source": "S3", "tsCode": "600001.SH",
              "entryDate": "2026-07-01", "entryPrice": 10.0}]
    cands = [{"symbol": CN_B, "ts_code": "600002.SH", "score": 90.0, "rs": 0.9,
              "regime": "Strong", "industry": "计算机"}]
    with (
        patch.object(paper_s3, "close_paper_trade") as close,
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 20),
        patch.object(paper_s3, "round_trip_cost_pct", lambda m: 0.003),
    ):
        swapped, rest = paper_s3._swap_holds_for_candidates(
            day="2026-08-07",
            holds=holds,
            candidates=cands,
            rs_by_ts={"600001.SH": 0.1, "600002.SH": 0.9},
            closes={"600001.SH": 9.5, "600002.SH": 12.0},
        )
    assert len(swapped) == 1
    assert swapped[0]["symbol"] == CN_B
    assert swapped[0]["entry_price"] == 12.0
    assert rest == []
    close.assert_called_once()
    kwargs = close.call_args.kwargs
    assert kwargs["close_reason"] == "swapped"
    assert kwargs["close_date"] == "2026-08-07"
    assert abs(kwargs["pnl_pct"] - (-5.0 - 0.3)) < 1e-6  # -5% gross - 0.3% cost


def test_swap_keeps_candidate_when_held_rs_not_weak() -> None:
    """Held RS >= SWAP_WEAK_RS_BELOW => no swap, candidate stays in rest."""
    holds = [{"id": "h1", "symbol": CN_A, "source": "S3", "tsCode": "600001.SH",
              "entryDate": "2026-07-01", "entryPrice": 10.0}]
    cands = [{"symbol": CN_B, "ts_code": "600002.SH", "score": 90.0, "rs": 0.9,
              "regime": "Strong", "industry": "计算机"}]
    with (
        patch.object(paper_s3, "close_paper_trade") as close,
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 20),
    ):
        swapped, rest = paper_s3._swap_holds_for_candidates(
            day="2026-08-07", holds=holds, candidates=cands,
            rs_by_ts={"600001.SH": 0.5, "600002.SH": 0.9},
            closes={"600001.SH": 9.5, "600002.SH": 12.0},
        )
    assert swapped == []
    assert [c["symbol"] for c in rest] == [CN_B]
    close.assert_not_called()


def test_swap_respects_min_hold_days() -> None:
    """Held below SWAP_MIN_HOLD_DAYS (mock returns 5) => no swap."""
    holds = [{"id": "h1", "symbol": CN_A, "source": "S3", "tsCode": "600001.SH",
              "entryDate": "2026-08-01", "entryPrice": 10.0}]
    cands = [{"symbol": CN_B, "ts_code": "600002.SH", "score": 90.0, "rs": 0.9,
              "regime": "Strong", "industry": "计算机"}]
    with (
        patch.object(paper_s3, "close_paper_trade") as close,
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 5),
    ):
        swapped, rest = paper_s3._swap_holds_for_candidates(
            day="2026-08-07", holds=holds, candidates=cands,
            rs_by_ts={"600001.SH": 0.1, "600002.SH": 0.9},
            closes={"600001.SH": 9.5, "600002.SH": 12.0},
        )
    assert swapped == []
    close.assert_not_called()


def test_swap_requires_strong_rs_candidate() -> None:
    """Candidate RS below SWAP_STRONG_RS_AT_LEAST cannot swap in."""
    holds = [{"id": "h1", "symbol": CN_A, "source": "S3", "tsCode": "600001.SH",
              "entryDate": "2026-07-01", "entryPrice": 10.0}]
    cands = [{"symbol": CN_B, "ts_code": "600002.SH", "score": 90.0, "rs": 0.5,
              "regime": "Strong", "industry": "计算机"}]
    with (
        patch.object(paper_s3, "close_paper_trade") as close,
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 20),
    ):
        swapped, rest = paper_s3._swap_holds_for_candidates(
            day="2026-08-07", holds=holds, candidates=cands,
            rs_by_ts={"600001.SH": 0.1, "600002.SH": 0.5},
            closes={"600001.SH": 9.5, "600002.SH": 12.0},
        )
    assert swapped == []
    close.assert_not_called()


def test_swap_caps_per_day_and_skips_missing_close() -> None:
    """Max SWAP_MAX_PER_DAY pairs per day; a hold without a close is skipped."""
    holds = [
        {"id": "h1", "symbol": CN_A, "source": "S3", "tsCode": "600001.SH",
         "entryDate": "2026-07-01", "entryPrice": 10.0},
        {"id": "h2", "symbol": "CN:600003", "source": "S3", "tsCode": "600003.SH",
         "entryDate": "2026-07-01", "entryPrice": 10.0},
        {"id": "h3", "symbol": "CN:600004", "source": "S3", "tsCode": "600004.SH",
         "entryDate": "2026-07-01", "entryPrice": 10.0},
    ]
    cands = [
        {"symbol": "CN:000001", "ts_code": "000001.SZ", "score": 90.0, "rs": 0.9,
         "regime": "Strong", "industry": "计算机"},
        {"symbol": "CN:000002", "ts_code": "000002.SZ", "score": 88.0, "rs": 0.85,
         "regime": "Strong", "industry": "计算机"},
        {"symbol": "CN:000003", "ts_code": "000003.SZ", "score": 86.0, "rs": 0.82,
         "regime": "Strong", "industry": "计算机"},
    ]
    with (
        patch.object(paper_s3, "close_paper_trade") as close,
        patch.object(paper_s3, "_holding_days_for", lambda e, d: 20),
    ):
        swapped, rest = paper_s3._swap_holds_for_candidates(
            day="2026-08-07", holds=holds, candidates=cands,
            rs_by_ts={"600001.SH": 0.1, "600003.SH": 0.2, "600004.SH": 0.25},
            closes={"600003.SH": 9.5, "600004.SH": 12.0,  # h1 skipped: no close
                    "000001.SZ": 12.0, "000002.SZ": 12.0, "000003.SZ": 12.0},
        )
    assert len(swapped) == 2  # h1 skipped (no close), h2+h3 swapped, capped at 2
    assert len(rest) == 1
    assert close.call_count == 2


# ---------------------------------------------------------------------------
# Pyramiding (2026-08-09 · user-approved, §19.2 step 8)
# ---------------------------------------------------------------------------


def _s3_hold(symbol, ts, entry, why=""):
    return {"id": f"h-{symbol}", "symbol": symbol, "source": "S3", "tsCode": ts,
            "entryDate": "2026-07-01", "entryPrice": entry, "status": "open",
            "whyAtEntry": why}


def test_pyramid_adds_half_sleeve_on_plus_10() -> None:
    """Main leg +10% -> one add leg at the same-day close, half sleeve."""
    holds = [_s3_hold("CN:600001", "600001.SH", 10.0)]
    with (
        patch.object(paper_s3, "insert_paper_trade") as insert,
        patch.object(paper_s3, "PYRAMID_ENABLED", True),
    ):
        n = paper_s3._pyramid_adds(
            day="2026-08-07", holds=holds,
            closes={"600001.SH": 11.0},  # +10%
        )
    assert n == 1
    kw = insert.call_args.kwargs
    assert kw["symbol"] == "CN:600001"
    assert kw["entry_date"] == "2026-08-07"
    assert kw["entry_price"] == 11.0
    assert abs(kw["sleeve_pct"] - 0.025) < 1e-9  # 5% * 0.5
    assert "pyramid-add" in kw["why_at_entry"]


def test_pyramid_skips_below_trigger() -> None:
    """Main leg below +2.5% -> no add."""
    holds = [_s3_hold("CN:600001", "600001.SH", 10.0)]
    with (
        patch.object(paper_s3, "insert_paper_trade") as insert,
        patch.object(paper_s3, "PYRAMID_ENABLED", True),
    ):
        n = paper_s3._pyramid_adds(
            day="2026-08-07", holds=holds, closes={"600001.SH": 10.2},  # +2%
        )
    assert n == 0
    insert.assert_not_called()


def test_pyramid_skips_when_already_added() -> None:
    """A symbol with an existing pyramid-add leg is not added again."""
    holds = [
        _s3_hold("CN:600001", "600001.SH", 10.0),
        _s3_hold("CN:600001", "600001.SH", 10.8, why="S-3 pyramid-add (main leg +8%)"),
    ]
    with (
        patch.object(paper_s3, "insert_paper_trade") as insert,
        patch.object(paper_s3, "PYRAMID_ENABLED", True),
    ):
        n = paper_s3._pyramid_adds(
            day="2026-08-07", holds=holds, closes={"600001.SH": 12.0},  # +20%
        )
    assert n == 0
    insert.assert_not_called()


def test_pyramid_disabled_by_switch() -> None:
    holds = [_s3_hold("CN:600001", "600001.SH", 10.0)]
    with (
        patch.object(paper_s3, "insert_paper_trade") as insert,
        patch.object(paper_s3, "PYRAMID_ENABLED", False),
    ):
        n = paper_s3._pyramid_adds(
            day="2026-08-07", holds=holds, closes={"600001.SH": 12.0},
        )
    assert n == 0
    insert.assert_not_called()


def test_run_intake_s3_zero_allocation_skips_new_positions() -> None:
    """T4: a 0-weight market opens no NEW positions (existing holdings keep
    their exit management — handled by paper_trading_update, not intake)."""
    with _patch_day_gates(), patch(
        "data_sync_service.service.allocation.week_weights",
        return_value={"weekStart": "2026-08-03", "decision": {"w_cn": 0.0, "w_hk": 1.0}},
    ), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 0
    assert summary["allocation"] == 0.0
    assert summary["skippedReasons"].get("allocation-zero") == 1


def test_run_intake_s3_sleeve_scaled_by_allocation() -> None:
    """T4: sleeve = 5% * week weight (here 0.4 → 2%)."""
    with _patch_day_gates(), patch(
        "data_sync_service.service.allocation.week_weights",
        return_value={"weekStart": "2026-08-03", "decision": {"w_cn": 0.4, "w_hk": 1.0}},
    ), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}
    ), patch.object(paper_s3, "insert_paper_trade", return_value={"id": "x"}) as ins:
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 1
    assert summary["allocation"] == 0.4
    kw = ins.call_args.kwargs
    assert kw["sleeve_pct"] == pytest.approx(0.02)  # 0.05 * 0.4
