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
            {},
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


def test_build_s3_candidates_blocks_cn_red_light_day() -> None:
    """OPT-094: CN red-light days return no candidates (no recommendations);
    HK stays unaffected (index lights show no separation there)."""
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        with patch("data_sync_service.service.paper_s3._index_light_red", return_value=True):
            assert paper_s3.build_s3_candidates(trade_date="2026-08-07", market="CN") == []
        # HK ignores the CN red-light check entirely.
        paper_s3._load_today_scores.return_value = {"HK:00622": 90.0}
        with patch("data_sync_service.service.paper_s3._index_light_red", return_value=True):
            assert paper_s3.build_s3_candidates(trade_date="2026-08-07", market="HK") != []


def test_index_light_red_helper() -> None:
    with patch(
        "data_sync_service.service.market_regime.get_index_signals",
        return_value=[
            {"name": "沪深300", "signal": "green"},
            {"name": "中证500", "signal": "red"},
            {"name": "创业板指", "signal": "yellow"},
        ],
    ):
        assert paper_s3._index_light_red(as_of="2026-08-07") is True
    with patch(
        "data_sync_service.service.market_regime.get_index_signals",
        return_value=[
            {"name": "沪深300", "signal": "green"},
            {"name": "中证500", "signal": "green"},
        ],
    ):
        assert paper_s3._index_light_red(as_of="2026-08-07") is False


def test_circuit_blocked_losing_streak() -> None:
    """Drawdown circuit (2026-08-12): trailing 30d realized net pnl <= -25%
    (>= 3 trades) blocks new CN S-3 entries — the long-window bear-market
    defence (2022/2023), mirroring backtest drawdown_circuit_pct=-25."""

    closed = [
        {"symbol": "CN:600001", "status": "closed", "closeDate": "2026-07-20",
         "close_date": "2026-07-20", "pnlPct": -6.0, "pnl_pct": -6.0},
        {"symbol": "CN:600002", "status": "closed", "closeDate": "2026-07-25",
         "close_date": "2026-07-25", "pnlPct": -5.5, "pnl_pct": -5.5},
        {"symbol": "CN:600003", "status": "closed", "closeDate": "2026-08-01",
         "close_date": "2026-08-01", "pnlPct": -14.0, "pnl_pct": -14.0},
    ]
    with patch("data_sync_service.service.paper_s3.list_paper_trades", return_value=closed):
        assert paper_s3._circuit_blocked(as_of="2026-08-07") is True


def test_circuit_not_blocked_fresh_profit() -> None:
    """A healthy (profitable) recent window must NOT block entries."""
    closed = [
        {"symbol": "CN:600001", "status": "closed", "closeDate": "2026-07-20",
         "close_date": "2026-07-20", "pnlPct": 8.0, "pnl_pct": 8.0},
        {"symbol": "CN:600002", "status": "closed", "closeDate": "2026-07-25",
         "close_date": "2026-07-25", "pnlPct": 12.0, "pnl_pct": 12.0},
    ]
    with patch("data_sync_service.service.paper_s3.list_paper_trades", return_value=closed):
        assert paper_s3._circuit_blocked(as_of="2026-08-07") is False


def test_circuit_ignores_stale_trades() -> None:
    """Trades older than the 30d window must not count."""
    closed = [
        {"symbol": "CN:600001", "status": "closed", "closeDate": "2026-06-01",
         "close_date": "2026-06-01", "pnlPct": -30.0, "pnl_pct": -30.0},
        {"symbol": "CN:600002", "status": "closed", "closeDate": "2026-06-02",
         "close_date": "2026-06-02", "pnlPct": -30.0, "pnl_pct": -30.0},
    ]
    with patch("data_sync_service.service.paper_s3.list_paper_trades", return_value=closed):
        assert paper_s3._circuit_blocked(as_of="2026-08-07") is False


def test_build_s3_candidates_blocked_by_circuit() -> None:
    """End-to-end: circuit on → build_s3_candidates returns [] for CN."""
    with (
        _patch_day_gates(regime="Strong"),
        patch("data_sync_service.service.paper_s3._circuit_blocked", return_value=True),
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_blocks_flow_outflow() -> None:
    with _patch_day_gates(flow_ok=False):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_hk_rs_floor_06() -> None:
    """HK line RS floor is 0.6 (HK backtest baseline rs_rank_min) — names with
    RS in [0.5, 0.6) are excluded on the HK line but would pass the CN floor."""
    hk_a = "HK:00700"
    hk_b = "HK:01277"
    with (
        patch.multiple(
            paper_s3,
            _load_regime_by_day=lambda cfg, cal: {"2026-08-07": "Strong"},
            _load_rs_ranks=lambda cfg, cal, universe: {
                "2026-08-07": {ts: (0.55 if ts.startswith("00700") else 0.7) for ts in universe}
            },
        ),
        patch.object(paper_s3, "_load_today_scores", return_value={hk_a: 90.0, hk_b: 88.0}),
    ):
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07", market="HK")
    assert [c["symbol"] for c in out] == [hk_b]


def test_build_s3_candidates_blocks_low_rs() -> None:
    with patch.multiple(
        paper_s3,
        _load_regime_by_day=lambda cfg, cal: {"2026-08-07": "Strong"},
        _load_flow_mainline_data=lambda cfg, cal: ({"2026-08-07": True}, {"2026-08-07": {"计算机"}}, {}),
        _load_rs_ranks=lambda cfg, cal, universe: {"2026-08-07": {ts: 0.3 for ts in universe}},
        _load_industries=lambda ts_codes: {ts: "计算机" for ts in ts_codes},
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_rs_fallback_intraday() -> None:
    """Intraday (before 17:10 close_sync): today's daily bars are absent so
    the RS percentile falls back to the latest available RS day (previous
    session's close) — the intraday S-3 surface must not be empty. The EOD
    chain re-evaluates with today's close."""
    day = "2026-08-07"
    prev = "2026-08-06"

    def fake_rs(cfg, cal, universe):
        if cfg.end_date == day:
            return {}  # no daily rows for today yet (intraday)
        assert cfg.end_date == prev
        return {prev: {ts: 0.75 for ts in universe}}

    with (
        _patch_day_gates(),
        patch.object(paper_s3, "_load_rs_ranks", side_effect=fake_rs),
        patch.object(paper_s3, "_latest_daily_date_before", return_value=prev),
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        out = paper_s3.build_s3_candidates(trade_date=day)
    assert [c["symbol"] for c in out] == [CN_A]
    assert out[0]["rs"] == 0.75


def test_build_s3_candidates_rs_fallback_missing_blocks() -> None:
    """No RS for today AND no fallback day available → stay fail-closed."""
    day = "2026-08-07"
    with (
        _patch_day_gates(),
        patch.object(paper_s3, "_load_rs_ranks", return_value={}),
        patch.object(paper_s3, "_latest_daily_date_before", return_value=None),
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        assert paper_s3.build_s3_candidates(trade_date=day) == []


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
    assert abs(kw["sleeve_pct"] - 0.05) < 1e-9  # 10% * 0.5 (2026-08-11: paper = backtest 10%)
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
    """T4: sleeve = 10% * week weight (here 0.4 → 4%)."""
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
    assert kw["sleeve_pct"] == pytest.approx(0.04)  # 0.10 * 0.4


def test_run_intake_s3_sleeve_env_scaled_uptrend_day() -> None:
    """D3: uptrend day → sleeve 10% * 1.25 = 12.5%."""
    with _patch_day_gates(), patch.object(
        paper_s3, "get_cn_sentiment",
        return_value={"items": [{"riskMode": "hot", "upCount": 300, "downCount": 100,
                                 "yesterdayLimitupPremium": 1.0}]},
    ), patch.object(
        paper_s3, "_ret5_for", return_value=10.0,
    ), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}
    ), patch.object(paper_s3, "insert_paper_trade", return_value={"id": "x"}) as ins:
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 1
    kw = ins.call_args.kwargs
    assert kw["sleeve_pct"] == pytest.approx(0.125)  # 0.10 * 1.25


def test_run_intake_s3_sleeve_env_scaled_fan_day() -> None:
    """D3: fan day → sleeve 10% * 0.75 = 7.5%."""
    with _patch_day_gates(), patch.object(
        paper_s3, "get_cn_sentiment",
        return_value={"items": [{"riskMode": "normal", "upCount": 100, "downCount": 100,
                                 "yesterdayLimitupPremium": 0.0}]},
    ), patch.object(
        paper_s3, "_ret5_for", return_value=-5.0,
    ), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value={"600001.SH": [("2026-08-07", 10, 10, 10, 10.5, 1000)]}
    ), patch.object(paper_s3, "insert_paper_trade", return_value={"id": "x"}) as ins:
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        summary = paper_s3.run_intake_s3(trade_date="2026-08-07")
    assert summary["inserted"] == 1
    kw = ins.call_args.kwargs
    assert kw["sleeve_pct"] == pytest.approx(0.075)  # 0.10 * 0.75


def test_env_position_scale_for_mapping() -> None:
    """D3: env → sleeve scale mapping (uptrend 1.25 / fan 0.75 / others 1.0)."""
    assert paper_s3._env_position_scale_for(
        [{"riskMode": "hot", "upCount": 300, "downCount": 100, "yesterdayLimitupPremium": 1.0}]
    ) == 1.25
    assert paper_s3._env_position_scale_for(
        [{"riskMode": "normal", "upCount": 100, "downCount": 100, "yesterdayLimitupPremium": 0.0}]
    ) == 0.75
    assert paper_s3._env_position_scale_for(
        [{"riskMode": "extreme_caution", "upCount": 100, "downCount": 100, "yesterdayLimitupPremium": 0.0}]
    ) == 1.0  # weak env not mapped
    assert paper_s3._env_position_scale_for([]) == 1.0


def test_signal_snapshot_for_captures_flow_and_alpha(monkeypatch) -> None:
    """C4 seed: entry snapshot carries industry flow rank + alpha count."""
    from data_sync_service.service import portfolio_health as ph

    monkeypatch.setattr(ph, "_industry_flow_map", lambda day: {
        "通信": {"industry": "通信", "netInflow5d": -47.69, "rank5d": 26, "total": 31},
    })
    monkeypatch.setattr(ph, "_alpha_events_for_symbols", lambda syms: {
        "CN:300628": [{"trend": "通信设备景气", "grade": "B"}],
    })
    snap = paper_s3._signal_snapshot_for(
        symbol="CN:300628", industry="通信", trade_date="2026-08-12",
    )
    assert snap["industryRank5d"] == 26
    assert snap["industryTotal"] == 31
    assert snap["industryNetInflow5d"] == -47.69
    assert snap["alphaEvents"] == 1


def test_signal_snapshot_hk_normalizes_symbol(monkeypatch) -> None:
    from data_sync_service.service import portfolio_health as ph

    monkeypatch.setattr(ph, "_industry_flow_map", lambda day: {})
    monkeypatch.setattr(ph, "_alpha_events_for_symbols", lambda syms: {
        "HK:02099": [{"trend": "黄金牛市", "grade": "A"}],
    })
    snap = paper_s3._signal_snapshot_for(
        symbol="HK:2099", industry=None, trade_date="2026-08-12",
    )
    assert snap["alphaEvents"] == 1


def test_signal_snapshot_none_when_no_data(monkeypatch) -> None:
    from data_sync_service.service import portfolio_health as ph

    monkeypatch.setattr(ph, "_industry_flow_map", lambda day: {})
    monkeypatch.setattr(ph, "_alpha_events_for_symbols", lambda syms: {})
    assert paper_s3._signal_snapshot_for(
        symbol="CN:300628", industry="通信", trade_date="2026-08-12",
    ) is None


def test_build_s3_candidates_blocks_implicit_weak_breadth() -> None:
    """TIP-014: up/down < 0.5 with normal risk_mode blocks CN candidates."""
    with _patch_day_gates():
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        paper_s3.get_cn_sentiment.return_value = {
            "items": [
                {
                    "riskMode": "normal",
                    "upCount": 800,
                    "downCount": 3200,
                }
            ]
        }
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []


def test_build_s3_candidates_allows_balanced_breadth() -> None:
    """up/down = 1.0 with normal risk_mode → fan day; a pullback strong
    stock (RS>=0.7, 5d <= -3%) passes the dip filter."""
    with _patch_day_gates(), patch.object(
        paper_s3, "fetch_last_ohlcv_batch",
        return_value={"600001.SH": [
            ("2026-07-31", 11.0, 11.0, 11.0, 11.0, 1000),
            ("2026-08-03", 11.0, 11.0, 11.0, 11.0, 1000),
            ("2026-08-04", 11.0, 11.0, 11.0, 11.0, 1000),
            ("2026-08-05", 11.0, 11.0, 11.0, 11.0, 1000),
            ("2026-08-06", 11.0, 11.0, 11.0, 11.0, 1000),
            ("2026-08-07", 10.5, 10.5, 10.5, 10.5, 1000),
        ]},
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        paper_s3.get_cn_sentiment.return_value = {
            "items": [
                {
                    "riskMode": "normal",
                    "upCount": 2000,
                    "downCount": 2000,
                    "yesterdayLimitupPremium": 0.5,
                }
            ]
        }
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07")
        # 5d return = 10.5/11 - 1 = -4.5% <= -3% → dip passes
        assert [c["symbol"] for c in out] == [CN_A]


def test_build_s3_candidates_fan_day_rejects_momentum_names() -> None:
    """Fan day: a strong stock WITHOUT a pullback (5d >= -3%) is rejected."""
    with _patch_day_gates(), patch.object(
        paper_s3, "fetch_last_ohlcv_batch",
        return_value={"600001.SH": [
            ("2026-07-31", 10.0, 10.0, 10.0, 10.0, 1000),
            ("2026-08-03", 10.0, 10.0, 10.0, 10.0, 1000),
            ("2026-08-04", 10.0, 10.0, 10.0, 10.0, 1000),
            ("2026-08-05", 10.0, 10.0, 10.0, 10.0, 1000),
            ("2026-08-06", 10.0, 10.0, 10.0, 10.0, 1000),
            ("2026-08-07", 10.6, 10.6, 10.6, 10.6, 1000),
        ]},
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        paper_s3.get_cn_sentiment.return_value = {
            "items": [
                {
                    "riskMode": "normal",
                    "upCount": 2000,
                    "downCount": 2000,
                    "yesterdayLimitupPremium": 0.5,
                }
            ]
        }
        out = paper_s3.build_s3_candidates(trade_date="2026-08-07")
        # 5d return = 10.6/10 - 1 = +6% → momentum name, fan day rejects
        assert out == []


def test_build_s3_candidates_uptrend_day_momentum_filter() -> None:
    """Uptrend day: a pullback strong stock (5d <= -3%) is rejected; a
    momentum stock (5d >= -3%) passes."""
    bars_pullback = {"600001.SH": [
        ("2026-07-31", 11.0, 11.0, 11.0, 11.0, 1000),
        ("2026-08-03", 11.0, 11.0, 11.0, 11.0, 1000),
        ("2026-08-04", 11.0, 11.0, 11.0, 11.0, 1000),
        ("2026-08-05", 11.0, 11.0, 11.0, 11.0, 1000),
        ("2026-08-06", 11.0, 11.0, 11.0, 11.0, 1000),
        ("2026-08-07", 10.5, 10.5, 10.5, 10.5, 1000),
    ]}
    with _patch_day_gates(), patch.object(
        paper_s3, "fetch_last_ohlcv_batch", return_value=bars_pullback
    ):
        paper_s3._load_today_scores.return_value = {CN_A: 90.0}
        paper_s3.get_cn_sentiment.return_value = {
            "items": [
                {
                    "riskMode": "hot",
                    "upCount": 4000,
                    "downCount": 1000,
                    "yesterdayLimitupPremium": 2.0,
                }
            ]
        }
        # 5d = -4.5% → pullback → momentum filter rejects
        assert paper_s3.build_s3_candidates(trade_date="2026-08-07") == []
