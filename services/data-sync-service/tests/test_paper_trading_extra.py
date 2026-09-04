"""service/paper_trading.py remaining branches (error guards / skip paths)."""

from __future__ import annotations

from unittest.mock import Mock

from data_sync_service.service import paper_trading as pt

CN_CH = {"field": "action", "newValue": "BUY", "symbol": "CN:600519", "source": "ALPHA"}
ADD_CH = {"field": "action", "newValue": "ADD", "symbol": "HK:00700", "source": None}
ETF_CH = {"field": "action", "newValue": "BUY", "symbol": "ETF:510300", "source": "alpha"}


def _patch_all(monkeypatch, *, closes=None, registry=None, changes=None, raise_changes=False, raise_registry=False, raise_closes=False):  # noqa: ANN001, ANN003
    from data_sync_service.db import execution_journal as ej_db
    from data_sync_service.db import watchlist_automation as wa_db

    def _changes(trade_date, limit):
        if raise_changes:
            raise RuntimeError("changes boom")
        return changes or []

    def _registry():
        if raise_registry:
            raise RuntimeError("registry boom")
        return registry or []

    def _closes(ts_codes, days):
        if raise_closes:
            raise RuntimeError("closes boom")
        return closes or {}

    monkeypatch.setattr(ej_db, "list_changes", _changes)
    monkeypatch.setattr(wa_db, "list_registry", _registry)
    monkeypatch.setattr(pt, "fetch_last_ohlcv_batch", _closes)

    # run_intake resolves fills via paper_entry_fill (next_open + placeholder);
    # stub it so intake tests don't touch the real daily table.
    from data_sync_service.service import paper_entry_fill as fill_mod

    def _fill(ts_code, signal_day, *, signal_close=None):  # noqa: ANN001, ANN003
        return {
            "entry_date": "2026-08-10",
            "entry_price": float(signal_close or 0),
            "pending_open_fill": True,
            "signal_snapshot": {
                "entryMode": "next_open",
                "signalDate": signal_day,
                "pendingOpenFill": True,
            },
        }

    monkeypatch.setattr(fill_mod, "resolve_next_open_fill", _fill)


class TestRunIntake:
    def test_changes_error_recorded(self, monkeypatch) -> None:
        _patch_all(monkeypatch, raise_changes=True)
        out = pt.run_intake(trade_date="2026-08-07")
        assert "error" in out and "changes boom" in out["error"]

    def test_registry_error_tolerated(self, monkeypatch) -> None:
        _patch_all(monkeypatch, raise_registry=True)
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["candidates"] == 0

    def test_skips_non_action_and_blank_symbols(self, monkeypatch) -> None:
        _patch_all(
            monkeypatch,
            changes=[
                None,
                {"field": "positionPct", "newValue": "20", "symbol": "CN:600519"},
                {"field": "action", "newValue": "EXIT", "symbol": "CN:600519"},
                {"field": "action", "newValue": "BUY", "symbol": "  "},
            ],
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["candidates"] == 0

    def test_etf_out_of_scope_skipped(self, monkeypatch) -> None:
        _patch_all(monkeypatch, changes=[ETF_CH])
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 1
        assert out["skippedReasons"]["out-of-scope"] == 1

    def test_held_position_not_candidate(self, monkeypatch) -> None:
        _patch_all(
            monkeypatch,
            changes=[CN_CH],
            registry=[{"symbol": "CN:600519", "positionPct": 20}],
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["candidates"] == 0

    def test_no_close_price_skipped(self, monkeypatch) -> None:
        _patch_all(monkeypatch, changes=[CN_CH])
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 1
        assert out["skippedReasons"]["no-close-price"] == 1

    def test_empty_bars_and_bad_close_guarded(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        insert = Mock(return_value={"id": "x"})
        monkeypatch.setattr(pt_db, "insert_paper_trade", insert)
        _patch_all(
            monkeypatch,
            changes=[CN_CH, ADD_CH],
            closes={
                "600519.SH": [("2026-08-06", 1, 1, 1, 10.0, 100)],
                "00700.HK": [("2026-08-06", 1, 1, 1, "abc", 100)],
            },
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["inserted"] == 1 and out["skipped"] == 1

    def test_closes_fetch_error_skips_all(self, monkeypatch) -> None:
        _patch_all(monkeypatch, changes=[CN_CH], raise_closes=True)
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 1 and out["skippedReasons"]["no-close-price"] == 1

    def test_insert_error_and_duplicate(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        calls = []
        monkeypatch.setattr(
            pt_db,
            "insert_paper_trade",
            lambda **kw: calls.append(kw) or None,
        )
        _patch_all(
            monkeypatch,
            changes=[
                CN_CH,
                {"field": "action", "newValue": "BUY", "symbol": "CN:000001", "source": "alpha"},
            ],
            closes={
                "600519.SH": [("2026-08-06", 1, 1, 1, 10.0, 100)],
                "000001.SZ": [("2026-08-06", 1, 1, 1, 5.0, 100)],
            },
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 2
        assert out["skippedReasons"]["duplicate"] == 2

    def test_insert_error_path(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        def boom(**kw):
            raise RuntimeError("insert boom")

        monkeypatch.setattr(pt_db, "insert_paper_trade", boom)
        _patch_all(
            monkeypatch,
            changes=[CN_CH],
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.0, 100)]},
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 1 and out["skippedReasons"]["insert-error"] == 1

    def test_unresolvable_symbol_out_of_scope(self, monkeypatch) -> None:
        _patch_all(
            monkeypatch,
            changes=[{"field": "action", "newValue": "BUY", "symbol": "BOGUS", "source": "ALPHA"}],
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["skipped"] == 1 and out["skippedReasons"]["out-of-scope"] == 1

    def test_inserted_ok(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        insert = Mock(return_value={"id": "x"})
        monkeypatch.setattr(pt_db, "insert_paper_trade", insert)
        _patch_all(
            monkeypatch,
            changes=[CN_CH],
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.0, 100)]},
        )
        out = pt.run_intake(trade_date="2026-08-07")
        assert out["inserted"] == 1
        kwargs = insert.call_args.kwargs
        assert kwargs["symbol"] == "CN:600519"
        assert kwargs["side"] == "BUY"
        assert kwargs["source"] == "ALPHA"
        assert kwargs["entry_price"] == 10.0


class TestRunUpdate:
    def test_open_trades_error(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db, "get_open_paper_trades", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert "error" in out and "boom" in out["error"]

    def test_no_open_trades(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(pt_db, "get_open_paper_trades", lambda: [])
        out = pt.run_update(today_iso="2026-08-07")
        assert out["scanned"] == 0

    def test_missing_price_skipped(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-01"}],
        )
        _patch_all(monkeypatch)
        out = pt.run_update(today_iso="2026-08-07")
        assert out["scanned"] == 1 and out["updated"] == 0

    def test_closes_error_guarded(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-01"}],
        )
        _patch_all(
            monkeypatch,
            raise_closes=True,
            registry=[{"symbol": "CN:600519", "positionPct": 0}],
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 0

    def test_update_empty_bars_and_bad_close(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [
                {"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"},
                {"id": "t2", "symbol": "CN:000001", "entryPrice": 5.0, "entryDate": "2026-08-06"},
            ],
        )
        upd = Mock()
        monkeypatch.setattr(pt_db, "update_paper_trade_price", upd)
        _patch_all(
            monkeypatch,
            closes={
                "600519.SH": [],
                "000001.SZ": [("2026-08-06", 1, 1, 1, "abc", 100)],
            },
            registry=[{"symbol": "CN:600519", "positionPct": 20}],
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 0

    def test_update_registry_error_fails_open(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
        )
        monkeypatch.setattr(pt_db, "update_paper_trade_price", Mock())
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.5, 100)]},
            raise_registry=True,
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 1

    def test_bad_entry_price_skipped(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": None, "entryDate": "2026-08-01"}],
        )
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.0, 100)]},
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 0

    def test_close_error_guarded(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-01"}],
        )
        monkeypatch.setattr(pt_db, "close_paper_trade", lambda **kw: (_ for _ in ()).throw(RuntimeError("close boom")))
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 9.0, 100)]},
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["closed"] == 0 and out["updated"] == 0

    def test_update_error_guarded(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
        )
        monkeypatch.setattr(pt_db, "update_paper_trade_price", lambda **kw: (_ for _ in ()).throw(RuntimeError("upd boom")))
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.5, 100)]},
            registry=[{"symbol": "CN:600519", "positionPct": 20}],
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 0

    def test_update_ok(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
        )
        upd = Mock()
        monkeypatch.setattr(pt_db, "update_paper_trade_price", upd)
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 10.5, 100)]},
            registry=[{"symbol": "CN:600519", "positionPct": 20}],
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["updated"] == 1
        assert upd.call_args.kwargs["pnl_pct"] == 5.0


class TestHoldingDays:
    def test_bad_dates_zero(self) -> None:
        assert pt._holding_days_for("bad", "2026-08-07") == 0
        assert pt._holding_days_for(None, "2026-08-07") == 0


class TestRowHelpers:
    def test_row_number_snake_fallback_and_bad(self) -> None:
        assert pt._row_number({"entry_price": "7"}, "entryPrice", "entry_price") == 7.0
        assert pt._row_number({}, "entryPrice", "entry_price") is None
        assert pt._row_number({"entryPrice": "abc"}, "entryPrice", "entry_price") is None

    def test_row_str(self) -> None:
        assert pt._row_str({"entry_date": "2026-08-01"}, "entryDate", "entry_date") == "2026-08-01"
        assert pt._row_str({}, "entryDate", "entry_date") is None


class TestPickCloseReason:
    def test_stop_hit(self) -> None:
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519"},
            pnl_pct=-8.0,
            holding_days=1,
            registry_symbols={"CN:600519"},
        )
        assert reason == "stop_hit"

    def test_target_hit(self, monkeypatch) -> None:
        import data_sync_service.db.paper_trading as pt_db

        monkeypatch.setattr(pt_db, "TARGET_PNL_PCT", 10.0)
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519"},
            pnl_pct=15.0,
            holding_days=1,
            registry_symbols={"CN:600519"},
        )
        assert reason == "target_hit"

    def test_score_floor(self, monkeypatch) -> None:
        import data_sync_service.db.paper_trading as pt_db
        from data_sync_service.db import watchlist_automation as wa_db

        monkeypatch.setattr(pt_db, "SCORE_FLOOR", 30.0)
        monkeypatch.setattr(wa_db, "fetch_latest_score_since", lambda symbol, entry_date: 25.0)
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519"},
            pnl_pct=1.0,
            holding_days=1,
            registry_symbols={"CN:600519"},
        )
        assert reason == "score_floor"

    def test_pool_exit(self, monkeypatch) -> None:
        from data_sync_service.db import watchlist_automation as wa_db

        monkeypatch.setattr(wa_db, "fetch_latest_score_since", lambda symbol, entry_date: None)
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519"},
            pnl_pct=1.0,
            holding_days=1,
            registry_symbols={"CN:000001"},
        )
        assert reason == "pool_exit"

    def test_score_fetch_error_fails_open(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db
        from data_sync_service.db import watchlist_automation as wa_db

        monkeypatch.setattr(
            wa_db,
            "fetch_latest_score_since",
            lambda symbol, entry_date: (_ for _ in ()).throw(RuntimeError("score boom")),
        )
        monkeypatch.setattr(pt_db, "MAX_HOLD_DAYS", 5)
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519", "entryDate": "2026-08-01"},
            pnl_pct=-1.0,
            holding_days=6,
            registry_symbols={"CN:600519"},
        )
        assert reason == "max_hold"


class TestComputeStats:
    def test_db_error(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(pt_db, "count_since", lambda since: (_ for _ in ()).throw(RuntimeError("stats boom")))
        out = pt.compute_stats(since_iso="2026-08-01")
        assert "error" in out

    def test_market_bucket_missing_zeroes(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(pt_db, "count_since", lambda since: (10, 5))
        monkeypatch.setattr(pt_db, "avg_pnl_pct_since", lambda since: 2.5)
        monkeypatch.setattr(pt_db, "count_by_market_since", lambda since: {"CN": {"closedCount": 3, "winningCount": 2, "avgPnlPct": 1.5, "winRate": 0.66}})
        out = pt.compute_stats(since_iso="2026-08-01", market="HK")
        assert out["closedCount"] == 0 and out["winningCount"] == 0
        assert out["winRate"] is None and out["avgPnlPct"] is None

    def test_target_hit_in_update(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
        )
        monkeypatch.setattr(pt_db, "TARGET_PNL_PCT", 10.0)
        close = Mock()
        monkeypatch.setattr(pt_db, "close_paper_trade", close)
        _patch_all(
            monkeypatch,
            closes={"600519.SH": [("2026-08-06", 1, 1, 1, 11.5, 100)]},
            registry=[{"symbol": "CN:600519", "positionPct": 20}],
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["closed"] == 1
        assert close.call_args.kwargs["close_reason"] == "target_hit"

    def test_market_bucket_present(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(pt_db, "count_since", lambda since: (10, 5))
        monkeypatch.setattr(pt_db, "avg_pnl_pct_since", lambda since: 2.5)
        monkeypatch.setattr(pt_db, "count_by_market_since", lambda since: {"CN": {"closedCount": 3, "winningCount": 2, "avgPnlPct": 1.5, "winRate": 0.66}})
        out = pt.compute_stats(since_iso="2026-08-01", market="CN")
        assert out["closedCount"] == 3 and out["winningCount"] == 2


def test_trailing_stop_closes_on_peak_pullback(monkeypatch) -> None:
    """S-3 trailing stop: close when close pulls back >= 8% from the
    post-entry CLOSE peak (backtest-strategy.md 6.6; 2026-08-12 C4
    alignment — close-based peak, same as the backtest engine)."""
    from data_sync_service.db import paper_trading as pt_db

    monkeypatch.setattr(
        pt_db,
        "get_open_paper_trades",
        lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
    )
    close = Mock()
    monkeypatch.setattr(pt_db, "close_paper_trade", close)
    _patch_all(
        monkeypatch,
        closes={"600519.SH": [
            ("2026-08-06", "10.0", "10.5", "9.8", "10.4", "100"),  # close peak 10.4 (high 10.5 ignored)
            ("2026-08-07", "9.6", "9.7", "9.4", "9.55", "100"),    # -8.2% from 10.4; net -4.5% → trailing, not stop
        ]},
        registry=[{"symbol": "CN:600519", "positionPct": 20}],
    )
    out = pt.run_update(today_iso="2026-08-07")
    assert out["closed"] == 1
    assert close.call_args.kwargs["close_reason"] == "trailing_stop"


def test_trailing_stop_holds_below_threshold(monkeypatch) -> None:
    """Pullback of 5% from peak does not trigger the 8% trailing stop."""
    from data_sync_service.db import paper_trading as pt_db

    monkeypatch.setattr(
        pt_db,
        "get_open_paper_trades",
        lambda: [{"id": "t1", "symbol": "CN:600519", "entryPrice": 10.0, "entryDate": "2026-08-06"}],
    )
    close = Mock()
    monkeypatch.setattr(pt_db, "close_paper_trade", close)
    _patch_all(
        monkeypatch,
        closes={"600519.SH": [
            ("2026-08-06", "10.0", "10.5", "9.8", "10.0", "100"),
            ("2026-08-07", "9.98", "10.0", "9.9", "9.98", "100"),  # -5.0% from peak
        ]},
        registry=[{"symbol": "CN:600519", "positionPct": 20}],
    )
    out = pt.run_update(today_iso="2026-08-07")
    assert out["closed"] == 0
    close.assert_not_called()


class TestS3PaperProtections:
    """2026-08-11: S-3 paper lines are managed by the S-3 rule set — the
    v0 registry pool_exit must NOT apply to them, and the trailing peak
    must be measured from ENTRY onwards (not the pre-entry history high)."""

    def test_s3_trade_excluded_from_pool_exit(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{
                "id": "s3-1", "symbol": "HK:00178", "entryPrice": 1.0,
                "entryDate": "2026-08-10", "source": "S3HK", "market": "HK",
            }],
        )
        upd = Mock()
        close = Mock()
        monkeypatch.setattr(pt_db, "update_paper_trade_price", upd)
        monkeypatch.setattr(pt_db, "close_paper_trade", close)
        # Empty registry: a v0 manual trade would pool_exit here, the S-3
        # paper line must NOT.
        _patch_all(monkeypatch, registry=[])
        closes = {
            "00178.HK": [("2026-08-08", 0.95, 0.95, 0.95, 1.0, 100), ("2026-08-10", 1.0, 1.0, 1.0, 1.05, 100)],
        }
        monkeypatch.setattr(pt, "fetch_last_ohlcv_batch", lambda codes, days: closes)
        out = pt.run_update(today_iso="2026-08-10")
        assert out["closed"] == 0
        assert out["updated"] == 1
        assert close.call_count == 0

    def test_v0_manual_trade_still_pool_exits(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{
                "id": "m1", "symbol": "CN:600519", "entryPrice": 10.0,
                "entryDate": "2026-08-01", "source": "MANUAL", "market": "CN",
            }],
        )
        close = Mock()
        monkeypatch.setattr(pt_db, "close_paper_trade", close)
        _patch_all(monkeypatch, registry=[])
        monkeypatch.setattr(
            pt, "fetch_last_ohlcv_batch",
            lambda codes, days: {"600519.SH": [("2026-08-07", 10.0, 10.0, 10.0, 10.0, 100)]},
        )
        out = pt.run_update(today_iso="2026-08-07")
        assert out["closed"] == 1
        assert close.call_count == 1

    def test_trailing_peak_measured_from_entry(self, monkeypatch) -> None:
        from data_sync_service.db import paper_trading as pt_db

        # History high 15.0 BEFORE entry must not set the trailing peak;
        # post-entry high is 11.0, current close 10.2 → -7.3% → NO trail.
        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{
                "id": "t1", "symbol": "CN:600519", "entryPrice": 10.0,
                "entryDate": "2026-08-05", "source": "S3", "market": "CN",
            }],
        )
        upd = Mock()
        close = Mock()
        monkeypatch.setattr(pt_db, "update_paper_trade_price", upd)
        monkeypatch.setattr(pt_db, "close_paper_trade", close)
        _patch_all(monkeypatch, registry=[])
        closes = {
            "600519.SH": [
                ("2026-08-01", 14.0, 15.0, 14.0, 14.0, 100),   # pre-entry peak
                ("2026-08-05", 10.0, 10.5, 10.0, 10.0, 100),   # entry day
                ("2026-08-06", 10.1, 11.0, 10.0, 10.1, 100),   # post-entry high 11.0
                ("2026-08-07", 10.2, 10.3, 10.0, 10.2, 100),   # -7.3% vs 11.0 → hold
            ],
        }
        monkeypatch.setattr(pt, "fetch_last_ohlcv_batch", lambda codes, days: closes)
        out = pt.run_update(today_iso="2026-08-07")
        assert out["closed"] == 0
        assert out["updated"] == 1
        # If the peak were the pre-entry 15.0: (10.2-15)/15 = -32% → would trail.


class TestS3StrongATRStop:
    """OPT-105: CN S-3 paper exits switch to the entry-locked ATR line while
    the regime is Strong; Diverging/Weak keep the fixed -5/-8 constants."""

    def _run(self, monkeypatch, *, regime: str | None, close_px: float) -> dict:
        from data_sync_service.db import paper_trading as pt_db

        # 8 pre-entry bars with TR ~0.5 → ATR ≈ 0.5 → atr_pct 5% → ATR stop -10%
        pre = []
        for i in range(8):
            day = f"2026-07-{20 + i:02d}"
            pre.append((day, 10.0, 10.2, 9.7, 10.0, 100))
        bars = pre + [
            ("2026-08-05", 10.0, 10.1, 9.9, 10.0, 100),  # entry
            ("2026-08-06", 9.5, 9.6, 9.3, close_px, 100),  # -6% vs entry
        ]
        monkeypatch.setattr(
            pt_db,
            "get_open_paper_trades",
            lambda: [{
                "id": "t1", "symbol": "CN:600519", "entryPrice": 10.0,
                "entryDate": "2026-08-05", "source": "S3", "market": "CN",
            }],
        )
        upd = Mock()
        close = Mock()
        monkeypatch.setattr(pt_db, "update_paper_trade_price", upd)
        monkeypatch.setattr(pt_db, "close_paper_trade", close)
        monkeypatch.setattr(pt, "_cn_regime_today", lambda: regime)
        _patch_all(monkeypatch, registry=[])
        closes = {"600519.SH": bars}
        monkeypatch.setattr(pt, "fetch_last_ohlcv_batch", lambda codes, days: closes)
        return pt.run_update(today_iso="2026-08-06")

    def test_strong_regime_uses_atr_line_holds(self, monkeypatch) -> None:
        # -6%: the FIXED -5% stop would exit; the ATR line (-10%) holds.
        out = self._run(monkeypatch, regime="Strong", close_px=9.4)
        assert out["closed"] == 0
        assert out["updated"] == 1

    def test_weak_regime_uses_fixed_line_exits(self, monkeypatch) -> None:
        # Same -6% drawdown under Weak → fixed -5% stop fires.
        out = self._run(monkeypatch, regime="Weak", close_px=9.4)
        assert out["closed"] == 1

    def test_regime_unavailable_falls_back_to_fixed(self, monkeypatch) -> None:
        # Regime lookup failure must never loosen the stop (fail-closed).
        out = self._run(monkeypatch, regime=None, close_px=9.4)
        assert out["closed"] == 1
