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

    def test_target_hit(self) -> None:
        reason = pt._pick_close_reason(
            t={"symbol": "CN:600519"},
            pnl_pct=15.0,
            holding_days=1,
            registry_symbols={"CN:600519"},
        )
        assert reason == "target_hit"

    def test_score_floor(self, monkeypatch) -> None:
        from data_sync_service.db import watchlist_automation as wa_db

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
