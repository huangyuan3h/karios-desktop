"""OPT-049 tests: paper_trades db CRUD + service intake/update/stats + /v1 API.

DB-touching tests are marked ``@pytest.mark.requires_postgres`` and will be
skipped when no Postgres is reachable. Service-level tests mock the DB layer
and the upstream journal / daily table so they run everywhere.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    from data_sync_service import config

    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fixtures: pure-data dicts returned by the mocked DB layer.
# ---------------------------------------------------------------------------


_OPEN_ROW = {
    "id": "row-1",
    "symbol": "CN:000001",
    "entryDate": "2026-08-01",
    "side": "BUY",
    "entryPrice": 12.0,
    "scoreAtEntry": 88.0,
    "whyAtEntry": "MAINLINE_OK",
    "sleevePct": 5.0,
    "status": "open",
    "closeDate": None,
    "closePrice": None,
    "pnlPct": None,
    "grossPnlPct": None,
    "costsPct": None,
    "holdingDays": None,
    "closeReason": None,
    "source": "TV",
    "market": "CN",
    "createdAt": "2026-08-01T09:40:00+00:00",
    "updatedAt": "2026-08-01T09:40:00+00:00",
}

_CLOSED_ROW = {
    **_OPEN_ROW,
    "id": "row-2",
    "status": "closed",
    "closeDate": "2026-08-04",
    "closePrice": 12.6,
    "pnlPct": 4.7,
    "grossPnlPct": 5.0,
    "costsPct": 0.3,
    "holdingDays": 3,
    "closeReason": "max_hold",
}

_HK_ROW = {
    **_OPEN_ROW,
    "id": "row-3",
    "symbol": "HK:00700",
    "entryDate": "2026-08-02",
    "entryPrice": 480.0,
    "market": "HK",
}


# ---------------------------------------------------------------------------
# /v1/paper-trades list
# ---------------------------------------------------------------------------


def test_paper_trades_list_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        return_value=[_OPEN_ROW, _CLOSED_ROW, _HK_ROW],
    ):
        body = client.get("/v1/paper-trades").json()
    assert set(body.keys()) == {"asOfDate", "count", "items"}
    assert body["count"] == 3
    assert len(body["items"]) == 3
    first = body["items"][0]
    assert set(first.keys()) >= {
        "id",
        "symbol",
        "entryDate",
        "side",
        "entryPrice",
        "scoreAtEntry",
        "whyAtEntry",
        "status",
        "closeDate",
        "closePrice",
        "pnlPct",
        "grossPnlPct",
        "costsPct",
        "holdingDays",
        "closeReason",
        "market",
    }
    assert first["market"] == "CN"


def test_paper_trades_list_includes_market_field() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        return_value=[_HK_ROW],
    ):
        body = client.get("/v1/paper-trades").json()
    assert body["items"][0]["market"] == "HK"


def test_paper_trades_list_passes_filters() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        return_value=[_OPEN_ROW],
    ) as mock:
        resp = client.get("/v1/paper-trades?status=open&since=2026-08-01&limit=10&market=CN")
    assert resp.status_code == 200
    assert mock.call_args.kwargs == {
        "status": "open",
        "since": "2026-08-01",
        "limit": 10,
        "market": "CN",
    }


def test_paper_trades_list_rejects_bad_status() -> None:
    resp = client.get("/v1/paper-trades?status=banana")
    assert resp.status_code == 422


def test_paper_trades_list_rejects_bad_market() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        side_effect=ValueError("market must be one of ('CN', 'HK')"),
    ):
        resp = client.get("/v1/paper-trades?market=US")
    assert resp.status_code == 422


def test_paper_trades_list_caps_limit() -> None:
    resp = client.get("/v1/paper-trades?limit=9999")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /v1/paper-trades/stats
# ---------------------------------------------------------------------------


def test_paper_trades_stats_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_compute_stats",
        return_value={
            "since": "2026-08-01",
            "closedCount": 10,
            "winningCount": 7,
            "winRate": 0.7,
            "avgPnlPct": 1.23,
            "byMarket": {
                "CN": {"closedCount": 8, "winningCount": 6, "winRate": 0.75, "avgPnlPct": 1.5},
                "HK": {"closedCount": 2, "winningCount": 1, "winRate": 0.5, "avgPnlPct": 0.1},
            },
        },
    ):
        body = client.get("/v1/paper-trades/stats?since=2026-08-01").json()
    assert set(body.keys()) == {
        "since",
        "closedCount",
        "winningCount",
        "winRate",
        "avgPnlPct",
        "byMarket",
    }
    assert body["closedCount"] == 10
    assert body["winRate"] == 0.7
    assert body["byMarket"]["HK"]["winRate"] == 0.5


def test_paper_trades_stats_passes_market_filter() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_compute_stats",
        return_value={
            "since": "2026-08-01",
            "closedCount": 2,
            "winningCount": 1,
            "winRate": 0.5,
            "avgPnlPct": 0.1,
            "byMarket": {
                "HK": {"closedCount": 2, "winningCount": 1, "winRate": 0.5, "avgPnlPct": 0.1},
            },
        },
    ) as mock:
        body = client.get("/v1/paper-trades/stats?since=2026-08-01&market=HK").json()
    assert mock.call_args.kwargs == {"since_iso": "2026-08-01", "market": "HK"}
    assert body["closedCount"] == 2
    assert body["byMarket"]["HK"]["winRate"] == 0.5


def test_paper_trades_stats_handles_empty_window() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_compute_stats",
        return_value={
            "since": "2026-08-01",
            "closedCount": 0,
            "winningCount": 0,
            "winRate": None,
            "avgPnlPct": None,
            "byMarket": {},
        },
    ):
        body = client.get("/v1/paper-trades/stats?since=2026-08-01").json()
    assert body["winRate"] is None
    assert body["avgPnlPct"] is None
    assert body["byMarket"] == {}


def test_paper_trades_stats_requires_since() -> None:
    resp = client.get("/v1/paper-trades/stats")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Service: run_intake idempotency + filters
# ---------------------------------------------------------------------------


def test_run_intake_filters_out_of_scope_symbols() -> None:
    """An ETF decision journal change must be skipped, not inserted."""
    fake_journal = [
        {"symbol": "HK:00700", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
        {"symbol": "ETF:510300", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"00700.HK": [("2026-08-01", 480.0, 481.0, 479.0, 480.0, 10000)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    # ETF is out of scope (skipped with 'out-of-scope'); HK is now accepted
    # (v0.2) and inserted with market='HK'.
    assert summary["candidates"] == 1
    assert summary["inserted"] == 1
    assert summary["skipped"] == 1
    assert summary["skippedReasons"].get("out-of-scope") == 1
    mock_insert.assert_called_once()
    assert mock_insert.call_args.kwargs["market"] == "HK"


def test_run_intake_skips_already_positioned_symbols() -> None:
    """If the user has already taken a real position, we don't paper-trade
    on top of it — that would double-count."""
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
    ]
    fake_registry = [
        {"symbol": "CN:000001", "positionPct": 8.5},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=fake_registry,
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["candidates"] == 0
    mock_insert.assert_not_called()


def test_run_intake_inserts_only_unfollowed() -> None:
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
        {"symbol": "CN:600519", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 70, "sleevePct": 5.0},
    ]
    fake_registry = [
        {"symbol": "CN:000001", "positionPct": 8.5},  # already followed
        {"symbol": "CN:600519", "positionPct": None},  # NOT followed
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=fake_registry,
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={
                "000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)],
                "600519.SH": [("2026-08-01", 1700.0, 1700.0, 1699.0, 1700.0, 100)],
            },
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["candidates"] == 1  # only 600519 is unfollowed
    assert summary["inserted"] == 1
    # Insert was called with 600519, not 000001.
    assert mock_insert.call_args.kwargs["symbol"] == "CN:600519"
    assert mock_insert.call_args.kwargs["entry_price"] == 1700.0


def test_run_intake_treats_idempotent_insert_as_skip() -> None:
    """If pt_db.insert_paper_trade returns None, the (symbol, date, side)
    row already exists — count it as skipped, not inserted."""
    fake_journal = [
        {"symbol": "CN:600519", "field": "action", "newValue": "BUY", "why": "MAINLINE_OK", "score": 70, "sleevePct": 5.0},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"600519.SH": [("2026-08-01", 1700.0, 1700.0, 1699.0, 1700.0, 100)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value=None,  # already exists
        ),
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["inserted"] == 0
    assert summary["skipped"] == 1
    assert summary["skippedReasons"].get("duplicate") == 1


# ---------------------------------------------------------------------------
# Service: run_update close conditions
# ---------------------------------------------------------------------------


def _patched_run_update(open_rows, bars_by_ts, today_iso="2026-08-06", registry=None, score=99.0):
    if registry is None:
        registry = [{"symbol": t.get("symbol") or ""} for t in open_rows]
    return (
        patch("data_sync_service.service.paper_trading.pt_db.get_open_paper_trades", return_value=open_rows),
        patch("data_sync_service.service.paper_trading.fetch_last_ohlcv_batch", return_value=bars_by_ts),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=registry,
        ),
        patch(
            "data_sync_service.service.paper_trading.wa_db.fetch_latest_score_since",
            return_value=score,
        ),
    )


def test_run_update_closes_on_stop_loss() -> None:
    """pnl_pct <= -5% must close with reason 'stop_hit'."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 9.4, 9.4, 9.3, 9.4, 1000)]}  # -6% drawdown
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "stop_hit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("stop_hit") == 1
    assert mock_close.call_args.kwargs["close_reason"] == "stop_hit"
    mock_update.assert_not_called()


def test_run_update_closes_on_max_hold(monkeypatch) -> None:
    """holding_days >= MAX_HOLD_DAYS must close with reason 'max_hold'."""
    import data_sync_service.db.paper_trading as pt_db_mod

    monkeypatch.setattr(pt_db_mod, "MAX_HOLD_DAYS", 5)
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 10.3, 10.3, 10.2, 10.3, 1000)]}  # +3% (no stop)
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "max_hold"},
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        # today is 5 days after entry → triggers max_hold.
        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("max_hold") == 1
    mock_update.assert_not_called()


def test_run_update_updates_without_closing_when_within_bounds() -> None:
    """Day 2 with -2% drawdown: just update, no close."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 9.8, 9.8, 9.7, 9.8, 1000)]}
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
        return_value={**_OPEN_ROW, "pnl_pct": -2.0, "holding_days": 2},
    ) as mock_update, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["updated"] == 1
    assert summary["closed"] == 0
    mock_close.assert_not_called()
    # holding_days = 2, pnl_pct ≈ -2.0
    assert mock_update.call_args.kwargs["holding_days"] == 2
    assert abs(mock_update.call_args.kwargs["pnl_pct"] + 2.0) < 0.01


def test_run_update_skips_symbols_without_fresh_close() -> None:
    """If the daily table hasn't closed yet, skip without erroring."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars: dict[str, list] = {}  # nothing
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["scanned"] == 1
    assert summary["updated"] == 0
    assert summary["closed"] == 0
    mock_update.assert_not_called()
    mock_close.assert_not_called()


# ---------------------------------------------------------------------------
# Service: run_update v0.1 close conditions (target_hit / score_floor / pool_exit)
# ---------------------------------------------------------------------------


def test_run_update_closes_on_target_hit_even_with_max_hold(monkeypatch) -> None:
    """pnl_pct >= TARGET_PNL_PCT must close with 'target_hit' — and beat 'max_hold'."""
    import data_sync_service.db.paper_trading as pt_db_mod

    monkeypatch.setattr(pt_db_mod, "TARGET_PNL_PCT", 10.0)
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 11.2, 11.2, 11.1, 11.2, 1000)]}  # +12%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "target_hit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        # holding_days would be 5 (max_hold) but target beats it.
        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("target_hit") == 1
    assert mock_close.call_args.kwargs["close_reason"] == "target_hit"
    mock_update.assert_not_called()


def test_run_update_stop_beats_target_on_high_volatility() -> None:
    """A -6% day must close as 'stop_hit', never 'target_hit'."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 9.4, 9.4, 9.3, 9.4, 1000)]}  # -6%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "stop_hit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closeReasons"].get("stop_hit") == 1
    assert mock_close.call_args.kwargs["close_reason"] == "stop_hit"
    mock_update.assert_not_called()


# ---------------------------------------------------------------------------
# Service: run_update v0.2 net-of-costs close semantics (OPT-062)
# ---------------------------------------------------------------------------


def test_run_update_stop_triggers_on_net_not_gross() -> None:
    """Gross -4.8% does NOT stop by itself, but net (-5.1% after CN 0.3%
    round-trip cost) DOES — stop/target conditions are net-of-costs (v0.2)."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 9.52, 9.52, 9.5, 9.52, 1000)]}  # gross -4.8%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "stop_hit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("stop_hit") == 1
    assert abs(mock_close.call_args.kwargs["pnl_pct"] - (-5.1)) < 0.05  # net
    assert abs(mock_close.call_args.kwargs["gross_pnl_pct"] - (-4.8)) < 0.05
    assert abs(mock_close.call_args.kwargs["costs_pct"] - 0.3) < 0.01
    mock_update.assert_not_called()


def test_run_update_gross_within_bounds_net_within_bounds_stays_open() -> None:
    """Gross +0.5% / net +0.2% (CN) — no close, and open rows keep the
    GROSS pnl (costs land once at close time)."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 10.05, 10.05, 10.0, 10.05, 1000)]}  # +0.5%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
        return_value={**_OPEN_ROW, "pnl_pct": 0.5, "holding_days": 2},
    ) as mock_update, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["updated"] == 1
    assert summary["closed"] == 0
    mock_close.assert_not_called()
    # Open rows show GROSS pnl until closed.
    assert abs(mock_update.call_args.kwargs["pnl_pct"] - 0.5) < 0.01


def test_run_update_hk_trade_uses_hk_cost_model() -> None:
    """A HK open trade resolves via its own market: cost model 0.6% round
    trip, close written with net/gross/costs split, score_floor fails open."""
    open_rows = [{**_HK_ROW, "entry_price": 480.0, "entryDate": "2026-08-04"}]
    bars = {"00700.HK": [("2026-08-06", 484.8, 485.0, 484.0, 484.8, 5000)]}  # +1%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars, score=None)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
        return_value={**_HK_ROW, "pnl_pct": 1.0, "holding_days": 2},
    ) as mock_update, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["updated"] == 1
    assert summary["closed"] == 0
    assert abs(mock_update.call_args.kwargs["pnl_pct"] - 1.0) < 0.01  # gross on open
    mock_close.assert_not_called()


def test_run_update_hk_trade_closes_with_hk_costs() -> None:
    """HK +0.5% gross → net -0.1% (0.6% round trip) — but a later -7% day
    closes as stop_hit with HK cost split written to the row."""
    open_rows = [{**_HK_ROW, "entry_price": 480.0, "entryDate": "2026-08-04"}]
    bars = {"00700.HK": [("2026-08-06", 446.4, 447.0, 445.0, 446.4, 5000)]}  # -7%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars, score=None)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_HK_ROW, "status": "closed", "close_reason": "stop_hit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("stop_hit") == 1
    assert abs(mock_close.call_args.kwargs["gross_pnl_pct"] - (-7.0)) < 0.05
    assert abs(mock_close.call_args.kwargs["costs_pct"] - 0.6) < 0.01
    assert abs(mock_close.call_args.kwargs["pnl_pct"] - (-7.6)) < 0.05
    mock_update.assert_not_called()


def test_run_update_closes_on_score_floor(monkeypatch) -> None:
    """Latest TrendOK score < SCORE_FLOOR must close with 'score_floor'."""
    import data_sync_service.db.paper_trading as pt_db_mod

    monkeypatch.setattr(pt_db_mod, "SCORE_FLOOR", 30.0)
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 10.1, 10.1, 10.0, 10.1, 1000)]}  # +1%
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars, score=18.0)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "score_floor"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("score_floor") == 1
    assert mock_close.call_args.kwargs["close_reason"] == "score_floor"
    mock_update.assert_not_called()


def test_run_update_score_floor_fails_open_without_score_data() -> None:
    """Missing score data never closes a trade (fail-open)."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 10.1, 10.1, 10.0, 10.1, 1000)]}
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars, score=None)
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
        return_value={**_OPEN_ROW, "pnl_pct": 1.0, "holding_days": 2},
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["updated"] == 1
    assert summary["closed"] == 0
    mock_close.assert_not_called()


def test_run_update_closes_on_pool_exit() -> None:
    """Symbol purged from the watchlist registry must close with 'pool_exit'."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 10.1, 10.1, 10.0, 10.1, 1000)]}
    # Registry contains the symbol → not a pool exit... then without it → exit.
    p1, p2, p3, p4 = _patched_run_update(open_rows, bars, registry=[{"symbol": "CN:999999"}])
    with p1, p2, p3, p4, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "pool_exit"},
    ) as mock_close, patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price"
    ) as mock_update:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["closed"] == 1
    assert summary["closeReasons"].get("pool_exit") == 1
    assert mock_close.call_args.kwargs["close_reason"] == "pool_exit"
    mock_update.assert_not_called()


def test_run_update_pool_exit_fails_open_when_registry_unreadable() -> None:
    """A registry read failure must never close a trade (fail-open)."""
    open_rows = [{**_OPEN_ROW, "entryPrice": 10.0, "entryDate": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 10.1, 10.1, 10.0, 10.1, 1000)]}
    p1, p2 = _patched_run_update(open_rows, bars)[:2]
    with p1, p2, patch(
        "data_sync_service.db.watchlist_automation.list_registry",
        side_effect=RuntimeError("db down"),
    ), patch(
        "data_sync_service.service.paper_trading.wa_db.fetch_latest_score_since",
        return_value=90.0,
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
        return_value={**_OPEN_ROW, "pnl_pct": 1.0, "holding_days": 2},
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade"
    ) as mock_close:
        from data_sync_service.service.paper_trading import run_update

        summary = run_update(today_iso="2026-08-06")
    assert summary["updated"] == 1
    assert summary["closed"] == 0
    mock_close.assert_not_called()


# ---------------------------------------------------------------------------
# Service: compute_stats passthrough
# ---------------------------------------------------------------------------


def test_compute_stats_passes_since() -> None:
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(10, 7),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=1.23,
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.count_by_market_since",
        return_value={
            "CN": {"closedCount": 8, "winningCount": 6, "winRate": 0.75, "avgPnlPct": 1.5},
            "HK": {"closedCount": 2, "winningCount": 1, "winRate": 0.5, "avgPnlPct": 0.1},
        },
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01")
    assert out["closedCount"] == 10
    assert out["winningCount"] == 7
    assert out["winRate"] == 0.7
    assert out["avgPnlPct"] == 1.23
    assert out["byMarket"]["HK"]["winRate"] == 0.5


def test_compute_stats_narrows_to_market() -> None:
    by_market = {
        "CN": {"closedCount": 8, "winningCount": 6, "winRate": 0.75, "avgPnlPct": 1.5},
        "HK": {"closedCount": 2, "winningCount": 1, "winRate": 0.5, "avgPnlPct": 0.1},
    }
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(10, 7),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=1.23,
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.count_by_market_since",
        return_value=by_market,
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01", market="HK")
    assert out["closedCount"] == 2
    assert out["winningCount"] == 1
    assert out["winRate"] == 0.5
    assert out["avgPnlPct"] == 0.1
    # Headline narrowing never mutates the full byMarket breakdown.
    assert out["byMarket"] == by_market


def test_compute_stats_market_with_no_trades_is_empty() -> None:
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(10, 7),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=1.23,
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.count_by_market_since",
        return_value={"CN": {"closedCount": 10, "winningCount": 7, "winRate": 0.7, "avgPnlPct": 1.23}},
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01", market="HK")
    assert out["closedCount"] == 0
    assert out["winRate"] is None
    assert out["avgPnlPct"] is None


def test_compute_stats_handles_empty() -> None:
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(0, 0),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=None,
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.count_by_market_since",
        return_value={},
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01")
    assert out["winRate"] is None
    assert out["avgPnlPct"] is None
    assert out["byMarket"] == {}


# ---------------------------------------------------------------------------
# DB layer: helper functions tested directly
# ---------------------------------------------------------------------------


def test_holding_days_for_calendar_diff() -> None:
    from data_sync_service.service.paper_trading import _holding_days_for

    assert _holding_days_for("2026-08-01", "2026-08-06") == 5
    assert _holding_days_for("2026-08-04", "2026-08-06") == 2
    assert _holding_days_for("2026-08-06", "2026-08-06") == 0
    # Bad inputs are clamped to 0 instead of raising.
    assert _holding_days_for("not-a-date", "2026-08-06") == 0


def test_resolve_ts_code_accepts_cn_and_hk() -> None:
    from data_sync_service.service.paper_trading import _resolve_ts_code

    assert _resolve_ts_code("CN:000001") == ("CN", "000001.SZ")
    assert _resolve_ts_code("CN:600519") == ("CN", "600519.SH")
    assert _resolve_ts_code("HK:00700") == ("HK", "00700.HK")
    assert _resolve_ts_code("HK:0001") == ("HK", "00001.HK")
    # ETF / unknown stay out of scope (v0.2).
    assert _resolve_ts_code("ETF:510300") is None
    assert _resolve_ts_code("unknown") is None


def test_run_intake_insert_side_not_leaked_from_last_change() -> None:
    """Regression (H2 smoke 2026-08-08): `action` is function-scoped in the
    filter loop; a trailing WATCH/TRIM change used to leak into every insert
    (`side=action` in the insert loop), failing or mislabelling all inserts."""
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY", "source": "TV"},
        {"symbol": "CN:000002", "field": "action", "newValue": "WATCH_SILENT", "source": "TV"},
    ]
    inserted: list[dict] = []

    def fake_insert(**kwargs):
        inserted.append(kwargs)
        return {"id": "x"}

    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            side_effect=fake_insert,
        ),
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["inserted"] == 1
    assert inserted and inserted[0]["side"] == "BUY"


def test_holding_days_calendar_semantics() -> None:
    """H6: _holding_days_for counts CALENDAR days (documented v0 trade-off;
    a weekend is 2-3 days, same-week same-day is 0)."""
    from data_sync_service.service.paper_trading import _holding_days_for

    # Same day → 0
    assert _holding_days_for("2026-08-03", "2026-08-03") == 0
    # Next calendar day → 1
    assert _holding_days_for("2026-08-03", "2026-08-04") == 1
    # Cross-weekend: Friday → Monday = 3 calendar days
    assert _holding_days_for("2026-08-07", "2026-08-10") == 3
    # Cross-month: 07-31 → 08-03 = 3
    assert _holding_days_for("2026-07-31", "2026-08-03") == 3
    # Invalid dates → 0 (no crash)
    assert _holding_days_for("", "2026-08-03") == 0
    assert _holding_days_for("2026-08-03", "garbage") == 0
    assert _holding_days_for(None, "2026-08-03") == 0
    # Reverse order → clamped to 0
    assert _holding_days_for("2026-08-10", "2026-08-03") == 0


# ---------------------------------------------------------------------------
# Failure / resilience paths (coverage wave 1)
# ---------------------------------------------------------------------------


def test_run_intake_handles_list_changes_failure() -> None:
    from data_sync_service.service.paper_trading import run_intake

    with patch(
        "data_sync_service.service.paper_trading.ej_db.list_changes",
        side_effect=RuntimeError("db down"),
    ):
        summary = run_intake(trade_date="2026-08-01")
    assert summary["error"] == "ej_db.list_changes failed: db down"
    assert summary["candidates"] == 0


def test_run_intake_handles_registry_failure() -> None:
    """Registry read failure must not abort intake (fail open, no positions known)."""
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY"},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            side_effect=RuntimeError("db down"),
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["inserted"] == 1
    assert "error" not in summary
    mock_insert.assert_called_once()


def test_run_intake_skips_candidates_without_close_price() -> None:
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY"},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={},  # no bars at all → no close price
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["candidates"] == 1
    assert summary["inserted"] == 0
    assert summary["skippedReasons"].get("no-close-price") == 1
    mock_insert.assert_not_called()


def test_run_intake_counts_insert_errors_as_skipped() -> None:
    fake_journal = [
        {"symbol": "CN:000001", "field": "action", "newValue": "BUY"},
    ]
    with (
        patch(
            "data_sync_service.service.paper_trading.ej_db.list_changes",
            return_value=fake_journal,
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)]},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            side_effect=RuntimeError("constraint"),
        ),
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    assert summary["skipped"] == 1
    assert summary["skippedReasons"].get("insert-error") == 1


def test_run_update_handles_open_trades_failure() -> None:
    from data_sync_service.service.paper_trading import run_update

    with patch(
        "data_sync_service.service.paper_trading.pt_db.get_open_paper_trades",
        side_effect=RuntimeError("db down"),
    ):
        summary = run_update()
    assert summary["error"] == "get_open_paper_trades failed: db down"
    assert summary["updated"] == 0


def test_run_update_registry_failure_never_pool_exits() -> None:
    """Fail-open: when the registry read fails, pool_exit must not close anything."""
    from data_sync_service.service.paper_trading import run_update

    open_row = {**_OPEN_ROW, "closeReason": None, "entryDate": "2026-08-08"}
    with (
        patch(
            "data_sync_service.service.paper_trading.pt_db.get_open_paper_trades",
            return_value=[open_row],
        ),
        patch(
            "data_sync_service.service.paper_trading.fetch_last_ohlcv_batch",
            return_value={"000001.SZ": [("2026-08-01", 12.0, 12.0, 11.9, 12.0, 1000)]},
        ),
        patch(
            "data_sync_service.db.watchlist_automation.list_registry",
            side_effect=RuntimeError("db down"),
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
            return_value={"id": "row-1"},
        ) as mock_close,
        patch(
            "data_sync_service.service.paper_trading.pt_db.update_paper_trade_price",
            return_value=None,
        ),
    ):
        summary = run_update(today_iso="2026-08-08")
    # symbol not in registry (None) → must not hit the pool_exit branch
    assert summary["updated"] == 1
    mock_close.assert_not_called()


def test_s3_close_thresholds_pinned() -> None:
    """S-3 backtest params are the live defaults (evidence: backtest-strategy.md).

    Any change here must first be re-validated on the validation window.
    """
    import data_sync_service.db.paper_trading as pt_db_mod

    assert pt_db_mod.MAX_HOLD_DAYS == 60
    assert pt_db_mod.TARGET_PNL_PCT == 100.0
    assert pt_db_mod.SCORE_FLOOR == 0.0
    assert pt_db_mod.STOP_LOSS_PCT == -5.0
