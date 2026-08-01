"""OPT-049 tests: paper_trades db CRUD + service intake/update/stats + /v1 API.

DB-touching tests are marked ``@pytest.mark.requires_postgres`` and will be
skipped when no Postgres is reachable. Service-level tests mock the DB layer
and the upstream journal / daily table so they run everywhere.
"""

from __future__ import annotations

from typing import Any
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
    "holdingDays": None,
    "closeReason": None,
    "createdAt": "2026-08-01T09:40:00+00:00",
    "updatedAt": "2026-08-01T09:40:00+00:00",
}

_CLOSED_ROW = {
    **_OPEN_ROW,
    "id": "row-2",
    "status": "closed",
    "closeDate": "2026-08-04",
    "closePrice": 12.6,
    "pnlPct": 5.0,
    "holdingDays": 3,
    "closeReason": "max_hold",
}


# ---------------------------------------------------------------------------
# /v1/paper-trades list
# ---------------------------------------------------------------------------


def test_paper_trades_list_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        return_value=[_OPEN_ROW, _CLOSED_ROW],
    ):
        body = client.get("/v1/paper-trades").json()
    assert set(body.keys()) == {"asOfDate", "count", "items"}
    assert body["count"] == 2
    assert len(body["items"]) == 2
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
        "holdingDays",
        "closeReason",
    }


def test_paper_trades_list_passes_filters() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_db.list_paper_trades",
        return_value=[_OPEN_ROW],
    ) as mock:
        resp = client.get("/v1/paper-trades?status=open&since=2026-08-01&limit=10")
    assert resp.status_code == 200
    assert mock.call_args.kwargs == {"status": "open", "since": "2026-08-01", "limit": 10}


def test_paper_trades_list_rejects_bad_status() -> None:
    resp = client.get("/v1/paper-trades?status=banana")
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
        },
    ):
        body = client.get("/v1/paper-trades/stats?since=2026-08-01").json()
    assert set(body.keys()) == {"since", "closedCount", "winningCount", "winRate", "avgPnlPct"}
    assert body["closedCount"] == 10
    assert body["winRate"] == 0.7


def test_paper_trades_stats_handles_empty_window() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.pt_compute_stats",
        return_value={
            "since": "2026-08-01",
            "closedCount": 0,
            "winningCount": 0,
            "winRate": None,
            "avgPnlPct": None,
        },
    ):
        body = client.get("/v1/paper-trades/stats?since=2026-08-01").json()
    assert body["winRate"] is None
    assert body["avgPnlPct"] is None


def test_paper_trades_stats_requires_since() -> None:
    resp = client.get("/v1/paper-trades/stats")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Service: run_intake idempotency + filters
# ---------------------------------------------------------------------------


def test_run_intake_filters_non_cn_symbols() -> None:
    """A HK/ETF decision journal change must be skipped, not inserted."""
    fake_journal = [
        {"symbol": "HK:00700", "action": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
        {"symbol": "ETF:510300", "action": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
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
            return_value={},
        ),
        patch(
            "data_sync_service.service.paper_trading.pt_db.insert_paper_trade",
            return_value={"id": "x"},
        ) as mock_insert,
    ):
        from data_sync_service.service.paper_trading import run_intake

        summary = run_intake(trade_date="2026-08-01")
    # non-cn symbols are filtered out at the candidate stage (counted as
    # skipped with reason 'non-cn' so the operator can see they were seen),
    # so candidates is 0 and skipped is 2.
    assert summary["candidates"] == 0
    assert summary["inserted"] == 0
    assert summary["skipped"] == 2
    assert summary["skippedReasons"].get("non-cn") == 2
    mock_insert.assert_not_called()


def test_run_intake_skips_already_positioned_symbols() -> None:
    """If the user has already taken a real position, we don't paper-trade
    on top of it — that would double-count."""
    fake_journal = [
        {"symbol": "CN:000001", "action": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
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
        {"symbol": "CN:000001", "action": "BUY", "why": "MAINLINE_OK", "score": 80, "sleevePct": 5.0},
        {"symbol": "CN:600519", "action": "BUY", "why": "MAINLINE_OK", "score": 70, "sleevePct": 5.0},
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
        {"symbol": "CN:600519", "action": "BUY", "why": "MAINLINE_OK", "score": 70, "sleevePct": 5.0},
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


def _patched_run_update(open_rows, bars_by_ts, today_iso="2026-08-06"):
    return (
        patch("data_sync_service.service.paper_trading.pt_db.get_open_paper_trades", return_value=open_rows),
        patch("data_sync_service.service.paper_trading.fetch_last_ohlcv_batch", return_value=bars_by_ts),
    )


def test_run_update_closes_on_stop_loss() -> None:
    """pnl_pct <= -5% must close with reason 'stop_hit'."""
    open_rows = [{**_OPEN_ROW, "entry_price": 10.0, "entry_date": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 9.4, 9.4, 9.3, 9.4, 1000)]}  # -6% drawdown
    p1, p2 = _patched_run_update(open_rows, bars)
    with p1, p2, patch(
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


def test_run_update_closes_on_max_hold() -> None:
    """holding_days >= 5 must close with reason 'max_hold'."""
    open_rows = [{**_OPEN_ROW, "entry_price": 10.0, "entry_date": "2026-08-01"}]
    bars = {"000001.SZ": [("2026-08-06", 10.3, 10.3, 10.2, 10.3, 1000)]}  # +3% (no stop)
    p1, p2 = _patched_run_update(open_rows, bars)
    with p1, p2, patch(
        "data_sync_service.service.paper_trading.pt_db.close_paper_trade",
        return_value={**_OPEN_ROW, "status": "closed", "close_reason": "max_hold"},
    ) as mock_close, patch(
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
    open_rows = [{**_OPEN_ROW, "entry_price": 10.0, "entry_date": "2026-08-04"}]
    bars = {"000001.SZ": [("2026-08-06", 9.8, 9.8, 9.7, 9.8, 1000)]}
    p1, p2 = _patched_run_update(open_rows, bars)
    with p1, p2, patch(
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
    open_rows = [{**_OPEN_ROW, "entry_price": 10.0, "entry_date": "2026-08-04"}]
    bars: dict[str, list] = {}  # nothing
    p1, p2 = _patched_run_update(open_rows, bars)
    with p1, p2, patch(
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
# Service: compute_stats passthrough
# ---------------------------------------------------------------------------


def test_compute_stats_passes_since() -> None:
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(10, 7),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=1.23,
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01")
    assert out["closedCount"] == 10
    assert out["winningCount"] == 7
    assert out["winRate"] == 0.7
    assert out["avgPnlPct"] == 1.23


def test_compute_stats_handles_empty() -> None:
    with patch(
        "data_sync_service.service.paper_trading.pt_db.count_since",
        return_value=(0, 0),
    ), patch(
        "data_sync_service.service.paper_trading.pt_db.avg_pnl_pct_since",
        return_value=None,
    ):
        from data_sync_service.service.paper_trading import compute_stats

        out = compute_stats(since_iso="2026-08-01")
    assert out["winRate"] is None
    assert out["avgPnlPct"] is None


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


def test_resolve_cn_ts_code_accepts_only_cn() -> None:
    from data_sync_service.service.paper_trading import _resolve_cn_ts_code

    assert _resolve_cn_ts_code("CN:000001") == "000001.SZ"
    assert _resolve_cn_ts_code("HK:00700") is None
    assert _resolve_cn_ts_code("ETF:510300") is None
    assert _resolve_cn_ts_code("unknown") is None
