"""user_trades_routes API coverage (mocked db layer)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app

client = TestClient(app)

import data_sync_service.api.user_trades_routes as ur  # noqa: E402


@pytest.fixture(autouse=True)
def _patch_deps(monkeypatch):
    monkeypatch.setattr(ur, "ensure_tables", lambda: None)
    monkeypatch.setattr(
        ur,
        "insert_trade",
        lambda **kw: {
            "id": "t1",
            "symbol": kw["symbol"],
            "side": kw["side"],
            "price": kw["price"],
            "positionPct": kw["position_pct"],
            "costBasis": kw.get("cost_basis"),
            "entryDate": kw.get("entry_date"),
            "pnlPct": kw.get("pnl_pct"),
            "holdingDays": kw.get("holding_days"),
            "source": kw.get("source"),
            "market": kw.get("market", "CN"),
            "note": kw.get("note"),
        },
    )
    monkeypatch.setattr(
        ur,
        "list_trades",
        lambda limit=50, symbol=None: [{"id": "t1", "symbol": "CN:600000"}],
    )
    monkeypatch.setattr(
        ur,
        "compute_trade_stats",
        lambda: {"total": 1, "bySource": {}, "roundTripCostPct": 0.3},
    )
    monkeypatch.setattr(ur, "delete_trade", lambda trade_id: trade_id == "t1")
    yield


def test_record_sell_computes_pnl() -> None:
    r = client.post(
        "/trades",
        json={
            "symbol": "CN:600000",
            "side": "SELL",
            "price": 11.0,
            "positionPct": 5.0,
            "costBasis": 10.0,
            "entryDate": "2026-08-01",
            "tradeDate": "2026-08-08",
            "source": "ALPHA",
            "market": "CN",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    # pnl = (11 - 10) / 10 * 100 = 10%; holding = 7 days
    assert body["trade"]["pnlPct"] == pytest.approx(10.0)
    assert body["trade"]["holdingDays"] == 7


def test_record_buy_accepts_without_cost_basis() -> None:
    r = client.post(
        "/trades",
        json={"symbol": "CN:600000", "side": "BUY", "price": 10.0, "positionPct": 5.0},
    )
    assert r.status_code == 200
    assert r.json()["trade"]["pnlPct"] is None


def test_record_sell_without_cost_basis_records_no_pnl() -> None:
    """2026-08-09: SELL no longer requires costBasis/entryDate — holdings
    missing them must still be recordable; pnl stays null."""
    r = client.post(
        "/trades",
        json={"symbol": "CN:600000", "side": "SELL", "price": 10.0, "positionPct": 5.0},
    )
    assert r.status_code == 200
    trade = r.json()["trade"]
    assert trade["side"] == "SELL"
    assert trade["pnlPct"] is None
    assert trade["holdingDays"] is None


def test_record_sell_with_cost_basis_computes_pnl() -> None:
    r = client.post(
        "/trades",
        json={
            "symbol": "CN:600000",
            "side": "SELL",
            "price": 12.0,
            "positionPct": 5.0,
            "costBasis": 10.0,
            "entryDate": "2026-08-01",
        },
    )
    assert r.status_code == 200
    trade = r.json()["trade"]
    assert trade["pnlPct"] == 20.0
    assert trade["holdingDays"] is not None


def test_record_invalid_symbol_rejected() -> None:
    r = client.post(
        "/trades",
        json={"symbol": "bad-symbol!", "side": "BUY", "price": 10.0, "positionPct": 5.0},
    )
    assert r.status_code == 400


def test_record_invalid_side_rejected() -> None:
    r = client.post(
        "/trades",
        json={"symbol": "CN:600000", "side": "HOLD", "price": 10.0, "positionPct": 5.0},
    )
    assert r.status_code == 400


def test_record_zero_price_rejected() -> None:
    r = client.post(
        "/trades",
        json={"symbol": "CN:600000", "side": "BUY", "price": 0, "positionPct": 5.0},
    )
    assert r.status_code == 400


def test_list_trades() -> None:
    r = client.get("/trades", params={"limit": 10, "symbol": "CN:600000"})
    assert r.status_code == 200
    assert r.json()["count"] == 1


def test_stats_endpoint() -> None:
    r = client.get("/trades/stats")
    assert r.status_code == 200
    assert r.json()["stats"]["roundTripCostPct"] == 0.3


def test_delete_trade() -> None:
    r = client.delete("/trades/t1")
    assert r.status_code == 200
    r2 = client.delete("/trades/missing")
    assert r2.status_code == 404
