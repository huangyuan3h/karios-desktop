"""OPT-045 Phase B / OPT-046 tests: the 3 read-only business endpoints.

The business endpoints thinly wrap existing service-layer functions. To keep
these tests fast and Postgres-independent we patch the upstream functions with
fixtures; integration tests that hit real services are marked
``@pytest.mark.requires_postgres``.
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
    """Clear the @lru_cache on config.get_settings before each test.

    Several tests in this module mutate KARIOS_API_KEYS via monkeypatch. The
    env change is reverted by monkeypatch at teardown, but the lru_cache on
    ``config.get_settings`` would otherwise keep serving the stale Settings
    instance to subsequent tests and break the "auth disabled by default"
    invariant. Clearing the cache at the start of every test makes each
    assertion independent of the previous test's env state.
    """
    from data_sync_service import config

    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_TRENDOK_FIXTURE: list[dict[str, Any]] = [
    {
        "symbol": "CN:000001",
        "name": "平安银行",
        "market": "CN",
        "trendOk": True,
        "score": 88,
        "currentPrice": 12.34,
        "changePct": 0.012,
        "buyAction": "buy",
        "buyZoneHigh": 12.0,
        "stopLossPrice": 11.0,
    },
    {
        "symbol": "HK:00700",
        "name": "腾讯控股",
        "market": "HK",
        "trendOk": False,
        "score": 42,
        "currentPrice": 380.0,
        "changePct": -0.025,
        "buyAction": "wait",
        "buyZoneHigh": 370.0,
        "stopLossPrice": 360.0,
    },
]

_WATCHLIST_FIXTURE: list[dict[str, Any]] = [
    {
        "symbol": "CN:000001",
        "name": "平安银行",
        "source": "screener",
        "color": "blue",
        "positionPct": 8.5,
        "costPrice": 11.5,
        "maxPrice": 12.8,
        "entryDate": "2026-07-20",
    },
    {
        "symbol": "HK:00700",
        "name": "腾讯控股",
        "source": "alpha_radar",
        "color": None,
        "positionPct": None,
        "costPrice": None,
        "maxPrice": None,
        "entryDate": None,
    },
]

_JOURNAL_FIXTURE: list[dict[str, Any]] = [
    {
        "changeId": "c-1",
        "symbol": "CN:000001",
        "action": "BUY",
        "why": "MAINLINE_OK",
        "capturedAt": "2026-08-01T10:00:00+08:00",
        "tradeDate": "2026-08-01",
    },
    {
        "changeId": "c-2",
        "symbol": "HK:00700",
        "action": "HOLD",
        "why": "TIME_LOCK_WEAK_REGIME",
        "capturedAt": "2026-08-01T14:35:00+08:00",
        "tradeDate": "2026-08-01",
    },
]


# ---------------------------------------------------------------------------
# /v1/market/snapshot
# ---------------------------------------------------------------------------


def test_market_snapshot_returns_200() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.compute_trendok_for_symbols",
        return_value=_TRENDOK_FIXTURE,
    ):
        resp = client.get("/v1/market/snapshot?symbols=CN:000001&symbols=HK:00700")
    assert resp.status_code == 200


def test_market_snapshot_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.compute_trendok_for_symbols",
        return_value=_TRENDOK_FIXTURE,
    ):
        body = client.get("/v1/market/snapshot?symbols=CN:000001&symbols=HK:00700").json()
    assert set(body.keys()) == {"asOfDate", "items"}
    assert len(body["items"]) == 2
    first = body["items"][0]
    assert set(first.keys()) >= {
        "symbol",
        "name",
        "market",
        "trendOk",
        "score",
        "currentPrice",
        "changePct",
        "buyAction",
        "buyZoneHigh",
        "stopLossPrice",
    }


def test_market_snapshot_dedupes_symbols() -> None:
    """Passing the same symbol twice should return it once, in input order."""
    with patch(
        "data_sync_service.api.v1_business_routes.compute_trendok_for_symbols",
        return_value=_TRENDOK_FIXTURE[:1],
    ) as mock:
        resp = client.get("/v1/market/snapshot?symbols=CN:000001&symbols=CN:000001")
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 1
    # The wrapper must have called upstream with the de-duped list.
    assert mock.call_args.args[0] == ["CN:000001"]


def test_market_snapshot_missing_symbols_422() -> None:
    resp = client.get("/v1/market/snapshot")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# /v1/watchlist/items
# ---------------------------------------------------------------------------


def test_watchlist_items_returns_200() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.list_registry",
        return_value=_WATCHLIST_FIXTURE,
    ):
        resp = client.get("/v1/watchlist/items")
    assert resp.status_code == 200


def test_watchlist_items_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.list_registry",
        return_value=_WATCHLIST_FIXTURE,
    ):
        body = client.get("/v1/watchlist/items").json()
    assert set(body.keys()) == {"asOfDate", "count", "items"}
    assert body["count"] == 2
    assert len(body["items"]) == 2
    for it in body["items"]:
        assert set(it.keys()) >= {
            "symbol",
            "name",
            "source",
            "color",
            "positionPct",
            "costPrice",
            "maxPrice",
            "entryDate",
        }


def test_watchlist_items_preserves_positionPct() -> None:
    """An AI assistant needs positionPct to compute SECTOR_CONC_BLOCK; null
    must round-trip as null, not be silently dropped."""
    with patch(
        "data_sync_service.api.v1_business_routes.list_registry",
        return_value=_WATCHLIST_FIXTURE,
    ):
        items = client.get("/v1/watchlist/items").json()["items"]
    cn = next(i for i in items if i["symbol"] == "CN:000001")
    hk = next(i for i in items if i["symbol"] == "HK:00700")
    assert cn["positionPct"] == 8.5
    assert hk["positionPct"] is None


def test_watchlist_items_empty_registry_ok() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.list_registry",
        return_value=[],
    ):
        body = client.get("/v1/watchlist/items").json()
    assert body["count"] == 0
    assert body["items"] == []


# ---------------------------------------------------------------------------
# /v1/decision-journal/query
# ---------------------------------------------------------------------------


def test_decision_journal_query_returns_200() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.ej_db.list_changes",
        return_value=_JOURNAL_FIXTURE,
    ):
        resp = client.get("/v1/decision-journal/query")
    assert resp.status_code == 200


def test_decision_journal_query_shape() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.ej_db.list_changes",
        return_value=_JOURNAL_FIXTURE,
    ):
        body = client.get("/v1/decision-journal/query?since=2026-08-01&limit=50").json()
    assert set(body.keys()) == {"asOfDate", "changes"}
    assert len(body["changes"]) == 2
    for c in body["changes"]:
        assert set(c.keys()) >= {
            "changeId",
            "symbol",
            "action",
            "why",
            "capturedAt",
            "tradeDate",
        }


def test_decision_journal_query_passes_since_and_limit() -> None:
    with patch(
        "data_sync_service.api.v1_business_routes.ej_db.list_changes",
        return_value=[],
    ) as mock:
        client.get("/v1/decision-journal/query?since=2026-07-01&limit=10")
    assert mock.call_args.kwargs == {"since": "2026-07-01", "limit": 10}


def test_decision_journal_query_clamps_limit() -> None:
    """limit > 500 should fail FastAPI validation (422)."""
    resp = client.get("/v1/decision-journal/query?limit=9999")
    assert resp.status_code == 422


def test_decision_journal_query_preserves_why_codes() -> None:
    """`why` is the field an LLM aggregates on; the wrapper must not drop it."""
    with patch(
        "data_sync_service.api.v1_business_routes.ej_db.list_changes",
        return_value=_JOURNAL_FIXTURE,
    ):
        changes = client.get("/v1/decision-journal/query").json()["changes"]
    reasons = {c["why"] for c in changes}
    assert "MAINLINE_OK" in reasons
    assert "TIME_LOCK_WEAK_REGIME" in reasons


# ---------------------------------------------------------------------------
# Auth: when KARIOS_API_KEYS is set, the business router demands a key.
# ---------------------------------------------------------------------------


def test_business_unauthenticated_by_default() -> None:
    """No KARIOS_API_KEYS → all 3 business endpoints must be reachable without
    an Authorization header (preserves existing internal frontends)."""
    with (
        patch(
            "data_sync_service.api.v1_business_routes.compute_trendok_for_symbols",
            return_value=_TRENDOK_FIXTURE,
        ),
        patch(
            "data_sync_service.api.v1_business_routes.list_registry",
            return_value=[],
        ),
        patch(
            "data_sync_service.api.v1_business_routes.ej_db.list_changes",
            return_value=[],
        ),
    ):
        for path in (
            "/v1/market/snapshot?symbols=CN:000001",
            "/v1/watchlist/items",
            "/v1/decision-journal/query",
        ):
            resp = client.get(path)
            assert resp.status_code == 200, f"{path} unexpectedly required auth"


def test_business_rejects_when_auth_enabled_no_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config

    monkeypatch.setenv("KARIOS_API_KEYS", "good-key")
    config.get_settings.cache_clear()
    resp = client.get("/v1/watchlist/items")
    assert resp.status_code == 401


def test_business_rejects_when_auth_enabled_wrong_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config

    monkeypatch.setenv("KARIOS_API_KEYS", "good-key")
    config.get_settings.cache_clear()
    resp = client.get(
        "/v1/watchlist/items",
        headers={"Authorization": "Bearer bad-key"},
    )
    assert resp.status_code == 401


def test_business_accepts_when_auth_enabled_correct_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config

    monkeypatch.setenv("KARIOS_API_KEYS", "good-key")
    config.get_settings.cache_clear()
    with patch(
        "data_sync_service.api.v1_business_routes.list_registry",
        return_value=[],
    ):
        resp = client.get(
            "/v1/watchlist/items",
            headers={"Authorization": "Bearer good-key"},
        )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Field descriptions: every Pydantic Field must have a non-empty description.
# This is the api-contract.md rule the LLM depends on.
# ---------------------------------------------------------------------------


def _walk_descriptions(model: type) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for name, field in model.model_fields.items():
        out.append((name, field.description or ""))
        if hasattr(field, "annotation"):
            pass  # Pydantic v2 doesn't expose nested models this way
    return out


def test_all_business_models_have_descriptions() -> None:
    from data_sync_service.api.v1_business_routes import (
        DecisionChange,
        DecisionJournalResponse,
        MarketSnapshotItem,
        MarketSnapshotResponse,
        WatchlistItem,
        WatchlistResponse,
    )

    for model in (
        MarketSnapshotItem,
        MarketSnapshotResponse,
        WatchlistItem,
        WatchlistResponse,
        DecisionChange,
        DecisionJournalResponse,
    ):
        for name, desc in _walk_descriptions(model):
            assert desc, f"{model.__name__}.{name} has empty description"
            assert "TODO" not in desc and "TBD" not in desc, (
                f"{model.__name__}.{name} description is a stub: {desc!r}"
            )
