"""OPT-047 Phase C tests: GET /v1/explain/{symbol} + auth + description guard.

Uses mocks so the test does not depend on a live Postgres / Tushare.
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
# Fixtures
# ---------------------------------------------------------------------------


_TRENDOK_FIXTURE: list[dict[str, Any]] = [
    {
        "symbol": "CN:000001",
        "name": "平安银行",
        "market": "CN",
        "trendOk": True,
        "trendStatus": "healthy",
        "score": 88,
        "scoreParts": {
            "emaTrend": 32,
            "macdMomentum": 18,
            "volume": 16,
            "breakoutSmooth": 8,
            "rsiComfort": 9,
        },
        "stopLossPrice": 11.0,
        "stopLossParts": {"exit_now": False, "support": 11.4, "hardStop": 11.0},
        "buyMode": "A_pullback",
        "buyAction": "buy",
        "buyZoneLow": 11.8,
        "buyZoneHigh": 12.0,
        "buyRefPrice": 11.9,
        "currentPrice": 11.95,
        "changePct": 0.012,
    }
]

_WATCHLIST_FIXTURE: list[dict[str, Any]] = [
    {
        "symbol": "CN:000001",
        "name": "平安银行",
        "source": "screener",
        "positionPct": 8.5,
        "costPrice": 11.5,
        "maxPrice": 12.1,
        "entryDate": "2026-07-20",
    }
]

_JOURNAL_FIXTURE: list[dict[str, Any]] = [
    {
        "changeId": "c-1",
        "symbol": "CN:000001",
        "action": "BUY",
        "why": "MAINLINE_OK",
        "capturedAt": "2026-07-21T10:00:00+08:00",
        "tradeDate": "2026-07-21",
    },
    {
        "changeId": "c-2",
        "symbol": "CN:000001",
        "action": "HOLD",
        "why": "TIME_LOCK_WEAK_REGIME",
        "capturedAt": "2026-07-25T14:35:00+08:00",
        "tradeDate": "2026-07-25",
    },
    {
        "changeId": "c-other",
        "symbol": "CN:600519",  # different symbol — must be filtered out
        "action": "HOLD",
        "why": "OTHER",
        "capturedAt": "2026-07-26T11:00:00+08:00",
        "tradeDate": "2026-07-26",
    },
]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def _patch_all(trendok=_TRENDOK_FIXTURE, watchlist=_WATCHLIST_FIXTURE, journal=_JOURNAL_FIXTURE):
    return (
        patch(
            "data_sync_service.api.v1_explain_routes.compute_trendok_for_symbols",
            return_value=trendok,
        ),
        patch(
            "data_sync_service.api.v1_explain_routes.list_registry",
            return_value=watchlist,
        ),
        patch(
            "data_sync_service.api.v1_explain_routes.ej_db.list_changes",
            return_value=journal,
        ),
    )


def test_explain_returns_200() -> None:
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        resp = client.get("/v1/explain/CN:000001")
    assert resp.status_code == 200


def test_explain_envelope_shape() -> None:
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    assert set(body.keys()) == {
        "asOfDate",
        "symbol",
        "name",
        "market",
        "trendok",
        "watchlist",
        "recentChanges",
        "recentChangesWindowDays",
    }
    assert body["symbol"] == "CN:000001"
    assert body["name"] == "平安银行"
    assert body["market"] == "CN"
    assert body["recentChangesWindowDays"] == 30


def test_explain_includes_full_trendok() -> None:
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    tk = body["trendok"]
    # scoreParts + stopLossParts are the LLM-facing sub-fields.
    assert tk["score"] == 88
    assert tk["trendOk"] is True
    assert tk["buyAction"] == "buy"
    assert "scoreParts" in tk and tk["scoreParts"]["emaTrend"] == 32
    assert "stopLossParts" in tk and tk["stopLossParts"]["hardStop"] == 11.0


def test_explain_watchlist_state() -> None:
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    wl = body["watchlist"]
    assert wl["inWatchlist"] is True
    assert wl["source"] == "screener"
    assert wl["positionPct"] == 8.5
    assert wl["costPrice"] == 11.5
    assert wl["entryDate"] == "2026-07-20"


def test_explain_recent_changes_filtered_to_symbol() -> None:
    """`recentChanges` must NOT include rows for other symbols."""
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    changes = body["recentChanges"]
    assert len(changes) == 2  # the 600519 row must be filtered out
    assert all(c["action"] in {"BUY", "HOLD"} for c in changes)
    assert any(c["why"] == "MAINLINE_OK" for c in changes)
    assert any(c["why"] == "TIME_LOCK_WEAK_REGIME" for c in changes)


def test_explain_caps_recent_changes_at_5() -> None:
    big = []
    for i in range(20):
        big.append(
            {
                "changeId": f"c-{i}",
                "symbol": "CN:000001",
                "action": "HOLD",
                "why": f"REASON_{i}",
                "capturedAt": f"2026-07-{(i % 28) + 1:02d}T10:00:00+08:00",
                "tradeDate": f"2026-07-{(i % 28) + 1:02d}",
            }
        )
    p1, p2, p3 = _patch_all(journal=big)
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    assert len(body["recentChanges"]) == 5


def test_explain_symbol_not_in_watchlist() -> None:
    p1, p2, p3 = _patch_all(watchlist=[])
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    assert body["watchlist"]["inWatchlist"] is False
    assert body["watchlist"]["positionPct"] is None
    assert body["watchlist"]["source"] is None


def test_explain_no_journal_changes() -> None:
    p1, p2, p3 = _patch_all(journal=[])
    with p1, p2, p3:
        body = client.get("/v1/explain/CN:000001").json()
    assert body["recentChanges"] == []


def test_explain_unresolved_symbol() -> None:
    """A symbol that has no trendok / no watchlist row / no journal rows still
    returns 200 with mostly-null fields — the caller decides what 'no data' means."""
    p1, p2, p3 = _patch_all(
        trendok=[],
        watchlist=[],
        journal=[],
    )
    with p1, p2, p3:
        resp = client.get("/v1/explain/CN:999999")
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] is None
    assert body["market"] is None
    assert body["trendok"] == {}
    assert body["watchlist"]["inWatchlist"] is False
    assert body["recentChanges"] == []


def test_explain_passes_30d_window_to_journal() -> None:
    with (
        patch(
            "data_sync_service.api.v1_explain_routes.compute_trendok_for_symbols",
            return_value=_TRENDOK_FIXTURE,
        ),
        patch(
            "data_sync_service.api.v1_explain_routes.list_registry",
            return_value=_WATCHLIST_FIXTURE,
        ),
        patch(
            "data_sync_service.api.v1_explain_routes.ej_db.list_changes",
            return_value=[],
        ) as mock_journal,
    ):
        client.get("/v1/explain/CN:000001")
    # Window must be exactly 30 days. If this changes, update the docstring
    # AND the schema's `recentChangesWindowDays` description.
    assert mock_journal.call_args.kwargs["limit"] == 200
    since_arg = mock_journal.call_args.kwargs["since"]
    assert since_arg is not None
    assert len(since_arg) == 10  # YYYY-MM-DD


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_explain_unauthenticated_by_default() -> None:
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        resp = client.get("/v1/explain/CN:000001")
    assert resp.status_code == 200


def test_explain_rejects_when_auth_enabled_no_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config

    monkeypatch.setenv("KARIOS_API_KEYS", "good-key")
    config.get_settings.cache_clear()
    resp = client.get("/v1/explain/CN:000001")
    assert resp.status_code == 401


def test_explain_accepts_correct_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config

    monkeypatch.setenv("KARIOS_API_KEYS", "good-key")
    config.get_settings.cache_clear()
    p1, p2, p3 = _patch_all()
    with p1, p2, p3:
        resp = client.get(
            "/v1/explain/CN:000001",
            headers={"Authorization": "Bearer good-key"},
        )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Field descriptions: every Field must have a non-empty description.
# ---------------------------------------------------------------------------


def test_all_explain_models_have_descriptions() -> None:
    from data_sync_service.api.v1_explain_routes import (
        ExplainResponse,
        RecentChange,
        WatchlistState,
    )

    for model in (ExplainResponse, WatchlistState, RecentChange):
        for name, field in model.model_fields.items():
            assert field.description, f"{model.__name__}.{name} has empty description"
            assert "TODO" not in field.description and "TBD" not in field.description, (
                f"{model.__name__}.{name} description is a stub: {field.description!r}"
            )
