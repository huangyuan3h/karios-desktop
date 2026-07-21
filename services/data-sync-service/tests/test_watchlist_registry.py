from __future__ import annotations

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.db.watchlist_automation import list_registry, upsert_registry
from data_sync_service.main import app  # type: ignore[import-not-found]

pytestmark = pytest.mark.requires_postgres


@pytest.fixture(autouse=True)
def _restore_watchlist_registry() -> None:
    """Snapshot/restore so these tests never leave the developer's real registry wiped.

    POST /watchlist/registry is a full replace (including empty → DELETE ALL). Without
    restore, a local pytest run against DATABASE_URL ends on 贵州茅台 and destroys
    the user's watchlist + position fields.
    """
    before = list_registry()
    yield
    upsert_registry(before)


def _clear_registry(client: TestClient) -> None:
    client.post("/watchlist/registry", json={"items": []})


def test_get_registry_empty() -> None:
    client = TestClient(app)
    _clear_registry(client)
    resp = client.get("/watchlist/registry")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["items"] == []
    assert payload["count"] == 0


def test_post_then_get_roundtrip() -> None:
    client = TestClient(app)
    _clear_registry(client)
    items = [
        {
            "symbol": "CN:600000",
            "name": "浦发银行",
            "addedAt": "2026-06-18T00:00:00.000Z",
            "source": "manual",
            "costPrice": 10.5,
            "positionPct": 5,
        },
        {
            "symbol": "CN:000001",
            "name": "平安银行",
            "addedAt": "2026-06-18T01:00:00.000Z",
            "source": "screener",
        },
    ]
    post = client.post("/watchlist/registry", json={"items": items})
    assert post.status_code == 200
    assert post.json()["count"] == 2

    resp = client.get("/watchlist/registry")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["count"] == 2
    by_sym = {x["symbol"]: x for x in payload["items"]}
    assert by_sym["CN:600000"]["costPrice"] == 10.5
    assert by_sym["CN:600000"]["positionPct"] == 5
    assert by_sym["CN:000001"]["source"] == "screener"


def test_post_replace_deletes_removed() -> None:
    client = TestClient(app)
    _clear_registry(client)
    client.post(
        "/watchlist/registry",
        json={
            "items": [
                {"symbol": "CN:600000", "addedAt": "2026-06-18T00:00:00.000Z"},
                {"symbol": "CN:000001", "addedAt": "2026-06-18T00:00:00.000Z"},
            ]
        },
    )
    client.post(
        "/watchlist/registry",
        json={"items": [{"symbol": "CN:600000", "addedAt": "2026-06-18T00:00:00.000Z"}]},
    )
    resp = client.get("/watchlist/registry")
    symbols = {x["symbol"] for x in resp.json()["items"]}
    assert symbols == {"CN:600000"}


def test_get_registry_item_fields() -> None:
    client = TestClient(app)
    _clear_registry(client)
    client.post(
        "/watchlist/registry",
        json={
            "items": [
                {
                    "symbol": "CN:600519",
                    "name": "贵州茅台",
                    "addedAt": "2026-06-18T00:00:00.000Z",
                    "color": "#dcfce7",
                    "maxPrice": 1800,
                    "positionPct": 12.5,
                    "costPrice": 1650,
                }
            ]
        },
    )
    row = client.get("/watchlist/registry").json()["items"][0]
    assert row["symbol"] == "CN:600519"
    assert row["maxPrice"] == 1800
    assert row["positionPct"] == 12.5
    assert row["color"] == "#dcfce7"
