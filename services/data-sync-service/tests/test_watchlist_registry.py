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


@pytest.fixture
def _seed_hk_stock_basic_names() -> None:
    """Ensure stock_basic contains the HK names the backfill reads.

    CI's fresh database has no stock_basic rows, so the backfill tests must
    seed the lookup table themselves instead of relying on a prior hk_basic
    sync (which is a network operation).
    """
    import pandas as pd  # type: ignore[import-not-found, import-untyped]

    from data_sync_service.db import get_connection
    from data_sync_service.db.stock_basic import ensure_table, upsert_from_dataframe

    ensure_table()
    ts_codes = ["01810.HK", "00700.HK"]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, name FROM stock_basic WHERE ts_code = ANY(%s)",
                (ts_codes,),
            )
            before = {str(r[0]): r[1] for r in cur.fetchall()}
    df = pd.DataFrame(
        [
            {"ts_code": "01810.HK", "symbol": "01810", "name": "小米集团-W", "market": "HK"},
            {"ts_code": "00700.HK", "symbol": "00700", "name": "腾讯控股", "market": "HK"},
        ]
    )
    upsert_from_dataframe(df, keep_industry=True)
    yield
    with get_connection() as conn:
        with conn.cursor() as cur:
            for tc in ts_codes:
                if tc in before:
                    cur.execute(
                        "UPDATE stock_basic SET name = %s WHERE ts_code = %s",
                        (before[tc], tc),
                    )
                else:
                    cur.execute("DELETE FROM stock_basic WHERE ts_code = %s", (tc,))
        conn.commit()


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


def test_post_backfills_hk_name_from_stock_basic(_seed_hk_stock_basic_names) -> None:
    """When client sends HK:01810 with name=null, the route fills it from stock_basic.

    The client resolve path can miss this when stock_basic wasn't synced yet or when
    the watchlist item was migrated from a registry without name resolution.
    """
    client = TestClient(app)
    _clear_registry(client)
    client.post(
        "/watchlist/registry",
        json={
            "items": [
                {
                    "symbol": "HK:01810",
                    "name": None,
                    "addedAt": "2026-06-18T00:00:00.000Z",
                },
                {
                    "symbol": "HK:00700",
                    "name": None,
                    "addedAt": "2026-06-18T00:00:00.000Z",
                },
            ]
        },
    )
    rows = client.get("/watchlist/registry").json()["items"]
    by_sym = {r["symbol"]: r for r in rows}
    # stock_basic should already contain these names (hk_basic sync).
    assert by_sym["HK:01810"]["name"] == "小米集团-W"
    assert by_sym["HK:00700"]["name"] == "腾讯控股"


def test_post_preserves_client_provided_name() -> None:
    """If the client already sends a name, the route should NOT overwrite it."""
    client = TestClient(app)
    _clear_registry(client)
    client.post(
        "/watchlist/registry",
        json={
            "items": [
                {
                    "symbol": "HK:01810",
                    "name": "Custom Name",
                    "addedAt": "2026-06-18T00:00:00.000Z",
                }
            ]
        },
    )
    rows = client.get("/watchlist/registry").json()["items"]
    assert rows[0]["name"] == "Custom Name"


def test_backfill_names_endpoint_fills_nulls(_seed_hk_stock_basic_names) -> None:
    """POST /watchlist/registry/backfill-names fills null names using stock_basic."""
    client = TestClient(app)
    _clear_registry(client)
    # Stage items with null names for symbols known to exist in stock_basic.
    client.post(
        "/watchlist/registry",
        json={
            "items": [
                {"symbol": "HK:01810", "name": None, "addedAt": "2026-06-18T00:00:00.000Z"},
                {"symbol": "HK:00700", "name": None, "addedAt": "2026-06-18T00:00:00.000Z"},
                {"symbol": "CN:999999", "name": None, "addedAt": "2026-06-18T00:00:00.000Z"},
            ]
        },
    )
    resp = client.post("/watchlist/registry/backfill-names")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["total"] == 3
    # CN:999999 does not exist in stock_basic → still null.
    rows = client.get("/watchlist/registry").json()["items"]
    by_sym = {r["symbol"]: r for r in rows}
    assert by_sym["HK:01810"]["name"] == "小米集团-W"
    assert by_sym["HK:00700"]["name"] == "腾讯控股"
    assert by_sym["CN:999999"]["name"] is None
