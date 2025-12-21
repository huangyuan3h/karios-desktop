from fastapi.testclient import TestClient

import main
from tv.capture import CaptureResult


def test_tv_screeners_seed_and_crud(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)

    # Seed defaults on first list.
    resp = client.get("/integrations/tradingview/screeners")
    assert resp.status_code == 200
    items = resp.json()["items"]
    ids = {x["id"] for x in items}
    assert {"falcon", "blackhorse"}.issubset(ids)

    # Create a custom screener.
    resp = client.post(
        "/integrations/tradingview/screeners",
        json={
            "name": "My Screener",
            "url": "https://www.tradingview.com/screener/abc/",
            "enabled": True,
        },
    )
    assert resp.status_code == 200
    new_id = resp.json()["id"]
    assert isinstance(new_id, str) and new_id

    resp = client.get("/integrations/tradingview/screeners")
    assert resp.status_code == 200
    assert any(x["id"] == new_id and x["name"] == "My Screener" for x in resp.json()["items"])

    # Update it.
    resp = client.put(
        f"/integrations/tradingview/screeners/{new_id}",
        json={
            "name": "My Screener v2",
            "url": "https://www.tradingview.com/screener/xyz/",
            "enabled": False,
        },
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get("/integrations/tradingview/screeners")
    assert resp.status_code == 200
    updated = next(x for x in resp.json()["items"] if x["id"] == new_id)
    assert updated["name"] == "My Screener v2"
    assert updated["enabled"] is False

    # Delete it.
    resp = client.delete(f"/integrations/tradingview/screeners/{new_id}")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.delete(f"/integrations/tradingview/screeners/{new_id}")
    assert resp.status_code == 404


def test_tv_sync_persists_snapshot(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Make CDP appear available.
    monkeypatch.setattr(main, "_cdp_version", lambda host, port: {"Browser": "Chrome"})

    # Mock capture.
    fake = CaptureResult(
        url="https://www.tradingview.com/screener/TMcms1mM/",
        captured_at="2025-12-21T00:00:00+00:00",
        screen_title="Swing Falcon Filter",
        filters=["Market 2", "Price > EMA (50)", "RSI (14) 50 to 80"],
        headers=["Symbol", "Price"],
        rows=[{"Symbol": "000001", "Price": "10.00 CNY"}],
    )
    monkeypatch.setattr(main, "capture_screener_over_cdp_sync", lambda **kwargs: fake)

    client = TestClient(main.app)

    # Ensure defaults are present.
    resp = client.get("/integrations/tradingview/screeners")
    assert resp.status_code == 200

    # Sync default screener.
    resp = client.post("/integrations/tradingview/screeners/falcon/sync")
    assert resp.status_code == 200
    snapshot_id = resp.json()["snapshotId"]
    assert isinstance(snapshot_id, str) and snapshot_id
    assert resp.json()["rowCount"] == 1

    # List snapshots.
    resp = client.get("/integrations/tradingview/screeners/falcon/snapshots?limit=10")
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert any(x["id"] == snapshot_id for x in items)

    # Fetch snapshot detail.
    resp = client.get(f"/integrations/tradingview/snapshots/{snapshot_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == snapshot_id
    assert data["screenerId"] == "falcon"
    assert data["rowCount"] == 1
    assert "filters" in data and isinstance(data["filters"], list)
    assert data["headers"] == ["Symbol", "Price"]
    assert data["rows"][0]["Symbol"] == "000001"


