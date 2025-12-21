from fastapi.testclient import TestClient

import main


def test_market_sync_and_list(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Mock providers (avoid network/AkShare dependency in tests).
    def fake_cn():
        return [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Ping An Bank",
                currency="CNY",
                quote={"price": "10.00", "change_pct": "1.23"},
            ),
        ]

    def fake_hk():
        return [
            main.StockRow(
                symbol="HK:00005",
                market="HK",
                ticker="00005",
                name="HSBC",
                currency="HKD",
                quote={"price": "60.00", "change_pct": "-0.50"},
            ),
        ]

    monkeypatch.setattr(main, "fetch_cn_a_spot", fake_cn)
    monkeypatch.setattr(main, "fetch_hk_spot", fake_hk)

    client = TestClient(main.app)

    # Before sync: empty.
    resp = client.get("/market/status")
    assert resp.status_code == 200
    assert resp.json()["stocks"] == 0

    # Sync.
    resp = client.post("/market/sync")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert resp.json()["stocks"] == 2

    # List.
    resp = client.get("/market/stocks?limit=50&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 2
    symbols = {x["symbol"] for x in data["items"]}
    assert symbols == {"CN:000001", "HK:00005"}

    # Filter by market.
    resp = client.get("/market/stocks?market=CN&limit=50&offset=0")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1
    assert resp.json()["items"][0]["symbol"] == "CN:000001"

    # Search.
    resp = client.get("/market/stocks?q=HSBC&limit=50&offset=0")
    assert resp.status_code == 200
    assert resp.json()["total"] == 1
    assert resp.json()["items"][0]["symbol"] == "HK:00005"


