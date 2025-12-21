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


def test_market_chips_cn_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed one CN stock.
    client = TestClient(main.app)
    client.post("/market/sync")  # may fail if not patched, so patch providers below

    # Patch spot providers to insert CN/HK quickly.
    monkeypatch.setattr(
        main,
        "fetch_cn_a_spot",
        lambda: [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Ping An Bank",
                currency="CNY",
                quote={},
            )
        ],
    )
    monkeypatch.setattr(
        main,
        "fetch_hk_spot",
        lambda: [
            main.StockRow(
                symbol="HK:00005",
                market="HK",
                ticker="00005",
                name="HSBC",
                currency="HKD",
                quote={},
            )
        ],
    )
    client.post("/market/sync")

    # Patch chip provider.
    monkeypatch.setattr(
        main,
        "fetch_cn_a_chip_summary",
        lambda ticker, days=60: [
            {
                "date": "2025-12-20",
                "profitRatio": "0.5",
                "avgCost": "10.0",
                "cost90Low": "9.0",
                "cost90High": "11.0",
                "cost90Conc": "0.2",
                "cost70Low": "9.5",
                "cost70High": "10.5",
                "cost70Conc": "0.1",
            }
        ],
    )

    resp = client.get("/market/stocks/CN:000001/chips?days=60")
    assert resp.status_code == 200
    assert resp.json()["symbol"] == "CN:000001"
    assert resp.json()["items"][0]["avgCost"] == "10.0"

    resp = client.get("/market/stocks/HK:00005/chips?days=60")
    assert resp.status_code == 400


def test_market_fund_flow_cn_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    monkeypatch.setattr(
        main,
        "fetch_cn_a_spot",
        lambda: [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Ping An Bank",
                currency="CNY",
                quote={},
            )
        ],
    )
    monkeypatch.setattr(
        main,
        "fetch_hk_spot",
        lambda: [
            main.StockRow(
                symbol="HK:00005",
                market="HK",
                ticker="00005",
                name="HSBC",
                currency="HKD",
                quote={},
            )
        ],
    )
    client = TestClient(main.app)
    client.post("/market/sync")

    monkeypatch.setattr(
        main,
        "fetch_cn_a_fund_flow",
        lambda ticker, days=60: [
            {
                "date": "2025-12-20",
                "close": "10.0",
                "changePct": "1.0",
                "mainNetAmount": "100",
                "mainNetRatio": "2.0",
                "superNetAmount": "40",
                "superNetRatio": "1.0",
                "largeNetAmount": "30",
                "largeNetRatio": "0.8",
                "mediumNetAmount": "20",
                "mediumNetRatio": "0.5",
                "smallNetAmount": "10",
                "smallNetRatio": "0.2",
            }
        ],
    )

    resp = client.get("/market/stocks/CN:000001/fund-flow?days=60")
    assert resp.status_code == 200
    assert resp.json()["items"][0]["mainNetAmount"] == "100"

    resp = client.get("/market/stocks/HK:00005/fund-flow?days=60")
    assert resp.status_code == 400


