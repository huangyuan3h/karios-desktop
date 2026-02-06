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


def test_market_sync_handles_non_runtime_errors(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Simulate an upstream connection abort that is NOT a RuntimeError.
    monkeypatch.setattr(main, "fetch_cn_a_spot", lambda: (_ for _ in ()).throw(Exception("Connection aborted")))
    monkeypatch.setattr(main, "fetch_hk_spot", lambda: [])

    client = TestClient(main.app)
    resp = client.post("/market/sync")
    assert resp.status_code == 500
    data = resp.json()
    assert data["ok"] is False
    assert "Connection aborted" in str(data.get("error") or "")


def test_market_chips_cn_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Patch spot providers to insert CN/HK quickly (avoid network/AkShare in tests).
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


def test_market_bars_hk_provider_error(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Insert HK stock via sync
    monkeypatch.setattr(
        main,
        "fetch_cn_a_spot",
        lambda: [],
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

    # Make HK provider fail (simulating AkShare internal NoneType error).
    monkeypatch.setattr(main, "fetch_hk_daily_bars", lambda ticker, days=60: (_ for _ in ()).throw(RuntimeError("empty data")))

    resp = client.get("/market/stocks/HK:00005/bars?days=60")
    assert resp.status_code == 500


def test_market_bars_force_refresh_even_when_cached(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Insert CN stock via sync
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
    monkeypatch.setattr(main, "fetch_hk_spot", lambda: [])
    client = TestClient(main.app)
    client.post("/market/sync")

    # Seed cached bars with enough rows so the old logic would NOT fetch.
    with main._connect() as conn:
        ts = "2025-12-20T00:00:00Z"
        for i in range(60):
            d = f"2025-10-{(i % 30) + 1:02d}"
            conn.execute(
                """
                INSERT OR REPLACE INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("CN:000001", d, "10", "11", "9", "10", "100", "1000", ts),
            )
        conn.commit()

    called = {"n": 0}

    def fake_cn_bars(ticker: str, days: int = 60):
        called["n"] += 1
        return [
            main.BarRow(
                date="2025-12-21",
                open="10",
                high="12",
                low="10",
                close="11",
                volume="120",
                amount="1200",
            )
        ]

    monkeypatch.setattr(main, "fetch_cn_a_daily_bars", fake_cn_bars)

    resp = client.get("/market/stocks/CN:000001/bars?days=60&force=true")
    assert resp.status_code == 200
    assert called["n"] == 1
    assert resp.json()["bars"][-1]["close"] == "11"

