from fastapi.testclient import TestClient

from data_sync_service.main import app


def test_healthz() -> None:
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    payload = response.json()
    assert "status" in payload
    assert "db" in payload


def test_market_bars_compat_endpoint_shape() -> None:
    client = TestClient(app)
    resp = client.get("/market/stocks/CN:000001/bars?days=60")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"symbol", "market", "ticker", "name", "currency", "bars"}
    assert isinstance(payload["bars"], list)
