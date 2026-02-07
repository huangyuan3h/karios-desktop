from fastapi.testclient import TestClient

from data_sync_service.main import app


def test_foo_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/foo")
    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "foo ok"
    assert "timestamp" in payload
