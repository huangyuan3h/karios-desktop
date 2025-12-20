from fastapi.testclient import TestClient

from main import app


def test_system_prompt_roundtrip(tmp_path, monkeypatch) -> None:
    # Ensure the DB goes to a temp location for test isolation.
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(app)

    resp = client.get("/settings/system-prompt")
    assert resp.status_code == 200
    assert resp.json() == {"value": ""}

    resp = client.put("/settings/system-prompt", json={"value": "You are helpful."})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get("/settings/system-prompt")
    assert resp.status_code == 200
    assert resp.json() == {"value": "You are helpful."}


