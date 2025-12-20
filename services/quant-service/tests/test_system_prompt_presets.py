from fastapi.testclient import TestClient

from main import app


def test_system_prompt_presets_crud_and_active(tmp_path, monkeypatch) -> None:
    # Ensure the DB goes to a temp location for test isolation.
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(app)

    # Create a preset (also becomes active).
    resp = client.post("/system-prompts", json={"title": "Default", "content": "You are precise."})
    assert resp.status_code == 200
    preset_id = resp.json()["id"]
    assert isinstance(preset_id, str) and preset_id

    # List shows it.
    resp = client.get("/system-prompts")
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert any(x["id"] == preset_id and x["title"] == "Default" for x in items)

    # Active returns it.
    resp = client.get("/system-prompts/active")
    assert resp.status_code == 200
    assert resp.json()["id"] == preset_id
    assert resp.json()["title"] == "Default"
    assert resp.json()["content"] == "You are precise."

    # Update preset.
    resp = client.put(
        f"/system-prompts/{preset_id}",
        json={"title": "Default v2", "content": "You are concise."},
    )
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get(f"/system-prompts/{preset_id}")
    assert resp.status_code == 200
    assert resp.json() == {"id": preset_id, "title": "Default v2", "content": "You are concise."}

    # Legacy endpoint writes into active preset when active exists.
    resp = client.put("/settings/system-prompt", json={"value": "You are helpful."})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get(f"/system-prompts/{preset_id}")
    assert resp.status_code == 200
    assert resp.json()["content"] == "You are helpful."

    # Clear active -> /settings/system-prompt falls back to legacy value.
    resp = client.put("/system-prompts/active", json={"id": None})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.put("/settings/system-prompt", json={"value": "Legacy only."})
    assert resp.status_code == 200

    resp = client.get("/system-prompts/active")
    assert resp.status_code == 200
    assert resp.json() == {"id": None, "title": "Legacy", "content": "Legacy only."}

    # Delete preset.
    resp = client.delete(f"/system-prompts/{preset_id}")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get(f"/system-prompts/{preset_id}")
    assert resp.status_code == 404


