from fastapi.testclient import TestClient

import main


def test_broker_accounts_crud(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)

    # Create
    resp = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    )
    assert resp.status_code == 200
    acc = resp.json()
    acc_id = acc["id"]
    assert acc["broker"] == "pingan"
    assert acc["title"] == "Main"

    # List filter by broker
    resp = client.get("/broker/accounts?broker=pingan")
    assert resp.status_code == 200
    items = resp.json()
    assert any(x["id"] == acc_id for x in items)

    # Update
    resp = client.put(f"/broker/accounts/{acc_id}", json={"title": "Main v2"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.get("/broker/accounts?broker=pingan")
    assert resp.status_code == 200
    items = resp.json()
    updated = next(x for x in items if x["id"] == acc_id)
    assert updated["title"] == "Main v2"

    # Delete
    resp = client.delete(f"/broker/accounts/{acc_id}")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}

    resp = client.delete(f"/broker/accounts/{acc_id}")
    assert resp.status_code == 404


