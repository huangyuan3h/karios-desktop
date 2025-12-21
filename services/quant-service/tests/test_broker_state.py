from fastapi.testclient import TestClient

import main


def test_broker_state_sync_updates_consolidated_state(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Create account.
    client = TestClient(main.app)
    acc = client.post(
        "/broker/accounts",
        json={"broker": "pingan", "title": "Main", "accountMasked": "3260****7775"},
    ).json()
    account_id = acc["id"]

    # Mock AI extraction: return positions for first image, overview for second.
    calls = {"n": 0}

    def fake_extract(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "kind": "positions",
                "broker": "pingan",
                "extractedAt": "2025-12-21T00:00:00Z",
                "data": {
                    "positions": [
                        {"ticker": "300502", "name": "Xin Yi Sheng", "qtyHeld": "700", "price": "434.3"},
                        {"ticker": "600988", "name": "Chifeng Gold", "qtyHeld": "7100", "price": "31.95"},
                    ]
                },
            }
        return {
            "kind": "account_overview",
            "broker": "pingan",
            "extractedAt": "2025-12-21T00:00:00Z",
            "data": {"currency": "CNY", "totalAssets": "1414505.53", "cashAvailable": "712140.53"},
        }

    monkeypatch.setattr(main, "_ai_extract_pingan_screenshot", fake_extract)

    # Sync state.
    req = {
        "capturedAt": "2025-12-21T15:06:00+00:00",
        "images": [
            {"id": "a", "name": "a.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,AAAA"},
            {"id": "b", "name": "b.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,BBBB"},
        ],
    }
    resp = client.post(f"/broker/pingan/accounts/{account_id}/sync", json=req)
    assert resp.status_code == 200
    st = resp.json()
    assert st["accountId"] == account_id
    assert st["broker"] == "pingan"
    assert st["updatedAt"] == "2025-12-21T15:06:00+00:00"
    assert st["counts"]["positions"] == 2
    assert st["overview"]["totalAssets"] == "1414505.53"

    # Fetch state again.
    resp = client.get(f"/broker/pingan/accounts/{account_id}/state")
    assert resp.status_code == 200
    st2 = resp.json()
    assert st2["counts"]["positions"] == 2
    assert st2["overview"]["cashAvailable"] == "712140.53"


