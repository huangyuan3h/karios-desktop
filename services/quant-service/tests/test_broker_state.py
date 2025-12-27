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

    # Mock AI extraction: return overview+positions in one image (kind=account_overview),
    # conditional orders for second/third, overview for fourth.
    calls = {"n": 0}

    def fake_extract(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return {
                "kind": "account_overview",
                "broker": "pingan",
                "extractedAt": "2025-12-21T00:00:00Z",
                "data": {
                    "currency": "CNY",
                    "totalAssets": "1414505.53",
                    "cashAvailable": "712140.53",
                    "positions": [
                        {"ticker": "300502", "name": "Xin Yi Sheng", "qtyHeld": "700", "price": "434.3"},
                        {"ticker": "600988", "name": "Chifeng Gold", "qtyHeld": "7100", "price": "31.95"},
                    ]
                },
            }
        if calls["n"] == 2:
            return {
                "kind": "conditional_orders",
                "broker": "pingan",
                "extractedAt": "2025-12-21T00:00:00Z",
                "data": {
                    "orders": [
                        {"ticker": "300502", "name": "Xin Yi Sheng", "side": "sell", "triggerValue": "456", "qty": "500"},
                        {"ticker": "600988", "name": "Chifeng Gold", "side": "sell", "triggerValue": "30.8", "qty": "3500"},
                    ]
                },
            }
        if calls["n"] == 3:
            # Overlap 300502 sell appears again; should be deduped.
            return {
                "kind": "conditional_orders",
                "broker": "pingan",
                "extractedAt": "2025-12-21T00:00:00Z",
                "data": {
                    "orders": [
                        {"ticker": "300308", "name": "Zhong Ji Xuchuang", "side": "sell", "triggerValue": "560", "qty": "300"},
                        {"ticker": "300502", "name": "Xin Yi Sheng", "side": "sell", "triggerValue": "456", "qty": "500"},
                    ]
                },
            }
        if calls["n"] == 4:
            return {
                "kind": "trades",
                "broker": "pingan",
                "extractedAt": "2025-12-21T00:00:00Z",
                "data": {
                    "trades": [
                        {"time": "2025-12-18 09:30:19", "ticker": "300308", "side": "sell", "price": "573.740", "qty": "300"},
                        {"time": "2025-12-18 09:31:01", "ticker": "300502", "side": "sell", "price": "434.320", "qty": "500"},
                    ]
                },
            }
        return {
            "kind": "account_overview",
            "broker": "pingan",
            "extractedAt": "2025-12-21T00:00:00Z",
            # Duplicate trade row should be deduped by time + ticker.
            "data": {
                "currency": "CNY",
                "totalAssets": "1414505.53",
                "cashAvailable": "712140.53",
                "trades": [
                    {"time": "2025-12-18 09:31:01", "ticker": "300502", "side": "sell", "price": "434.320", "qty": "500"},
                ],
            },
        }

    monkeypatch.setattr(main, "_ai_extract_pingan_screenshot", fake_extract)

    # Sync state.
    req = {
        "capturedAt": "2025-12-21T15:06:00+00:00",
        "images": [
            {"id": "a", "name": "a.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,AAAA"},
            {"id": "b", "name": "b.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,BBBB"},
            {"id": "c", "name": "c.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,CCCC"},
            {"id": "d", "name": "d.png", "mediaType": "image/png", "dataUrl": "data:image/png;base64,DDDD"},
        ],
    }
    resp = client.post(f"/broker/pingan/accounts/{account_id}/sync", json=req)
    assert resp.status_code == 200
    st = resp.json()
    assert st["accountId"] == account_id
    assert st["broker"] == "pingan"
    assert st["updatedAt"] == "2025-12-21T15:06:00+00:00"
    assert st["counts"]["positions"] == 2
    assert st["counts"]["conditionalOrders"] == 3
    assert st["counts"]["trades"] == 2
    assert st["overview"]["totalAssets"] == "1414505.53"

    # Fetch state again.
    resp = client.get(f"/broker/pingan/accounts/{account_id}/state")
    assert resp.status_code == 200
    st2 = resp.json()
    assert st2["counts"]["positions"] == 2
    assert st2["overview"]["cashAvailable"] == "712140.53"

    # Manual adjustment: delete one conditional order from consolidated state.
    del_req = {"order": {"ticker": "300308", "side": "sell", "triggerValue": "560", "qty": "300"}}
    resp = client.post(
        f"/broker/pingan/accounts/{account_id}/state/conditional-orders/delete",
        json=del_req,
    )
    assert resp.status_code == 200
    st3 = resp.json()
    assert st3["counts"]["conditionalOrders"] == 2


