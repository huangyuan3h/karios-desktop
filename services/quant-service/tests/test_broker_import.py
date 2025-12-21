from fastapi.testclient import TestClient

import main


def test_broker_import_dedupes_by_sha256(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Mock AI extraction.
    monkeypatch.setattr(
        main,
        "_ai_extract_pingan_screenshot",
        lambda **kwargs: {"kind": "positions", "source": "pingan", "ok": True},
    )

    # Use a tiny fake PNG payload.
    img_bytes = b"\x89PNG\r\n\x1a\n" + b"0" * 32
    data_url = "data:image/png;base64," + __import__("base64").b64encode(img_bytes).decode("ascii")

    client = TestClient(main.app)

    req = {
        "capturedAt": "2025-12-21T15:06:00+00:00",
        "images": [
            {
                "id": "a",
                "name": "shot.png",
                "mediaType": "image/png",
                "dataUrl": data_url,
            },
            {
                "id": "b",
                "name": "shot-dup.png",
                "mediaType": "image/png",
                "dataUrl": data_url,
            },
        ],
    }

    resp = client.post("/broker/pingan/import", json=req)
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert len(data["items"]) == 2
    assert data["items"][0]["id"] == data["items"][1]["id"]

    # Listing should include the snapshot.
    resp = client.get("/broker/pingan/snapshots?limit=10")
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) >= 1
    assert any(x["id"] == data["items"][0]["id"] for x in items)

    # Detail should include extracted JSON.
    snap_id = data["items"][0]["id"]
    resp = client.get(f"/broker/pingan/snapshots/{snap_id}")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["id"] == snap_id
    assert detail["extracted"]["kind"] == "positions"


