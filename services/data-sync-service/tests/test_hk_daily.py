from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_sync_hk_daily_no_stock_list(monkeypatch) -> None:
    import data_sync_service.service.hk_daily as hk_daily  # type: ignore[import-not-found]

    monkeypatch.setattr(hk_daily, "fetch_ts_codes_by_market", lambda _market: [])
    result = hk_daily.sync_hk_daily_full()
    assert result["ok"] is True
    assert result["updated"] == 0


def test_sync_hk_daily_endpoint_shape(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]

    monkeypatch.setattr(
        sync_routes,
        "sync_hk_daily_full",
        lambda: {"ok": True, "updated": 1},
    )

    client = TestClient(app)
    resp = client.post("/sync/hk-daily")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["updated"] == 1

