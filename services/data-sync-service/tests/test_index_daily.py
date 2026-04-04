from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_sync_index_daily_no_index_list(monkeypatch) -> None:
    import data_sync_service.service.index_daily as index_daily  # type: ignore[import-not-found]

    monkeypatch.setattr(index_daily, "INDEX_CODES", [])
    result = index_daily.sync_index_daily_full()
    assert result["ok"] is True
    assert result.get("updated", 0) == 0


def test_sync_close_endpoint_includes_index_daily(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]

    monkeypatch.setattr(sync_routes, "sync_close", lambda exchange, force: {"ok": True, "updated": 1})

    def _post() -> dict:
        return {
            "indexDaily": {"ok": True, "updated": 2},
            "macroDaily": {"ok": True, "updated": 3},
        }

    monkeypatch.setattr(sync_routes, "run_post_close_sync", _post)

    client = TestClient(app)
    resp = client.post("/sync/close")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["updated"] == 1
    assert payload["indexDaily"]["ok"] is True
    assert payload["indexDaily"]["updated"] == 2
    assert payload["macroDaily"]["ok"] is True
    assert payload["macroDaily"]["updated"] == 3
