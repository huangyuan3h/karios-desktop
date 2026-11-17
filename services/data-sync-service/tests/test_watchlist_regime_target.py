import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


@pytest.mark.requires_postgres
def test_watchlist_regime_target_shape() -> None:
    client = TestClient(app)
    resp = client.get("/watchlist/regime-target?as_of=2026-11-13")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["state"] in {"bull", "bear"}
    assert payload["target"] in {"starship_robust", "starport"}
    assert payload["confirmedAt"] <= payload["asOf"]
    assert payload["nextReview"] >= payload["asOf"]
    assert "MA200" in payload["rule"]
