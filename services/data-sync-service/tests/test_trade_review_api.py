from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_trade_review_crud_endpoints(monkeypatch) -> None:
    import data_sync_service.api.trade_review_routes as routes  # type: ignore[import-not-found]

    state: dict[str, dict] = {}

    def _fetch_all(limit: int = 20, offset: int = 0, symbol: str | None = None):
        items = list(state.values())
        if symbol:
            items = [x for x in items if x.get("symbol") == symbol]
        total = len(items)
        return total, items[offset : offset + limit]

    def _fetch_by_id(review_id: str):
        return state.get(review_id)

    def _create_review(*, review_id: str, payload: dict, created_at: str, updated_at: str):
        obj = {
            "id": review_id,
            "symbol": payload["symbol"],
            "stockName": payload.get("stockName"),
            "buyDate": payload.get("buyDate"),
            "sellDate": payload.get("sellDate"),
            "holdingDays": payload.get("holdingDays"),
            "pnlAmount": payload.get("pnlAmount"),
            "pnlPct": payload.get("pnlPct"),
            "totalCapitalImpactPct": payload.get("totalCapitalImpactPct"),
            "maxLossGuardrailPct": payload.get("maxLossGuardrailPct", 2.0),
            "marketLightEntry": payload.get("marketLightEntry"),
            "marketLightExit": payload.get("marketLightExit"),
            "buyLogicFundResonance": payload.get("buyLogicFundResonance", False),
            "buyLogicPatternBreakout": payload.get("buyLogicPatternBreakout", False),
            "buyLogicMacroSentiment": payload.get("buyLogicMacroSentiment", False),
            "buyLogicNotes": payload.get("buyLogicNotes"),
            "positionPct": payload.get("positionPct"),
            "buyAvgPrice": payload.get("buyAvgPrice"),
            "initialDefensePrice": payload.get("initialDefensePrice"),
            "sellAvgPrice": payload.get("sellAvgPrice"),
            "sellReason": payload.get("sellReason"),
            "executionNotes": payload.get("executionNotes"),
            "goodActions": payload.get("goodActions"),
            "improvementAreas": payload.get("improvementAreas"),
            "customPayload": payload.get("customPayload", {}),
            "createdAt": created_at,
            "updatedAt": updated_at,
        }
        state[review_id] = obj
        return obj

    def _update_review(*, review_id: str, payload: dict, updated_at: str):
        existing = state.get(review_id)
        if not existing:
            return None
        existing.update(payload)
        existing["updatedAt"] = updated_at
        return existing

    def _delete_review(review_id: str):
        return state.pop(review_id, None) is not None

    monkeypatch.setattr(routes.trade_review_db, "fetch_all", _fetch_all)
    monkeypatch.setattr(routes.trade_review_db, "fetch_by_id", _fetch_by_id)
    monkeypatch.setattr(routes.trade_review_db, "create_review", _create_review)
    monkeypatch.setattr(routes.trade_review_db, "update_review", _update_review)
    monkeypatch.setattr(routes.trade_review_db, "delete_review", _delete_review)

    client = TestClient(app)

    created = client.post(
        "/trade-reviews",
        json={
            "symbol": "000001",
            "stockName": "Ping An Bank",
            "buyDate": "2026-03-01",
            "sellDate": "2026-03-10",
            "pnlPct": 4.8,
            "buyLogicFundResonance": True,
            "sellReason": "B",
            "customPayload": {"market": "CN"},
        },
    )
    assert created.status_code == 200
    item = created.json()
    assert item["symbol"] == "000001"
    assert item["buyLogicFundResonance"] is True
    rid = item["id"]

    fetched = client.get(f"/trade-reviews/{rid}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == rid

    listed = client.get("/trade-reviews?symbol=000001")
    assert listed.status_code == 200
    payload = listed.json()
    assert payload["total"] == 1
    assert len(payload["items"]) == 1

    updated = client.put(
        f"/trade-reviews/{rid}",
        json={
            "goodActions": "Stop-loss executed without hesitation",
            "improvementAreas": "Need better macro hedge checks",
        },
    )
    assert updated.status_code == 200
    body = updated.json()
    assert body["goodActions"]
    assert body["improvementAreas"]

    deleted = client.delete(f"/trade-reviews/{rid}")
    assert deleted.status_code == 200
    assert deleted.json()["ok"] is True

    missing = client.get(f"/trade-reviews/{rid}")
    assert missing.status_code == 404
