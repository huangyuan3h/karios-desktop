"""api/broker_routes.py coverage."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from fastapi.responses import Response

from data_sync_service.api import broker_routes as br
from data_sync_service.api.broker_routes import (
    BrokerImportRequest,
    BrokerSyncRequest,
    DeleteBrokerConditionalOrderRequest,
    create_account_endpoint,
    delete_account_endpoint,
    delete_pingan_conditional_order_endpoint,
    get_pingan_snapshot_endpoint,
    get_pingan_snapshot_image_endpoint,
    get_pingan_state_endpoint,
    import_pingan_screenshots_endpoint,
    list_accounts_endpoint,
    list_pingan_snapshots_endpoint,
    rename_account_endpoint,
    sync_pingan_state_endpoint,
)


class TestAccounts:
    def test_list(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "list_broker_accounts", lambda broker: [{"id": "a1"}])
        assert list_accounts_endpoint(broker="pingan") == [{"id": "a1"}]

    def test_create(self, monkeypatch) -> None:
        seen = {}
        monkeypatch.setattr(br, "create_broker_account", lambda **kw: seen.update(kw) or {"id": "a1"})
        assert create_account_endpoint({"broker": "PingAn", "title": " 主账户 ", "accountMasked": "6222"})["id"] == "a1"
        assert seen["broker"] == "pingan" and seen["title"] == "主账户"
        assert seen["account_masked"] == "6222"
        with pytest.raises(HTTPException) as exc:
            create_account_endpoint({"broker": "pingan", "title": ""})
        assert exc.value.status_code == 400

    def test_rename(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "rename_broker_account", lambda **kw: True)
        assert rename_account_endpoint("a1", {"title": "新名字"}) == {"ok": True}
        monkeypatch.setattr(br, "rename_broker_account", lambda **kw: False)
        with pytest.raises(HTTPException) as exc:
            rename_account_endpoint("a1", {"title": "新名字"})
        assert exc.value.status_code == 404
        with pytest.raises(HTTPException) as exc:
            rename_account_endpoint("a1", {"title": "  "})
        assert exc.value.status_code == 400

    def test_delete(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "remove_broker_account", lambda account_id: True)
        assert delete_account_endpoint("a1") == {"ok": True}
        monkeypatch.setattr(br, "remove_broker_account", lambda account_id: False)
        with pytest.raises(HTTPException) as exc:
            delete_account_endpoint("a1")
        assert exc.value.status_code == 404


class TestSnapshots:
    def test_list(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "list_broker_snapshots", lambda **kw: [{"id": "s1"}])
        out = list_pingan_snapshots_endpoint(limit=20, accountId="a1")
        assert out == [{"id": "s1"}]

    def test_get(self, monkeypatch) -> None:
        snap = {"id": "s1", "broker": "pingan", "accountId": "a1", "capturedAt": "c", "kind": "k", "createdAt": "cr", "extracted": {"x": 1}}
        monkeypatch.setattr(br, "get_broker_snapshot", lambda sid: snap)
        out = get_pingan_snapshot_endpoint("s1")
        assert out["imagePath"] == "/broker/pingan/snapshots/s1/image"
        monkeypatch.setattr(br, "get_broker_snapshot", lambda sid: None)
        with pytest.raises(HTTPException) as exc:
            get_pingan_snapshot_endpoint("s1")
        assert exc.value.status_code == 404

    def test_get_image(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "get_broker_snapshot_image", lambda sid: {"bytes": b"abc", "mediaType": "image/png"})
        out = get_pingan_snapshot_image_endpoint("s1")
        assert isinstance(out, Response)
        assert out.body == b"abc"
        monkeypatch.setattr(br, "get_broker_snapshot_image", lambda sid: None)
        with pytest.raises(HTTPException) as exc:
            get_pingan_snapshot_image_endpoint("s1")
        assert exc.value.status_code == 404


class TestImport:
    def test_import(self, monkeypatch) -> None:
        seen = {}
        monkeypatch.setattr(br, "import_broker_screenshots", lambda **kw: seen.update(kw) or [{"id": "i1"}])
        req = BrokerImportRequest(accountId="a1", capturedAt="2026-08-07", images=[{"id": "i1", "name": "n", "mediaType": "image/png", "dataUrl": "data:"}])
        out = import_pingan_screenshots_endpoint(req)
        assert out == {"ok": True, "items": [{"id": "i1"}]}
        assert seen["broker"] == "pingan" and seen["account_id"] == "a1"
        assert seen["images"][0]["id"] == "i1"

    def test_import_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "import_broker_screenshots", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        req = BrokerImportRequest(images=[{"id": "i1", "name": "n", "mediaType": "t", "dataUrl": "d"}])
        with pytest.raises(HTTPException) as exc:
            import_pingan_screenshots_endpoint(req)
        assert exc.value.status_code == 500


class TestState:
    def test_get_state(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "get_account_state", lambda account_id: {"cash": 1.0})
        assert get_pingan_state_endpoint("a1") == {"cash": 1.0}
        with pytest.raises(HTTPException) as exc:
            get_pingan_state_endpoint("   ")
        assert exc.value.status_code == 400

    def test_sync(self, monkeypatch) -> None:
        seen = {}
        monkeypatch.setattr(br, "sync_account_from_images", lambda **kw: seen.update(kw) or {"ok": True})
        req = BrokerSyncRequest(capturedAt="c", images=[{"id": "i1", "name": "n", "mediaType": "t", "dataUrl": "d"}])
        assert sync_pingan_state_endpoint("a1", req) == {"ok": True}
        assert seen["account_id"] == "a1"
        with pytest.raises(HTTPException) as exc:
            sync_pingan_state_endpoint("   ", req)
        assert exc.value.status_code == 400

    def test_sync_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(br, "sync_account_from_images", lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        req = BrokerSyncRequest(images=[{"id": "i1", "name": "n", "mediaType": "t", "dataUrl": "d"}])
        with pytest.raises(HTTPException) as exc:
            sync_pingan_state_endpoint("a1", req)
        assert exc.value.status_code == 500

    def test_delete_conditional(self, monkeypatch) -> None:
        seen = {}
        monkeypatch.setattr(br, "delete_conditional_order", lambda **kw: seen.update(kw) or {"ok": True})
        req = DeleteBrokerConditionalOrderRequest(order={"id": "o1"})
        assert delete_pingan_conditional_order_endpoint("a1", req) == {"ok": True}
        with pytest.raises(HTTPException) as exc:
            delete_pingan_conditional_order_endpoint("   ", req)
        assert exc.value.status_code == 400
        with pytest.raises(HTTPException) as exc:
            delete_pingan_conditional_order_endpoint("a1", DeleteBrokerConditionalOrderRequest(order={}))
        assert exc.value.status_code == 400
        monkeypatch.setattr(br, "delete_conditional_order", lambda **kw: (_ for _ in ()).throw(KeyError("no such order")))
        with pytest.raises(HTTPException) as exc:
            delete_pingan_conditional_order_endpoint("a1", req)
        assert exc.value.status_code == 404
        monkeypatch.setattr(br, "delete_conditional_order", lambda **kw: (_ for _ in ()).throw(ValueError("bad order")))
        with pytest.raises(HTTPException) as exc:
            delete_pingan_conditional_order_endpoint("a1", req)
        assert exc.value.status_code == 400


def test_router_routes() -> None:
    paths = {r.path for r in br.router.routes}
    assert "/broker/accounts" in paths
    assert any("snapshot_id" in p and p.endswith("/image") for p in paths)
