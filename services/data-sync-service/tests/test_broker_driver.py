"""broker: decode, ai extract, account/state, images sync, conditional delete."""

from __future__ import annotations

import base64
import json
import urllib.error

from data_sync_service.service import broker as bk


def test_decode_data_url() -> None:
    b64 = base64.b64encode(b"abc").decode()
    raw, media = bk._decode_data_url(f"data:image/png;base64,{b64}")
    assert raw == b"abc" and media == "image/png"
    assert bk._decode_data_url("") == (b"", "application/octet-stream")
    assert bk._decode_data_url(f"data:image/jpeg;base64,{b64}")[1] == "image/jpeg"
    assert bk._decode_data_url("data:image/gif;base64,!!notb64!!")[0] == b""


def test_ai_service_base_url(monkeypatch) -> None:
    monkeypatch.setenv("AI_SERVICE_BASE_URL", "http://x:1/")
    assert bk._ai_service_base_url() == "http://x:1"
    monkeypatch.delenv("AI_SERVICE_BASE_URL")
    monkeypatch.setattr(bk, "get_settings", lambda: type("S", (), {"ai_service_base_url": ""})())
    assert bk._ai_service_base_url() == "http://127.0.0.1:4310"


class _Resp:
    def __init__(self, body):
        self._b = body.encode() if isinstance(body, str) else body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._b


def test_ai_extract_pingan_screenshot(monkeypatch) -> None:
    monkeypatch.setattr(bk, "_ai_service_base_url", lambda: "http://ai")
    captured = {}

    def fake_urlopen(req, timeout=120):
        captured["url"] = req.full_url
        return _Resp(json.dumps({"kind": "account_overview", "data": {}}))

    monkeypatch.setattr(bk.urllib.request, "urlopen", fake_urlopen)
    out = bk._ai_extract_pingan_screenshot(image_data_url="data:image/png;base64,AA==")
    assert captured["url"] == "http://ai/extract/broker/pingan"
    assert out["kind"] == "account_overview"

    def fail(req, timeout=120):
        raise urllib.error.HTTPError("http://ai/x", 500, "e", {}, None)

    monkeypatch.setattr(bk.urllib.request, "urlopen", fail)
    try:
        bk._ai_extract_pingan_screenshot(image_data_url="x")
        assert False
    except RuntimeError as exc:
        assert "ai-service error" in str(exc)


def test_dedupe_and_pick_first_str() -> None:
    rows = [{"ticker": " 600000 ", "qty": 1}, {"ticker": "600000", "qty": 1}, {"ticker": "000001", "qty": 2}]
    out = bk._dedupe(rows, keys=["ticker"])
    assert len(out) == 2
    assert bk._pick_first_str({"ticker": "  X  "}, ["ticker", "symbol"]) == "X"
    assert bk._pick_first_str({}, ["ticker"]) == ""
    assert bk._pick_first_str({"symbol": "Y"}, ["ticker", "symbol"]) == "Y"


def test_conditional_order_key() -> None:
    k1 = bk._conditional_order_key({"ticker": "600000.SH", "side": "BUY", "triggerValue": "10.5", "qty": "100"})
    k2 = bk._conditional_order_key({"symbol": "600000.SH", "方向": "buy", "触发价": "10.5", "数量": "100"})
    assert k1 == k2  # aliases normalize to same key


def test_seed_default_broker_account(monkeypatch) -> None:
    monkeypatch.setattr(bk, "list_accounts", lambda broker=None: [{"id": "acc-1"}])
    assert bk._seed_default_broker_account("  PingAn ") == "acc-1"

    monkeypatch.setattr(bk, "list_accounts", lambda broker=None: [])
    created = {}

    def fake_create(account_id, broker, title, account_masked, created_at, updated_at):
        created["id"] = account_id

    monkeypatch.setattr(bk, "create_account", fake_create)
    aid = bk._seed_default_broker_account("")
    assert aid == created["id"]


def test_account_crud_wrappers(monkeypatch) -> None:
    monkeypatch.setattr(bk, "list_accounts", lambda broker=None: [{"id": "a1"}])
    assert bk.list_broker_accounts(broker="pingan") == [{"id": "a1"}]
    monkeypatch.setattr(bk, "update_account_title", lambda account_id, title, updated_at: True)
    assert bk.rename_broker_account(account_id="a1", title="New") is True
    monkeypatch.setattr(bk, "delete_account", lambda account_id: True)
    assert bk.remove_broker_account(account_id="a1") is True


def test_get_account_state(monkeypatch) -> None:
    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: {
        "accountId": "a1", "broker": "pingan", "updatedAt": "t",
        "overview": {"totalAssets": 1}, "positions": [{"ticker": "x"}],
        "conditionalOrders": [{"ticker": "y"}], "trades": [{"ticker": "z"}],
    })
    out = bk.get_account_state(account_id="a1")
    assert out["counts"] == {"positions": 1, "conditionalOrders": 1, "trades": 1}

    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: None)
    monkeypatch.setattr(bk, "ensure_account_state", lambda **kw: None)
    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: {
        "accountId": "a1", "broker": "pingan", "updatedAt": "t",
        "overview": None, "positions": "not-a-list", "conditionalOrders": None, "trades": None,
    })
    out2 = bk.get_account_state(account_id="a1")
    assert out2["positions"] == [] and out2["overview"] == {}


def test_sync_account_from_images(monkeypatch) -> None:
    monkeypatch.setattr(bk, "_ai_extract_pingan_screenshot", lambda image_data_url: {
        "kind": "positions",
        "data": {
            "totalAssets": 100.0,
            "positions": [{"ticker": "600000"}, {"ticker": "600000"}, {"ticker": "000001"}],
            "orders": [{"ticker": "a"}, {"ticker": "a"}],
            "trades": [{"ticker": "b"}, {"ticker": "b"}],
        },
    })
    state = {"accountId": "a1", "broker": "pingan", "updatedAt": "t", "overview": {}, "positions": [], "conditionalOrders": [], "trades": []}
    upserted = {}

    def fake_upsert(**kw):
        upserted.update(kw)

    def fake_state(account_id):
        return state

    monkeypatch.setattr(bk, "upsert_account_state", fake_upsert)
    monkeypatch.setattr(bk, "get_account_state", fake_state)
    bk.sync_account_from_images(
        account_id="a1", captured_at="", images=[{"dataUrl": "data:image/png;base64,AA=="}],
    )
    assert upserted["overview"]["totalAssets"] == 100.0  # positions screen doubles as overview
    assert len(upserted["positions"]) == 2  # deduped
    assert len(upserted["conditional_orders"]) == 1
    assert len(upserted["trades"]) == 1

    monkeypatch.setattr(bk, "_ai_extract_pingan_screenshot", lambda image_data_url: {"kind": "other", "data": {}})
    bk.sync_account_from_images(account_id="a1", captured_at="2026-08-07", images=[{"dataUrl": "x"}])
    assert upserted["positions"] is None and upserted["updated_at"] == "2026-08-07"


def test_import_broker_screenshots(monkeypatch) -> None:
    b64 = base64.b64encode(b"img-bytes").decode()
    monkeypatch.setattr(bk, "_seed_default_broker_account", lambda b: "acc-1")
    monkeypatch.setattr(bk, "_ai_extract_pingan_screenshot", lambda image_data_url: {
        "kind": "positions", "data": {}, "__meta": {"tokens": 10},
    })
    monkeypatch.setattr(bk, "insert_snapshot", lambda **kw: None)
    out = bk.import_broker_screenshots(
        broker="pingan", account_id="", captured_at="",
        images=[{"name": "shot.png", "mediaType": "image/png", "dataUrl": f"data:image/png;base64,{b64}"}],
    )
    assert out[0]["kind"] == "positions" and out[0]["broker"] == "pingan"

    assert bk.import_broker_screenshots(broker="pingan", account_id="acc-1", captured_at="t", images=[{"dataUrl": ""}]) == []

    monkeypatch.setattr(bk, "_ai_extract_pingan_screenshot", lambda image_data_url: {})
    out2 = bk.import_broker_screenshots(
        broker="pingan", account_id="acc-1", captured_at="t",
        images=[{"dataUrl": f"data:image/png;base64,{b64}"}],
    )
    assert out2[0]["kind"] == "unknown"


def test_list_and_get_snapshot_wrappers(monkeypatch) -> None:
    monkeypatch.setattr(bk, "_seed_default_broker_account", lambda b: "acc-1")
    monkeypatch.setattr(bk, "list_snapshots", lambda broker, account_id, limit: [{"id": "s1"}])
    assert bk.list_broker_snapshots(broker="p", account_id=None, limit=5) == [{"id": "s1"}]
    monkeypatch.setattr(bk, "get_snapshot", lambda sid: {"id": sid})
    assert bk.get_broker_snapshot("s1") == {"id": "s1"}
    monkeypatch.setattr(bk, "get_snapshot_image", lambda sid: {"imageType": "png"})
    assert bk.get_broker_snapshot_image("s1") == {"imageType": "png"}


def test_delete_conditional_order(monkeypatch) -> None:
    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: {
        "accountId": "a1", "broker": "pingan", "updatedAt": "t", "overview": {},
        "positions": [], "trades": [],
        "conditionalOrders": [{"ticker": "600000.SH", "side": "BUY", "triggerValue": "10.5", "qty": "100"}],
    })
    monkeypatch.setattr(bk, "upsert_account_state", lambda **kw: None)
    monkeypatch.setattr(bk, "get_account_state", lambda account_id: {"kept": True})
    out = bk.delete_conditional_order(account_id="a1", order={"symbol": "600000.SH", "方向": "buy", "触发价": "10.5", "数量": "100"})
    assert out == {"kept": True}

    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: None)
    try:
        bk.delete_conditional_order(account_id="a1", order={"ticker": "x"})
        assert False
    except ValueError:
        pass

    monkeypatch.setattr(bk, "get_account_state_row", lambda aid: {
        "accountId": "a1", "broker": "p", "updatedAt": "t", "overview": {}, "positions": [], "trades": [],
        "conditionalOrders": [],
    })
    try:
        bk.delete_conditional_order(account_id="a1", order={})
        assert False
    except KeyError:
        pass  # empty-order key is not "{}" (all fields empty strings) → not found
    try:
        bk.delete_conditional_order(account_id="a1", order={"ticker": "zzz"})
        assert False
    except KeyError:
        pass
