from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query  # type: ignore[import-not-found]
from fastapi.responses import Response  # type: ignore[import-not-found]
from pydantic import BaseModel  # type: ignore[import-not-found]

from data_sync_service.service.broker import (
    create_broker_account,
    delete_conditional_order,
    get_account_state,
    get_broker_snapshot,
    get_broker_snapshot_image,
    import_broker_screenshots,
    list_broker_accounts,
    list_broker_snapshots,
    remove_broker_account,
    rename_broker_account,
    sync_account_from_images,
)

router = APIRouter()


class BrokerImportImage(BaseModel):
    id: str
    name: str
    mediaType: str
    dataUrl: str


class BrokerImportRequest(BaseModel):
    capturedAt: str | None = None
    accountId: str | None = None
    images: list[BrokerImportImage]


class BrokerSyncRequest(BaseModel):
    capturedAt: str | None = None
    images: list[BrokerImportImage]


class DeleteBrokerConditionalOrderRequest(BaseModel):
    order: dict[str, Any]


@router.get("/broker/accounts")
def list_accounts_endpoint(broker: str | None = Query(None)) -> list[dict[str, Any]]:
    return list_broker_accounts(broker=broker)


@router.post("/broker/accounts")
def create_account_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
    broker = str(payload.get("broker") or "").strip().lower() or "unknown"
    title = str(payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title is required")
    account_masked = payload.get("accountMasked")
    account_masked_str = str(account_masked).strip() if account_masked is not None else None
    account_masked_str = account_masked_str or None
    return create_broker_account(broker=broker, title=title, account_masked=account_masked_str)


@router.put("/broker/accounts/{account_id}")
def rename_account_endpoint(account_id: str, payload: dict[str, Any]) -> dict[str, bool]:
    title = str(payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title is required")
    ok = rename_broker_account(account_id=account_id, title=title)
    if not ok:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"ok": True}


@router.delete("/broker/accounts/{account_id}")
def delete_account_endpoint(account_id: str) -> dict[str, bool]:
    ok = remove_broker_account(account_id=account_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"ok": True}


@router.get("/broker/pingan/snapshots")
def list_pingan_snapshots_endpoint(
    limit: int = Query(20, ge=1, le=200),
    accountId: str | None = Query(None),
) -> list[dict[str, Any]]:
    return list_broker_snapshots(broker="pingan", account_id=accountId, limit=limit)


@router.get("/broker/pingan/snapshots/{snapshot_id}")
def get_pingan_snapshot_endpoint(snapshot_id: str) -> dict[str, Any]:
    snap = get_broker_snapshot(snapshot_id)
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return {
        "id": snap["id"],
        "broker": snap["broker"],
        "accountId": snap.get("accountId"),
        "capturedAt": snap["capturedAt"],
        "kind": snap["kind"],
        "createdAt": snap["createdAt"],
        "imagePath": f"/broker/pingan/snapshots/{snap['id']}/image",
        "extracted": snap.get("extracted") or {},
    }


@router.get("/broker/pingan/snapshots/{snapshot_id}/image")
def get_pingan_snapshot_image_endpoint(snapshot_id: str) -> Response:
    img = get_broker_snapshot_image(snapshot_id)
    if not img:
        raise HTTPException(status_code=404, detail="Snapshot image not found")
    return Response(content=img["bytes"], media_type=img["mediaType"])


@router.post("/broker/pingan/import")
def import_pingan_screenshots_endpoint(req: BrokerImportRequest) -> dict[str, Any]:
    try:
        items = import_broker_screenshots(
            broker="pingan",
            account_id=req.accountId,
            captured_at=req.capturedAt or "",
            images=[x.model_dump() for x in req.images],
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "items": items}


@router.get("/broker/pingan/accounts/{account_id}/state")
def get_pingan_state_endpoint(account_id: str) -> dict[str, Any]:
    if not account_id.strip():
        raise HTTPException(status_code=400, detail="account_id is required")
    return get_account_state(account_id=account_id)


@router.post("/broker/pingan/accounts/{account_id}/sync")
def sync_pingan_state_endpoint(account_id: str, req: BrokerSyncRequest) -> dict[str, Any]:
    if not account_id.strip():
        raise HTTPException(status_code=400, detail="account_id is required")
    captured_at = (req.capturedAt or "").strip() or ""
    try:
        return sync_account_from_images(
            account_id=account_id,
            captured_at=captured_at or "",
            images=[x.model_dump() for x in req.images],
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/broker/pingan/accounts/{account_id}/state/conditional-orders/delete")
def delete_pingan_conditional_order_endpoint(
    account_id: str,
    req: DeleteBrokerConditionalOrderRequest,
) -> dict[str, Any]:
    if not account_id.strip():
        raise HTTPException(status_code=400, detail="account_id is required")
    if not isinstance(req.order, dict) or not req.order:
        raise HTTPException(status_code=400, detail="order is required")
    try:
        return delete_conditional_order(account_id=account_id, order=req.order)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
