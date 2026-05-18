from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from typing import Any, Iterable

from data_sync_service.config import get_settings
from data_sync_service.db.broker import (
    create_account,
    delete_account,
    ensure_account_state,
    get_account_state_row,
    get_snapshot,
    get_snapshot_image,
    insert_snapshot,
    list_accounts,
    list_snapshots,
    update_account_title,
    upsert_account_state,
)


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _norm_str(v: Any) -> str:
    s = "" if v is None else str(v)
    return re.sub(r"\s+", " ", s).strip()


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _decode_data_url(data_url: str) -> tuple[bytes, str]:
    """
    Decode a data URL (data:<mime>;base64,....) to raw bytes + media type.
    """
    if not data_url or "base64," not in data_url:
        return b"", "application/octet-stream"
    head, b64 = data_url.split("base64,", 1)
    media_type = "application/octet-stream"
    if head.startswith("data:"):
        media_type = head[5:].split(";", 1)[0] or media_type
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        raw = b""
    return raw, media_type


def _ai_service_base_url() -> str:
    settings = get_settings()
    base = os.getenv("AI_SERVICE_BASE_URL") or getattr(settings, "ai_service_base_url", "")
    return (base or "http://127.0.0.1:4310").rstrip("/")


def _ai_extract_pingan_screenshot(*, image_data_url: str) -> dict[str, Any]:
    payload = json.dumps({"imageDataUrl": image_data_url}).encode("utf-8")
    req = urllib.request.Request(
        f"{_ai_service_base_url()}/extract/broker/pingan",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
            return json.loads(body.decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        msg = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"ai-service error: {msg}") from exc


def _dedupe(rows: Iterable[dict[str, Any]], *, keys: list[str]) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        base = {k: _norm_str(r.get(k)) for k in keys if k in r and _norm_str(r.get(k))}
        sig = json.dumps(base or r, ensure_ascii=False, sort_keys=True)
        if sig in seen:
            continue
        seen.add(sig)
        out_rows.append(r)
    return out_rows


def _pick_first_str(obj: dict[str, Any], keys: list[str]) -> str:
    for k in keys:
        if k in obj:
            v = _norm_str(obj.get(k))
            if v:
                return v
    return ""


def _conditional_order_key(order: dict[str, Any]) -> str:
    payload = {
        "ticker": _pick_first_str(order, ["ticker", "Ticker", "symbol", "Symbol", "代码"]),
        "side": _pick_first_str(order, ["side", "Side", "方向"]).lower(),
        "triggerCondition": _pick_first_str(order, ["triggerCondition", "condition", "触发条件"]),
        "triggerValue": _pick_first_str(order, ["triggerValue", "value", "触发价"]),
        "qty": _pick_first_str(order, ["qty", "quantity", "委托数量", "数量"]),
        "status": _pick_first_str(order, ["status", "Status", "状态"]),
        "validUntil": _pick_first_str(order, ["validUntil", "有效期"]),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _seed_default_broker_account(broker: str) -> str:
    b = (broker or "").strip().lower() or "unknown"
    accounts = list_accounts(broker=b)
    if accounts:
        return str(accounts[0]["id"])
    aid = str(uuid.uuid4())
    ts = now_iso()
    create_account(
        account_id=aid,
        broker=b,
        title="Default",
        account_masked=None,
        created_at=ts,
        updated_at=ts,
    )
    return aid


def list_broker_accounts(*, broker: str | None = None) -> list[dict[str, Any]]:
    return list_accounts(broker=broker)


def create_broker_account(*, broker: str, title: str, account_masked: str | None) -> dict[str, Any]:
    ts = now_iso()
    return create_account(
        account_id=str(uuid.uuid4()),
        broker=broker,
        title=title,
        account_masked=account_masked,
        created_at=ts,
        updated_at=ts,
    )


def rename_broker_account(*, account_id: str, title: str) -> bool:
    return update_account_title(account_id=account_id, title=title, updated_at=now_iso())


def remove_broker_account(*, account_id: str) -> bool:
    return delete_account(account_id=account_id)


def get_account_state(*, account_id: str) -> dict[str, Any]:
    row = get_account_state_row(account_id)
    if row is None:
        ensure_account_state(account_id=account_id, broker="pingan", updated_at=now_iso())
        row = get_account_state_row(account_id) or {
            "accountId": account_id,
            "broker": "pingan",
            "updatedAt": now_iso(),
            "overview": {},
            "positions": [],
            "conditionalOrders": [],
            "trades": [],
        }
    positions = row.get("positions") if isinstance(row.get("positions"), list) else []
    orders = row.get("conditionalOrders") if isinstance(row.get("conditionalOrders"), list) else []
    trades = row.get("trades") if isinstance(row.get("trades"), list) else []
    overview = row.get("overview") if isinstance(row.get("overview"), dict) else {}
    return {
        "accountId": str(row["accountId"]),
        "broker": str(row["broker"]),
        "updatedAt": str(row["updatedAt"]),
        "overview": overview,
        "positions": positions,
        "conditionalOrders": orders,
        "trades": trades,
        "counts": {
            "positions": len(positions),
            "conditionalOrders": len(orders),
            "trades": len(trades),
        },
    }


def sync_account_from_images(
    *,
    account_id: str,
    captured_at: str,
    images: list[dict[str, Any]],
) -> dict[str, Any]:
    captured = captured_at.strip() if captured_at else ""
    if not captured:
        captured = now_iso()
    overview: dict[str, Any] | None = None
    saw_positions = False
    saw_orders = False
    saw_trades = False
    positions_acc: list[dict[str, Any]] = []
    orders_acc: list[dict[str, Any]] = []
    trades_acc: list[dict[str, Any]] = []

    for img in images:
        data_url = str(img.get("dataUrl") or "")
        if not data_url:
            continue
        extracted = _ai_extract_pingan_screenshot(image_data_url=data_url)
        if not isinstance(extracted, dict):
            continue
        kind = str(extracted.get("kind") or "unknown")
        data = extracted.get("data")
        data2 = data if isinstance(data, dict) else {}

        if kind == "account_overview":
            overview = data2
        elif kind == "positions" and overview is None and any(
            k in data2 for k in ("totalAssets", "securitiesValue", "cashAvailable", "withdrawable")
        ):
            overview = data2

        ps = data2.get("positions")
        if isinstance(ps, list):
            saw_positions = True
            positions_acc.extend([p if isinstance(p, dict) else {"raw": p} for p in ps])

        os_ = data2.get("orders")
        if isinstance(os_, list):
            saw_orders = True
            orders_acc.extend([o if isinstance(o, dict) else {"raw": o} for o in os_])

        ts = data2.get("trades")
        if isinstance(ts, list):
            saw_trades = True
            trades_acc.extend([t if isinstance(t, dict) else {"raw": t} for t in ts])

    positions_out = _dedupe(
        positions_acc, keys=["ticker", "Ticker", "symbol", "Symbol", "name", "Name"]
    ) if saw_positions and positions_acc else None
    orders_out = (
        _dedupe(
            orders_acc,
            keys=[
                "ticker",
                "Ticker",
                "symbol",
                "Symbol",
                "name",
                "Name",
                "side",
                "Side",
                "triggerCondition",
                "triggerValue",
                "qty",
                "quantity",
                "status",
                "validUntil",
            ],
        )
        if saw_orders and orders_acc
        else None
    )
    trades_out = (
        _dedupe(trades_acc, keys=["ticker", "Ticker", "symbol", "Symbol", "time", "date"])
        if saw_trades and trades_acc
        else None
    )

    upsert_account_state(
        account_id=account_id,
        broker="pingan",
        updated_at=captured,
        overview=overview,
        positions=positions_out,
        conditional_orders=orders_out,
        trades=trades_out,
    )
    return get_account_state(account_id=account_id)


def import_broker_screenshots(
    *,
    broker: str,
    account_id: str | None,
    captured_at: str,
    images: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    b = (broker or "").strip().lower() or "unknown"
    aid = (account_id or "").strip() or _seed_default_broker_account(b)
    captured = captured_at.strip() if captured_at else ""
    if not captured:
        captured = now_iso()
    out: list[dict[str, Any]] = []
    for img in images:
        name = str(img.get("name") or "screenshot")
        media_type = str(img.get("mediaType") or "image/*")
        data_url = str(img.get("dataUrl") or "")
        if not data_url:
            continue
        raw, media = _decode_data_url(data_url)
        if not raw:
            continue
        sha = _sha256_bytes(raw)

        extracted = _ai_extract_pingan_screenshot(image_data_url=data_url)
        if isinstance(extracted, dict):
            meta = extracted.get("__meta")
            meta2 = meta if isinstance(meta, dict) else {}
            meta2.setdefault("source", "ai-service")
            extracted["__meta"] = meta2
        kind = str((extracted or {}).get("kind") or "unknown")

        snap_id = str(uuid.uuid4())
        insert_snapshot(
            snapshot_id=snap_id,
            broker=b,
            account_id=aid,
            captured_at=captured,
            kind=kind,
            sha256=sha,
            image_bytes=raw,
            image_type=media or media_type,
            image_name=name,
            extracted=extracted if isinstance(extracted, dict) else {"raw": extracted},
            created_at=now_iso(),
        )
        out.append(
            {
                "id": snap_id,
                "broker": b,
                "accountId": aid,
                "capturedAt": captured,
                "kind": kind,
                "createdAt": now_iso(),
            }
        )
    return out


def list_broker_snapshots(*, broker: str, account_id: str | None, limit: int = 20) -> list[dict[str, Any]]:
    aid = (account_id or "").strip() or _seed_default_broker_account(broker)
    return list_snapshots(broker=broker, account_id=aid, limit=limit)


def get_broker_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    return get_snapshot(snapshot_id)


def get_broker_snapshot_image(snapshot_id: str) -> dict[str, Any] | None:
    return get_snapshot_image(snapshot_id)


def delete_conditional_order(*, account_id: str, order: dict[str, Any]) -> dict[str, Any]:
    row = get_account_state_row(account_id)
    if row is None:
        raise ValueError("Account state not found")
    target_key = _conditional_order_key(order)
    if not target_key or target_key == "{}":
        raise ValueError("order is invalid")
    orders: list[Any] = row.get("conditionalOrders") if isinstance(row.get("conditionalOrders"), list) else []
    kept: list[dict[str, Any]] = []
    removed = 0
    for o in orders:
        if isinstance(o, dict) and _conditional_order_key(o) == target_key:
            removed += 1
            continue
        kept.append(o if isinstance(o, dict) else {"raw": o})
    if removed == 0:
        raise KeyError("Conditional order not found")
    upsert_account_state(
        account_id=account_id,
        broker=str(row.get("broker") or "pingan"),
        updated_at=now_iso(),
        overview=row.get("overview") if isinstance(row.get("overview"), dict) else {},
        positions=row.get("positions") if isinstance(row.get("positions"), list) else [],
        conditional_orders=kept,
        trades=row.get("trades") if isinstance(row.get("trades"), list) else [],
    )
    return get_account_state(account_id=account_id)
