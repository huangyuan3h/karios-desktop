"""Incremental, symbol-scoped agent watchlist ops (2026-11-11).

``POST /watchlist/registry`` is the desktop UI contract and a **full replace**:
it upserts the posted rows and deletes every symbol that is not in the list, so
an empty / single-row post wipes the book. 乐乐 (the decision agent) must be able
to write the book after the 14:30 confirmation *without* that risk.

This module owns the policy behind ``GET/POST /v1/agent/watchlist``:

* every op is symbol-scoped (``upsert`` / ``set_position`` / ``remove``),
* unrelated rows are never touched,
* the write never places orders and never auto-logs a decision,
* removing a row that still has ``positionPct > 0`` needs an explicit ``force``.

The DB helpers used here (``upsert_registry_rows`` / ``delete_registry_symbols``)
are the incremental counterparts of ``upsert_registry``; the full-replace path
is deliberately not reachable from this module.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.db import watchlist_automation as registry_db
from data_sync_service.service.market_quotes import normalize_market_symbol

logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

SUPPORTED_OPS = ("upsert", "set_position", "remove")
MAX_OPS = 50
MAX_REASON_LEN = 500
# Fields an op may carry in addition to op / symbol / force.
OP_FIELDS = (
    "name",
    "source",
    "color",
    "positionPct",
    "costPrice",
    "maxPrice",
    "entryDate",
)
# Fields that may be explicitly nulled (they are optional metadata).
NULLABLE_FIELDS = ("name", "source", "color", "costPrice", "maxPrice", "entryDate")
ALLOWED_KEYS = {"op", "symbol", "force", *OP_FIELDS}

FULL_REPLACE_WARNING = (
    "POST /watchlist/registry is a full replace and will delete symbols not in the "
    "posted list. Use this incremental API instead."
)
WRITE_API = "POST /v1/agent/watchlist"
GET_API = "GET /v1/agent/watchlist"
ITEM_FIELDS = (
    "symbol",
    "name",
    "source",
    "color",
    "positionPct",
    "costPrice",
    "maxPrice",
    "entryDate",
    "addedAt",
)


class WatchlistOpError(ValueError):
    """A rejected op; ``status_code`` maps to the HTTP error the route returns."""

    def __init__(self, message: str, status_code: int = 422) -> None:
        super().__init__(message)
        self.status_code = status_code


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _clean_position_pct(value: Any, *, field: str = "positionPct") -> float:
    number = _as_float(value)
    if number is None:
        raise WatchlistOpError(f"{field} must be a finite number")
    return round(min(100.0, max(0.0, number)), 2)


def _symbol_of(op: dict[str, Any]) -> str:
    symbol = normalize_market_symbol(str(op.get("symbol") or "").strip())
    if not symbol:
        raise WatchlistOpError("every op requires a non-empty symbol")
    return symbol


def _clean_item(row: dict[str, Any]) -> dict[str, Any]:
    """Public camelCase view of one registry row."""
    out: dict[str, Any] = {}
    for field in ITEM_FIELDS:
        value = row.get(field)
        if field in ("positionPct", "costPrice", "maxPrice"):
            value = _as_float(value)
        elif value is not None:
            value = str(value)
        out[field] = value
    return out


def _snapshot(row: dict[str, Any] | None) -> dict[str, Any]:
    return _clean_item(row) if row else {}


def _has_open_position(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    pct = _as_float(row.get("positionPct"))
    return pct is not None and pct > 0


def _validate_op(op: Any) -> dict[str, Any]:
    if not isinstance(op, dict):
        raise WatchlistOpError("each op must be an object")
    unknown = sorted(set(op) - ALLOWED_KEYS)
    if unknown:
        raise WatchlistOpError(
            f"unsupported op field(s): {', '.join(unknown)}; "
            f"allowed: {', '.join(sorted(ALLOWED_KEYS))}"
        )
    kind = str(op.get("op") or "").strip()
    if kind not in SUPPORTED_OPS:
        raise WatchlistOpError(f"op must be one of {', '.join(SUPPORTED_OPS)}")
    clean = dict(op)
    clean["op"] = kind
    clean["symbol"] = _symbol_of(op)
    if "positionPct" in clean:
        clean["positionPct"] = _clean_position_pct(clean["positionPct"])
    for field in ("costPrice", "maxPrice"):
        if field in clean and clean[field] is not None:
            value = _as_float(clean[field])
            if value is None:
                raise WatchlistOpError(f"{field} must be a finite number or null")
            clean[field] = value
    if kind == "set_position" and "positionPct" not in clean:
        raise WatchlistOpError("set_position requires positionPct")
    return clean


# ---------------------------------------------------------------------------
# core evaluation (validates + optionally persists)
# ---------------------------------------------------------------------------


def _evaluate_ops(
    rows: list[dict[str, Any]],
    ops: list[dict[str, Any]],
    *,
    default_source: str,
    now_iso: str,
    persist: bool,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    working: dict[str, dict[str, Any]] = {
        str(row.get("symbol")): dict(row) for row in rows if row.get("symbol")
    }
    applied: list[dict[str, Any]] = []

    for op in ops:
        kind = op["op"]
        symbol = op["symbol"]
        existing = working.get(symbol)
        before = _snapshot(existing)
        result = {"op": kind, "symbol": symbol, "applied": False, "before": before, "after": before}

        if kind == "upsert":
            merged: dict[str, Any] = dict(existing) if existing else {"symbol": symbol}
            for field in OP_FIELDS:
                if field in op:
                    merged[field] = op[field]
            if existing:
                if op.get("source") is None:
                    merged["source"] = existing.get("source") or default_source
                if not merged.get("addedAt"):
                    merged["addedAt"] = existing.get("addedAt") or now_iso
            else:
                if not merged.get("source"):
                    merged["source"] = default_source
                merged["addedAt"] = now_iso
            merged["symbol"] = symbol
            working[symbol] = merged
            if persist:
                registry_db.upsert_registry_rows([merged])
            result["applied"] = True
            result["after"] = _snapshot(merged)

        elif kind == "set_position":
            if not existing:
                raise WatchlistOpError(f"symbol not in watchlist: {symbol}", status_code=404)
            for field in ("positionPct", "costPrice", "maxPrice", "entryDate"):
                if field in op:
                    existing[field] = op[field]
            if persist:
                registry_db.upsert_registry_rows([existing])
            result["applied"] = True
            result["after"] = _snapshot(existing)

        else:  # remove
            if not existing:
                # Idempotent: already gone is a success, nothing was applied.
                applied.append(result)
                continue
            if _has_open_position(existing) and op.get("force") is not True:
                raise WatchlistOpError(
                    f"{symbol} still has positionPct={existing.get('positionPct')}; "
                    "pass force: true to remove a holding"
                )
            if persist:
                registry_db.delete_registry_symbols([symbol])
            working.pop(symbol, None)
            result["applied"] = True
            result["after"] = {}

        applied.append(result)

    return applied, working


def _positions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for item in items:
        pct = _as_float(item.get("positionPct"))
        if pct is not None and pct > 0:
            out.append(item)
    return out


def _as_of_date() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _registry_items(rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    source_rows = registry_db.list_registry() if rows is None else rows
    return [_clean_item(row) for row in source_rows if isinstance(row, dict)]


def _items_from_working(working: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(
        working.values(),
        key=lambda r: (str(r.get("addedAt") or ""), str(r.get("symbol") or "")),
    )
    return [_clean_item(row) for row in rows]


def _invalidate_brief_cache() -> None:
    """Fail-open: a cache eviction must never fail a successful write."""
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("watchlist")
    except Exception as exc:  # noqa: BLE001
        logger.warning("judgment brief cache invalidation failed (watchlist): %s", exc)


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------


def build_watchlist_snapshot(*, rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Current registry + open positions, with the incremental-write hint."""
    items = _registry_items(rows)
    payload: dict[str, Any] = {
        "ok": True,
        "placesOrders": False,
        "asOfDate": _as_of_date(),
        "count": len(items),
        "items": items,
        "positions": _positions(items),
        "writeApi": WRITE_API,
        "fullReplaceWarning": FULL_REPLACE_WARNING,
    }
    try:
        updated_at = registry_db.latest_registry_updated_at()
    except Exception as exc:  # noqa: BLE001 — optional metadata only
        logger.warning("registry updated_at unavailable: %s", exc)
        updated_at = None
    if updated_at:
        payload["updatedAt"] = updated_at
    return payload


def render_watchlist_markdown(snapshot: dict[str, Any]) -> str:
    """Compact Markdown table: symbol / name / positionPct / costPrice / source."""
    items = snapshot.get("items") or []
    lines = [
        "# Watchlist registry (incremental API)",
        "",
        f"- asOfDate: {snapshot.get('asOfDate')}",
        f"- count: {snapshot.get('count', len(items))}",
        f"- write: {WRITE_API} (incremental; never a full replace)",
        "- placesOrders: false",
        "",
        "| symbol | name | positionPct | costPrice | source |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in items:
        name = str(item.get("name") or "").replace("|", "/")
        pct = item.get("positionPct")
        cost = item.get("costPrice")
        pct_text = "—" if pct is None else f"{pct:g}"
        cost_text = "—" if cost is None else f"{cost:g}"
        lines.append(
            f"| {item.get('symbol') or ''} | {name} | {pct_text} | {cost_text} "
            f"| {item.get('source') or ''} |"
        )
    if not items:
        lines.append("| — | — | — | — | — |")
    lines.extend(["", f"> {FULL_REPLACE_WARNING}", ""])
    return "\n".join(lines)


def apply_watchlist_ops(
    *,
    reason: str,
    ops: list[dict[str, Any]],
    source: str = "agent",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Validate (and unless ``dry_run`` apply) a batch of symbol-scoped ops.

    The whole batch is dry-evaluated first, so a 404 / 422 never leaves a
    half-applied request behind.
    """
    reason_text = str(reason or "").strip()
    if not reason_text:
        raise WatchlistOpError("reason is required")
    if len(reason_text) > MAX_REASON_LEN:
        raise WatchlistOpError(f"reason must be at most {MAX_REASON_LEN} characters")
    if not isinstance(ops, list) or not ops:
        raise WatchlistOpError("ops must be a non-empty list")
    if len(ops) > MAX_OPS:
        raise WatchlistOpError(f"ops is limited to {MAX_OPS} entries")

    default_source = str(source or "agent").strip() or "agent"
    clean_ops = [_validate_op(op) for op in ops]

    rows = registry_db.list_registry()
    now_iso = _now_iso()

    # Pass 1: validate the whole batch without writing.
    preview_applied, preview = _evaluate_ops(
        rows, clean_ops, default_source=default_source, now_iso=now_iso, persist=False
    )

    if dry_run:
        items = _items_from_working(preview)
        return {
            "ok": True,
            "placesOrders": False,
            "dryRun": True,
            "reason": reason_text,
            "applied": preview_applied,
            "asOfDate": _as_of_date(),
            "count": len(items),
            "items": items,
            "positions": _positions(items),
        }

    applied, _ = _evaluate_ops(
        rows, clean_ops, default_source=default_source, now_iso=now_iso, persist=True
    )
    if any(row.get("applied") for row in applied):
        _invalidate_brief_cache()
    items = _registry_items()
    return {
        "ok": True,
        "placesOrders": False,
        "dryRun": False,
        "reason": reason_text,
        "applied": applied,
        "asOfDate": _as_of_date(),
        "count": len(items),
        "items": items,
        "positions": _positions(items),
    }
